# Stellt das Abgabe-ZIP zusammen: abgabe/Leistungsnachweis_DB_DWH_Erbsland_Liechti.zip
#
# Aufruf aus dem Repo-Hauptordner:
#   pwsh scripts/make_zip.ps1
#
# Erwartet in abgabe/: Leistungsnachweis_DB_DWH_Erbsland_Liechti.dbc und eine *.lvdash.json (Dashboard-Export).
# Das PDF wird nicht neu erzeugt; vorher `cd scripts/pdf; npm run pdf` ausführen.

$ErrorActionPreference = 'Stop'
$root   = Split-Path -Parent $PSScriptRoot
$out    = Join-Path $root 'abgabe/Leistungsnachweis_DB_DWH_Erbsland_Liechti.zip'
$stage  = Join-Path ([IO.Path]::GetTempPath()) "abgabe_$([guid]::NewGuid().ToString('N'))"
$fehler = @()

function Copy-Abgabe([string]$quelle, [string]$ziel) {
    $src = Join-Path $root $quelle
    if (-not (Test-Path $src)) { $script:fehler += "fehlt: $quelle"; return }
    $dst = Join-Path $stage $ziel
    New-Item -ItemType Directory -Force (Split-Path -Parent $dst) | Out-Null
    Copy-Item -Recurse $src $dst
}

New-Item -ItemType Directory -Force $stage | Out-Null

Copy-Abgabe 'bericht/bericht.pdf'                 'bericht.pdf'
Copy-Abgabe 'abgabe/Leistungsnachweis_DB_DWH_Erbsland_Liechti.dbc' 'Leistungsnachweis_DB_DWH_Erbsland_Liechti.dbc'
Copy-Abgabe 'INSTALL.pdf'                         'INSTALL.pdf'
Copy-Abgabe 'sql'                                 'sql'
Copy-Abgabe 'databricks'                          'databricks'
Copy-Abgabe 'docs/diagramme'                      'docs/diagramme'
Copy-Abgabe 'scripts/generate_data.py'            'scripts/generate_data.py'

$dash = @(Get-ChildItem (Join-Path $root 'abgabe') -Filter '*.lvdash.json' -ErrorAction SilentlyContinue)
if ($dash.Count -eq 1) { Copy-Item $dash[0].FullName (Join-Path $stage $dash[0].Name) }
elseif ($dash.Count -eq 0) { $fehler += 'fehlt: abgabe/*.lvdash.json (Dashboard-Export)' }
else { $fehler += "mehrere Dashboard-Dateien in abgabe/: $($dash.Name -join ', ')" }

# Warnung, wenn ein Markdown-Dokument nach dem letzten PDF-Build geändert wurde
foreach ($doc in 'bericht/bericht', 'INSTALL') {
    $md  = Get-Item (Join-Path $root "$doc.md")
    $pdf = Get-Item (Join-Path $root "$doc.pdf") -ErrorAction SilentlyContinue
    if ($pdf -and $md.LastWriteTime -gt $pdf.LastWriteTime) {
        $fehler += "$doc.pdf ist älter als $doc.md – PDF neu erzeugen (cd scripts/pdf; npm run pdf)"
    }
}

if (Test-Path $out) { Remove-Item $out }
Compress-Archive -Path (Join-Path $stage '*') -DestinationPath $out
Remove-Item -Recurse -Force $stage

Write-Host "ZIP erstellt: $out"
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [IO.Compression.ZipFile]::OpenRead($out)
$zip.Entries | Sort-Object FullName | ForEach-Object { '  {0,9:N0}  {1}' -f $_.Length, $_.FullName }
$zip.Dispose()

if ($fehler) {
    Write-Host ''
    Write-Warning 'Noch nicht abgabebereit:'
    $fehler | ForEach-Object { Write-Warning "  - $_" }
    exit 1
}
