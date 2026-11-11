# ZHAW DB & DWH – Leistungsnachweis (Variante 1)

**Szenario:** Schweizer Post – Verkauf und Versand von Brief- und Paketprodukten (Inland und International).

Wir bauen eine kleine operative Datenbank, darauf ein Datawarehouse in der **Medaillon-Architektur**
(Bronze → Silver → Gold) und ein Auswertungsdashboard. Technisch läuft alles auf **Databricks**
(Free Edition), das Dashboard optional in **Power BI**.

- Abgabe: **Montag, 30.11.2026, 23:00 Uhr** via Moodle, als `.zip` mit dem Bericht (PDF), dem Databricks-Export (`.dbc`) und ggf. der Power-BI-Datei
- Aufgabenstellung: siehe Moodle („DB-DWH Aufgabenstellung Leistungsnachweis“)

## Inhalt des Repositories

| Pfad | Inhalt |
|---|---|
| [`INSTALL.md`](INSTALL.md) | Installationsanleitung (Team und Dozent/in) |
| [`bericht/bericht.md`](bericht/bericht.md) | Bericht (Markdown, wird am Schluss als PDF exportiert) |
| [`docs/diagramme/`](docs/diagramme) | Mermaid-Diagramme: ER-Modell, Sternschema, Medaillon-Architektur |
| [`sql/01_create_schema.sql`](sql/01_create_schema.sql) | DDL der operativen Datenbank `post_oltp` (7 Tabellen inkl. PK/FK) |
| `sql/02_…` – `sql/04_…` | Beispieldaten als INSERT-Skripte (generiert) |
| [`sql/05_pruefabfragen.sql`](sql/05_pruefabfragen.sql) | Zeilenzahlen, Datenqualitäts-Checks, Beispielabfragen |
| [`sql/00_setup_alles.sql`](sql/00_setup_alles.sql) | Führt alle Skripte in der richtigen Reihenfolge aus |
| [`scripts/generate_data.py`](scripts/generate_data.py) | Datengenerator (Python, deterministisch) |
| [`databricks/`](databricks) | ETL-Notebooks Bronze → Silver → Gold (`10`–`30`), Dashboard-Sichten (`40`), Gesamtlauf `00_etl_alles` |

Alle `.sql`-Dateien sind gleichzeitig **Databricks-Notebooks im Quellformat**. Sie lassen sich direkt als Notebook
importieren oder über einen Databricks-Git-Ordner öffnen.

## Schnellstart

1. Databricks: **Workspace → Create → Git folder** → URL dieses Repos.
2. `sql/00_setup_alles` öffnen → **Run all**.
3. Kontrolle in `sql/05_pruefabfragen`.
4. `databricks/00_etl_alles` öffnen → **Run all** (Datawarehouse laden).
5. Dashboard gemäss [INSTALL.md](INSTALL.md), Kapitel 7, erstellen.

Details: [INSTALL.md](INSTALL.md).

## Zusammenarbeit

Mit der Free Edition kann nicht im selben Workspace zusammengearbeitet werden. Jedes Teammitglied hat
deshalb einen eigenen Workspace mit einem Git-Ordner auf dieses Repository. Das Repository ist die *Single Source of
Truth*. Für die Abgabe wird der Ordner aus **einem** Workspace als `.dbc` exportiert.

## Bericht als PDF erzeugen

Der Bericht ist in Markdown geschrieben (versionierbar). Für die Abgabe gibt es zum Beispiel diese Wege:

- **VS Code** mit der Erweiterung *Markdown PDF* (rendert auch Mermaid). Rechtsklick im Editor → *Markdown PDF: Export (pdf)*.
- **Pandoc**: `pandoc bericht/bericht.md -o bericht.pdf --toc`. Mermaid-Diagramme vorher als PNG/SVG exportieren,
  z.B. über <https://mermaid.live>.
