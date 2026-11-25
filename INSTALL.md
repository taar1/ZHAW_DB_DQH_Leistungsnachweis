# Installationsanleitung

Diese Anleitung beschreibt, wie die operative Datenbank `post_oltp` (und das Datawarehouse) in Databricks aufgesetzt und ausgeführt wird.

---

## 1. Hinweise vorab

Getestet mit der Databricks Free Edition (Serverless, Katalog `workspace`). Angelegt werden die Schemas `post_oltp`, `post_dwh_bronze`, `post_dwh_silver` und `post_dwh_gold`. Ein bestehendes Schema `post_oltp` wird dabei vollständig gelöscht und neu angelegt, in den DWH-Schemas werden gleichnamige Tabellen überschrieben.

---

## 2. Notebooks importieren und ausführen

Die Abgabe enthält die Databricks-Exportdatei `Leistungsnachweis_DB_DWH_Erbsland_Liechti.dbc` mit allen Notebooks.

1. In Databricks links auf **Workspace** klicken, den eigenen Benutzerordner öffnen.
2. Rechts oben ⋮ → **Import** wählen, *File* anklicken und die `.dbc`-Datei hochladen.

Der Import legt den Ordner `post_leistungsnachweis` mit den Unterordnern `sql/` und `databricks/` an. Darin die Notebooks in folgender Reihenfolge ausführen (jeweils oben **Run all**, Compute: *Serverless*):

| Reihenfolge | Notebook | Zweck |
|---|---|---|
| 1 | `sql/00_setup_alles` | Führt 01–05 automatisch nacheinander aus |
| – | *(alternativ einzeln:)* `01_create_schema` … `05_pruefabfragen` | |
| 2 | `databricks/00_etl_alles` | ETL Bronze → Silver → Gold und Dashboard-Sichten (Kapitel 4) |
| 3 | Dashboard | Datei `.lvdash.json` importieren (Kapitel 5) |

Kontrolle: Im Notebook `05_pruefabfragen` müssen die Zeilenzahlen gemäss Kapitel 3 erscheinen, im ETL die Werte gemäss Kapitel 4.

Falls der Import der `.dbc`-Datei nicht klappt, lassen sich die Dateien aus den Ordnern `sql/` und `databricks/` des Abgabe-ZIP auch einzeln importieren (⋮ → **Import** → *File*, Mehrfachauswahl möglich). Die Dateien jedes Ordners müssen dabei zusammen in einem Ordner liegen, weil `00_setup_alles` und `00_etl_alles` die anderen Notebooks mit `%run` aufrufen.

---

## 3. Erwartete Zeilenzahlen

Nach erfolgreichem Setup liefert `05_pruefabfragen` folgende Werte:

| Tabelle | Zeilen |
|---|---:|
| land | 24 |
| verkaufskanal | 7 |
| produkt | 37 |
| kunde | 658 |
| empfaenger | 3'000 |
| auftrag | 5'687 |
| sendung | 12'134 |

---

## 4. Datawarehouse laden (ETL)

Voraussetzung: Die operative DB ist aufgesetzt (Kapitel 2).

Im Ordner `databricks/` das Notebook **`00_etl_alles`** öffnen → **Run all** (Compute: *Serverless*). Es führt nacheinander aus:

| Notebook | Ergebnis |
|---|---|
| `10_bronze_laden` | Schema `post_dwh_bronze` mit unveränderten Kopien der 7 Tabellen (plus Ladezeitpunkt `_geladen_am`) |
| `20_silver_bereinigen` | Schema `post_dwh_silver` mit bereinigten Tabellen und `dq_protokoll` |
| `30_gold_sternschema` | Schema `post_dwh_gold` mit `dim_datum`, `dim_kunde`, `dim_produkt`, `dim_kanal`, `dim_zielland`, `fact_sendung` |
| `40_dashboard_views` | 6 Sichten `post_dwh_gold.v_…` als Datenquellen für das Dashboard |

Der ETL-Lauf kann beliebig oft wiederholt werden.

### Kontrolle

Jedes Notebook zeigt in der letzten Zelle ein Prüfergebnis.

**`10_bronze_laden`:** Die Zeilenzahl jeder Tabelle in Bronze wird mit der Quelle `post_oltp` verglichen. In der Spalte `ok` muss überall `true` stehen.

**`20_silver_bereinigen`:** Hier laufen vier Datenqualitätsprüfungen. In die Beispieldaten haben wir absichtlich Fehler eingebaut, die diese Prüfungen finden sollen. Fehlerhafte Datensätze werden markiert und bereinigt, aber nicht gelöscht. Erwartet wird folgende Anzahl Fehler:

| Prüfung | Was geprüft wird | Fehler |
|---|---|---:|
| P1 Referentielle Integrität | Der Empfänger einer Sendung existiert in der Tabelle `empfaenger`. | 12 |
| P2 Plausibilität Gewicht | Das Gewicht ist grösser als 0 und liegt innerhalb der Gewichtslimite des Produkts. | 153 |
| P3 Plausibilität Zustelldatum | Das Zustelldatum liegt nicht vor dem Aufgabedatum. | 7 |
| P4 Abstimmung Auftragstotal | Der Betrag eines Auftrags entspricht der Summe seiner Sendungen. | 43 |

Die Ergebnisse werden zusätzlich in der Tabelle `post_dwh_silver.dq_protokoll` gespeichert. Pro Lauf kommen vier Zeilen dazu, ältere Läufe bleiben erhalten.

**`30_gold_sternschema`:** Silver und Faktentabelle enthalten beide 12'134 Sendungen. Die Spalten `ohne_datum`, `ohne_kunde`, `ohne_produkt`, `ohne_kanal` und `ohne_zielland` zählen Sendungen ohne passenden Eintrag in der jeweiligen Dimension und müssen alle 0 sein. Dazu kommen der Umsatz von 153'541.10 CHF und die Pünktlichkeit der Pakete von 93.4 %.

---

## 5. Dashboard (Databricks AI/BI)

Die Daten des Dashboards kommen aus den Sichten von `40_dashboard_views`. Voraussetzung ist deshalb, dass der ETL-Lauf (Kapitel 4) durchgelaufen ist.

1. Links in der Seitenleiste **Dashboards** öffnen.
2. Beim Knopf **Create dashboard** auf den Pfeil daneben klicken → **Import dashboard from file** und die Datei `Schweizer_Post_Sendungen_2026.lvdash.json` aus der Abgabe wählen.
3. Kontrolle: Die Kennzahlen-Kacheln zeigen 12'134 Sendungen, 153'541.1 CHF Umsatz und 93.4 % Pünktlichkeit.

Die Datensätze verwenden voll qualifizierte Namen (`workspace.post_dwh_gold.v_…`). Wer einen anderen Katalog als `workspace` verwendet, passt die SQL der Datensätze im Reiter **Data** entsprechend an.

---

## 6. Häufige Probleme

| Problem | Lösung |
|---|---|
| `SCHEMA_NOT_FOUND` / `TABLE_OR_VIEW_NOT_FOUND` | Zuerst `01_create_schema` ausführen; prüfen, ob der richtige Katalog gewählt ist. |
| `PERMISSION_DENIED` beim `CREATE SCHEMA` | Einen Katalog wählen, in dem man Schreibrechte hat (Free Edition: `workspace`). |
| Insert sehr langsam | Prüfen, ob *Serverless* als Compute gewählt ist. Jede Insert-Zelle enthält 1000 Zeilen. |
| `post_dwh_…` nicht gefunden (Dashboard) | Zuerst `databricks/00_etl_alles` ausführen. Bei einem anderen Katalog als `workspace` die SQL der Datensätze anpassen (Kapitel 5). |
| Neu aufsetzen | Einfach `00_setup_alles` erneut ausführen. `01_create_schema` löscht das Schema `post_oltp` vorher vollständig. |
