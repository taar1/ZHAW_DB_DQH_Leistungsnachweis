# Installationsanleitung

Diese Anleitung richtet sich an das Projektteam **und** an die prüfende Person (Dozent/in). Sie beschreibt,
wie die operative Datenbank `post_oltp` (und das Datawarehouse) in **Databricks** aufgesetzt und
ausgeführt wird.

> Getestet für die **Databricks Free Edition** (Serverless Compute, Unity Catalog mit Standardkatalog `workspace`).
> In anderen Workspaces funktioniert es gleich, sofern ein Katalog mit Schreibrechten gewählt ist.

---

## 1. Voraussetzungen

| Was | Details |
|---|---|
| Databricks-Account | Free Edition genügt: <https://www.databricks.com/learn/free-edition> |
| Compute | *Serverless* (in der Free Edition automatisch vorhanden) |
| Katalog | `workspace` (Standard in der Free Edition). Es werden die Schemas `post_oltp`, `post_dwh_bronze`, `post_dwh_silver`, `post_dwh_gold` angelegt. |
| Optional: Python 3.9+ | Nur nötig, wenn die Beispieldaten neu generiert werden sollen (`scripts/generate_data.py`). |
| Optional: Power BI Desktop | Nur falls das Dashboard in Power BI statt in Databricks umgesetzt wird. |

---

## 2. Variante A – Abgabe-Datei importieren (für die prüfende Person)

Die Abgabe enthält eine **Databricks-Exportdatei** (`.dbc`) mit allen Notebooks.

1. In Databricks links auf **Workspace** klicken, den eigenen Benutzerordner öffnen.
2. Rechts oben **⋮ → Import** wählen, *File* anklicken und die `.dbc`-Datei hochladen.
3. Den importierten Ordner öffnen und die Notebooks in folgender Reihenfolge ausführen
   (jeweils oben **Run all**, Compute: *Serverless*):

   | Reihenfolge | Notebook | Zweck | Dauer ca. |
   |---|---|---|---|
   | 1 | `sql/00_setup_alles` | Führt 01–05 automatisch nacheinander aus | 1–2 min |
   | – | *(alternativ einzeln:)* `01_create_schema` … `05_pruefabfragen` | | |
   | 2 | `databricks/00_etl_alles` | ETL Bronze → Silver → Gold und Dashboard-Sichten (Kapitel 6) | 1–2 min |
   | 3 | Dashboard | In der Oberfläche erstellen (Kapitel 7) | 15–30 min |

4. Kontrolle: Im Notebook `05_pruefabfragen` müssen die Zeilenzahlen gemäss Kapitel 5 erscheinen, im ETL die Werte
   gemäss Kapitel 6.

---

## 3. Variante B – Aus dem GitHub-Repository (für das Team)

### B1. Als Git-Ordner verknüpfen (empfohlen)

1. **Workspace → Create → Git folder**.
2. Git-Repository-URL: `https://github.com/taar1/ZHAW_DB_DQH_Leistungsnachweis`, Provider *GitHub*.
   - Ist das Repository privat, zuerst unter **Settings → Linked accounts → Git integration** einen
     GitHub *Personal Access Token* hinterlegen.
3. Im Git-Ordner `sql/00_setup_alles` öffnen und **Run all** klicken.
4. Änderungen am Code: im Git-Ordner über den Branch-Button **Pull / Commit & Push**.

Die `.sql`-Dateien beginnen mit `-- Databricks notebook source`. Databricks erkennt sie deshalb automatisch als
**SQL-Notebooks** (Zellen sind durch `-- COMMAND ----------` getrennt). Gleichzeitig bleiben es normale SQL-Skripte, die
man im Editor lesen oder in einem anderen SQL-Tool ausführen kann.

### B2. Ohne Git: Dateien einzeln importieren

1. Repository als ZIP herunterladen (GitHub → *Code → Download ZIP*) und entpacken.
2. In Databricks einen Ordner anlegen, z.B. `post_leistungsnachweis/sql`.
3. **⋮ → Import → File** und alle Dateien aus `sql/` importieren (Mehrfachauswahl möglich).
   Die Dateien erscheinen als SQL-Notebooks.
4. `00_setup_alles` öffnen → **Run all**.

> Wichtig: `00_setup_alles` ruft die anderen Notebooks mit `%run ./…` auf. Alle Dateien aus `sql/` müssen deshalb
> **im selben Ordner** liegen.

### B3. Nur SQL-Editor (Fallback)

Jede Datei kann auch im **SQL Editor** geöffnet und mit *Run all* ausgeführt werden (Reihenfolge 01 → 05).
Die grosse Insert-Datei `04_…` (ca. 1.7 MB) ist dort allerdings träge. Die Notebook-Variante ist deshalb vorzuziehen.

---

## 4. Katalog wechseln (falls nicht `workspace`)

Die Skripte verwenden nur schemaqualifizierte Namen (`post_oltp.kunde`) und laufen im **aktuell gewählten Katalog**.
In einem anderen Workspace entweder in der Katalog-Auswahl oben im Notebook den gewünschten Katalog wählen
oder vor dem Setup ausführen:

```sql
USE CATALOG <mein_katalog>;
```

---

## 5. Erwartete Zeilenzahlen

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

## 6. Datawarehouse laden (ETL)

Voraussetzung: Die operative DB ist aufgesetzt (Kapitel 2 bzw. 3).

1. Im Ordner `databricks/` das Notebook **`00_etl_alles`** öffnen → **Run all** (Compute: *Serverless*, ca. 1–2 min).
   Es führt nacheinander aus:

   | Notebook | Ergebnis |
   |---|---|
   | `10_bronze_laden` | Schema `post_dwh_bronze` mit 7 Tabellen + Ladekontrolle |
   | `20_silver_bereinigen` | Schema `post_dwh_silver` mit bereinigten Tabellen und `dq_protokoll` |
   | `30_gold_sternschema` | Schema `post_dwh_gold` mit `dim_datum`, `dim_kunde`, `dim_produkt`, `dim_kanal`, `dim_zielland`, `fact_sendung` |
   | `40_dashboard_views` | 6 Sichten `post_dwh_gold.v_…` als Datenquellen für das Dashboard |

2. Kontrolle:
   - `10_bronze_laden`, letzte Zelle: überall `ok = true`.
   - `20_silver_bereinigen`, letzte Zelle: P1 = 12, P2 = 153, P3 = 19, P4 = 43 Fehler.
   - `30_gold_sternschema`, letzte Zelle: 12'134 Sendungen in Silver und Fakten, alle `ohne_…`-Spalten = 0,
     Umsatz 153'541.10 CHF, Pünktlichkeit 93.8 %.

Der ETL-Lauf kann beliebig oft wiederholt werden. Das DQ-Protokoll erhält pro Lauf neue Zeilen.

---

## 7. Dashboard erstellen (Databricks AI/BI)

Das Dashboard selbst wird in der Oberfläche zusammengeklickt. Die Daten kommen aus den Sichten von `40_dashboard_views`.

### 7.1 Dashboard und Datenquellen anlegen

1. Links in der Seitenleiste **Dashboards** → **Create dashboard**. Oben den Namen setzen, z.B. *Post Versand – Auswertung*.
2. Oben auf den Reiter **Data** wechseln → **Create from SQL**. Folgende Abfrage eingeben, **Run** klicken und den
   Datensatz (Name links in der Liste) umbenennen. Für jede Zeile der Tabelle einen eigenen Datensatz anlegen:

   | Datensatz-Name | SQL |
   |---|---|
   | `kpi` | `SELECT * FROM workspace.post_dwh_gold.v_kpi_uebersicht` |
   | `f1_monat` | `SELECT * FROM workspace.post_dwh_gold.v_f1_brief_paket_monat` |
   | `f2_kanal` | `SELECT * FROM workspace.post_dwh_gold.v_f2_umsatz_kanal` |
   | `f2_digital` | `SELECT * FROM workspace.post_dwh_gold.v_f2_digital_quartal` |
   | `f3_service` | `SELECT * FROM workspace.post_dwh_gold.v_f3_puenktlichkeit_service` |
   | `f3_monat` | `SELECT * FROM workspace.post_dwh_gold.v_f3_puenktlichkeit_monat` |

### 7.2 Visualisierungen hinzufügen

Zurück auf die Seite (Reiter mit dem Seitennamen, z.B. *Untitled page*). Unten in der Werkzeugleiste
**Add a visualization** wählen, das Widget auf der Fläche platzieren und rechts im Konfigurationsbereich einstellen:

| Widget | Datensatz | Visualisierung | Einstellungen |
|---|---|---|---|
| Kennzahl Sendungen | `kpi` | Counter | Value: `sendungen` (SUM) |
| Kennzahl Umsatz | `kpi` | Counter | Value: `umsatz_chf` (SUM) |
| Kennzahl Pünktlichkeit | `kpi` | Counter | Value: `puenktlichkeit_prozent` (SUM) |
| F1 Sendungen pro Monat | `f1_monat` | Line | X: `monat_start` (Monthly), Y: `sendungen` (SUM), Color: `sendungsart` |
| F1 Umsatz pro Jahr | `f1_monat` | Bar | X: `jahr`, Y: `umsatz_chf` (SUM), Color: `sendungsart` |
| F2 Umsatz pro Kanal | `f2_kanal` | Bar | X: `kanal_name`, Y: `umsatz_chf` (SUM), Color: `kundentyp` |
| F2 Anteil digital | `f2_digital` | Line | X: `jahr_quartal`, Y: `digital_anteil_prozent` (SUM) |
| F3 Pünktlichkeit pro Service | `f3_service` | Bar | X: `zone_service`, Y: `puenktlichkeit_prozent` (SUM) |
| F3 Pünktlichkeit pro Monat | `f3_monat` | Line | X: `monat_start` (Monthly), Y: `puenktlichkeit_prozent` (SUM), Color: `zone` |

Tipps:
- Die Sichten sind bereits aggregiert. „SUM“ verändert die Werte deshalb nicht. Ausnahme: Bei `f1_monat` und
  `f2_kanal` werden mehrere Zeilen zusammengezählt (z.B. alle Monate eines Jahres), das ist gewollt.
- Mit **Add a text box** Überschriften pro Frage einfügen (z.B. „F1 – Brief vs. Paket“).
- Optional: **Add a filter** → Feld `jahr`, damit man das Dashboard auf ein Jahr einschränken kann.

### 7.3 Veröffentlichen und für die Abgabe sichern

1. Oben rechts **Publish** → das Dashboard wird mit dem Serverless SQL Warehouse ausgeführt.
2. Screenshots pro Frage für den Bericht (Kapitel 9) erstellen.
3. Optional ins Repository übernehmen: Dashboard über **⋮ → Export** als `.lvdash.json` speichern (oder per
   **⋮ → Move** in den Git-Ordner verschieben) und committen.

---

## 8. Beispieldaten neu generieren (optional)

Die Insert-Skripte `02`–`04` werden durch `scripts/generate_data.py` erzeugt. Der Zufallsgenerator hat einen fixen Seed,
deshalb entstehen bei jedem Lauf **identische** Daten.

```bash
python scripts/generate_data.py
```

Der Generator benötigt nur die Python-Standardbibliothek. Er gibt am Ende eine Zusammenfassung aus (Zeilen pro Tabelle,
Anzahl eingebauter Datenqualitätsprobleme).

---

## 9. Häufige Probleme

| Problem | Lösung |
|---|---|
| `SCHEMA_NOT_FOUND` / `TABLE_OR_VIEW_NOT_FOUND` | Zuerst `01_create_schema` ausführen; prüfen, ob der richtige Katalog gewählt ist. |
| `%run`: Notebook nicht gefunden | Alle `sql/*`-Dateien müssen im selben Ordner liegen und als Notebook importiert sein. |
| `PERMISSION_DENIED` beim `CREATE SCHEMA` | Einen Katalog wählen, in dem man Schreibrechte hat (Free Edition: `workspace`). |
| Insert sehr langsam | Prüfen, ob *Serverless* als Compute gewählt ist. Jede Insert-Zelle enthält 1000 Zeilen. |
| `post_dwh_…` nicht gefunden (Dashboard/ETL) | Zuerst `databricks/00_etl_alles` ausführen; im Dashboard-SQL den Katalog `workspace.` voranstellen. |
| Neu aufsetzen | Einfach `00_setup_alles` erneut ausführen. `01_create_schema` löscht das Schema `post_oltp` vorher vollständig. |
