# Installationsanleitung

Diese Anleitung richtet sich an das Projektteam **und** an die prüfende Person (Dozent/in). Sie beschreibt,
wie die operative Datenbank `post_oltp` (und später das Datawarehouse) in **Databricks** aufgesetzt und
ausgeführt wird.

> Getestet für die **Databricks Free Edition** (Serverless Compute, Unity Catalog mit Standardkatalog `workspace`).
> In anderen Workspaces funktioniert es gleich, sofern ein Katalog mit Schreibrechten gewählt ist.

---

## 1. Voraussetzungen

| Was | Details |
|---|---|
| Databricks-Account | Free Edition genügt: <https://www.databricks.com/learn/free-edition> |
| Compute | *Serverless* (in der Free Edition automatisch vorhanden) |
| Katalog | `workspace` (Standard in der Free Edition). Es werden die Schemas `post_oltp` und später `post_dwh_bronze`, `post_dwh_silver`, `post_dwh_gold` angelegt. |
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
   | 1 | `sql/00_setup_alles` | Führt 01–06 automatisch nacheinander aus | 3–6 min |
   | – | *(alternativ einzeln:)* `01_create_schema` … `06_pruefabfragen` | | |
   | 2 | `databricks/…` | ETL Bronze → Silver → Gold *(folgt)* | |
   | 3 | Dashboard | *(folgt)* | |

4. Kontrolle: Im Notebook `06_pruefabfragen` müssen die Zeilenzahlen gemäss Kapitel 5 erscheinen.

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

Jede Datei kann auch im **SQL Editor** geöffnet und mit *Run all* ausgeführt werden (Reihenfolge 01 → 06).
Die grossen Insert-Dateien (> 1 MB) sind dort allerdings träge. Die Notebook-Variante ist deshalb vorzuziehen.

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

Nach erfolgreichem Setup liefert `06_pruefabfragen` folgende Werte:

| Tabelle | Zeilen |
|---|---:|
| land | 24 |
| verkaufskanal | 7 |
| produkt | 37 |
| zusatzleistung | 6 |
| standort | 175 |
| adresse | 3'650 |
| kunde | 658 |
| empfaenger | 3'000 |
| auftrag | 5'887 |
| sendung | 12'561 |
| sendung_zusatzleistung | 1'934 |
| tracking_event | 53'157 |

---

## 6. Beispieldaten neu generieren (optional)

Die Insert-Skripte `02`–`05` werden durch `scripts/generate_data.py` erzeugt. Der Zufallsgenerator hat einen fixen Seed,
deshalb entstehen bei jedem Lauf **identische** Daten.

```bash
python scripts/generate_data.py
```

Der Generator benötigt nur die Python-Standardbibliothek. Er gibt am Ende eine Zusammenfassung aus (Zeilen pro Tabelle,
Anzahl eingebauter Datenqualitätsprobleme). Werden Mengen oder Seed geändert, kann sich die Aufteilung der Dateien
(`05_insert_tracking_teilN.sql`) ändern. In diesem Fall die `%run`-Zellen in `00_setup_alles.sql` anpassen.

---

## 7. Häufige Probleme

| Problem | Lösung |
|---|---|
| `SCHEMA_NOT_FOUND` / `TABLE_OR_VIEW_NOT_FOUND` | Zuerst `01_create_schema` ausführen; prüfen, ob der richtige Katalog gewählt ist. |
| `%run`: Notebook nicht gefunden | Alle `sql/*`-Dateien müssen im selben Ordner liegen und als Notebook importiert sein. |
| `PERMISSION_DENIED` beim `CREATE SCHEMA` | Einen Katalog wählen, in dem man Schreibrechte hat (Free Edition: `workspace`). |
| Insert sehr langsam | Prüfen, ob *Serverless* als Compute gewählt ist. Jede Insert-Zelle enthält 1000 Zeilen. |
| Neu aufsetzen | Einfach `00_setup_alles` erneut ausführen. `01_create_schema` löscht das Schema `post_oltp` vorher vollständig. |
