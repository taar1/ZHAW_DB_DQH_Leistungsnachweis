-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 20 – Silver: Bereinigen und Datenqualität prüfen
-- MAGIC
-- MAGIC Liest die Bronze-Tabellen, bereinigt und standardisiert die Daten und führt die **Datenqualitätsprüfungen** durch.
-- MAGIC Fehlerhafte Datensätze werden nicht gelöscht, sondern mit einem DQ-Flag markiert und behandelt:
-- MAGIC
-- MAGIC | ID | Prüfung | Regel | Reaktion |
-- MAGIC |---|---|---|---|
-- MAGIC | P1 | Referentielle Integrität | `sendung.empfaenger_id` existiert in `empfaenger` | Zielland = `XX` (unbekannt) |
-- MAGIC | P2 | Plausibilität Gewicht | `gewicht_g > 0` und `<= produkt.max_gewicht_g` | Gewicht = NULL |
-- MAGIC | P3 | Plausibilität Datum | `zustell_datum >= aufgabe_datum` | Zustelldatum = NULL (keine Pünktlichkeit) |
-- MAGIC | P4 | Abstimmung Auftragstotal | `betrag_total_chf = Σ sendung.preis_chf` | Summe der Sendungen gilt |
-- MAGIC
-- MAGIC Zusätzlich: Ortsnamen vereinheitlichen (z.B. „Zuerich“ → „Zürich“), E-Mail trimmen und klein schreiben.
-- MAGIC Das Ergebnis jeder Prüfung wird in `post_dwh_silver.dq_protokoll` geschrieben (eine Zeile pro Prüfung und Lauf).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS post_dwh_silver
  COMMENT 'DWH Silver-Layer: bereinigte und geprüfte Daten';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Stammdaten (keine Bereinigung nötig)

-- COMMAND ----------

CREATE OR REPLACE TABLE post_dwh_silver.land AS
SELECT land_code, land_name, zone FROM post_dwh_bronze.land;

CREATE OR REPLACE TABLE post_dwh_silver.verkaufskanal AS
SELECT kanal_id, kanal_name, kanal_typ FROM post_dwh_bronze.verkaufskanal;

CREATE OR REPLACE TABLE post_dwh_silver.produkt AS
SELECT produkt_id, produkt_name, sendungsart, zone, service, max_gewicht_g, laufzeit_soll_werktage, preis_chf
FROM post_dwh_bronze.produkt;

CREATE OR REPLACE TABLE post_dwh_silver.empfaenger AS
SELECT empfaenger_id, trim(name) AS name, trim(strasse) AS strasse, trim(plz) AS plz, trim(ort) AS ort, land_code
FROM post_dwh_bronze.empfaenger;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Kunde: Ort vereinheitlichen, E-Mail standardisieren
-- MAGIC Ortsnamen werden zuerst über eine Liste bekannter Varianten (Sprache, Schreibweise) korrigiert, danach über die
-- MAGIC korrekt geschriebenen Orte aus den Empfängeradressen (Gross-/Kleinschreibung, z.B. „ZÜRICH“ → „Zürich“).

-- COMMAND ----------

CREATE OR REPLACE TABLE post_dwh_silver.kunde AS
WITH ort_varianten AS (
  SELECT * FROM VALUES
    ('Zuerich', 'Zürich'), ('Zurich', 'Zürich'),
    ('Geneve', 'Genève'), ('Genf', 'Genève'),
    ('Neuchatel', 'Neuchâtel'), ('Neuenburg', 'Neuchâtel'),
    ('Lucerne', 'Luzern'), ('Berne', 'Bern'), ('Bâle', 'Basel'),
    ('St.Gallen', 'St. Gallen'), ('Sankt Gallen', 'St. Gallen'), ('St Gallen', 'St. Gallen')
    AS t(variante, ort)
),
ort_referenz AS (
  SELECT DISTINCT ort FROM post_dwh_bronze.empfaenger WHERE land_code IN ('CH', 'LI')
)
SELECT
  k.kunde_id,
  k.kundentyp,
  trim(k.vorname)                                   AS vorname,
  trim(k.nachname)                                  AS nachname,
  trim(k.firmenname)                                AS firmenname,
  lower(trim(k.email))                              AS email,
  coalesce(lower(trim(k.email)) RLIKE '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$', false) AS email_gueltig,
  trim(k.strasse)                                   AS strasse,
  trim(k.plz)                                       AS plz,
  coalesce(v.ort, r.ort, trim(k.ort))               AS ort,
  k.ort                                             AS ort_original,
  k.land_code,
  k.registriert_am
FROM post_dwh_bronze.kunde k
LEFT JOIN ort_varianten v ON upper(v.variante) = upper(trim(k.ort))
LEFT JOIN ort_referenz  r ON upper(r.ort)      = upper(trim(k.ort));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sendung: Prüfungen P1–P3

-- COMMAND ----------

CREATE OR REPLACE TABLE post_dwh_silver.sendung AS
WITH geprueft AS (
  SELECT
    s.*,
    e.land_code                                                       AS empfaenger_land_code,
    e.empfaenger_id IS NOT NULL                                       AS dq_empfaenger_ok,  -- P1
    coalesce(s.gewicht_g > 0 AND s.gewicht_g <= p.max_gewicht_g, false) AS dq_gewicht_ok,  -- P2
    (s.zustell_datum IS NULL OR s.zustell_datum >= s.aufgabe_datum)   AS dq_datum_ok         -- P3
  FROM post_dwh_bronze.sendung s
  JOIN post_dwh_bronze.produkt p         ON s.produkt_id = p.produkt_id
  LEFT JOIN post_dwh_bronze.empfaenger e ON s.empfaenger_id = e.empfaenger_id
)
SELECT
  auftrag_id,
  position_nr,
  trim(tracking_nr)                                      AS tracking_nr,
  produkt_id,
  empfaenger_id,
  coalesce(empfaenger_land_code, 'XX')                   AS ziel_land_code,
  CASE WHEN dq_gewicht_ok THEN gewicht_g END             AS gewicht_g,
  gewicht_g                                              AS gewicht_g_original,
  preis_chf,
  aufgabe_datum,
  soll_zustell_datum,
  CASE WHEN dq_datum_ok THEN zustell_datum END           AS zustell_datum,
  status,
  dq_empfaenger_ok,
  dq_gewicht_ok,
  dq_datum_ok
FROM geprueft;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Auftrag: Prüfung P4 (Abstimmung mit den Sendungen)

-- COMMAND ----------

CREATE OR REPLACE TABLE post_dwh_silver.auftrag AS
SELECT
  a.auftrag_id,
  a.kunde_id,
  a.kanal_id,
  a.auftrag_datum,
  coalesce(s.summe_chf, 0)                               AS betrag_total_chf,
  a.betrag_total_chf                                     AS betrag_erfasst_chf,
  coalesce(a.betrag_total_chf = s.summe_chf, false)      AS dq_betrag_ok
FROM post_dwh_bronze.auftrag a
LEFT JOIN (
  SELECT auftrag_id, SUM(preis_chf) AS summe_chf FROM post_dwh_bronze.sendung GROUP BY auftrag_id
) s ON a.auftrag_id = s.auftrag_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DQ-Protokoll schreiben
-- MAGIC Die Tabelle wird nicht überschrieben, sondern pro Lauf ergänzt. So bleibt die Entwicklung der Datenqualität sichtbar.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS post_dwh_silver.dq_protokoll (
  lauf_zeitpunkt   TIMESTAMP COMMENT 'Ladezeitpunkt des Bronze-Laufs',
  pruefung         STRING    COMMENT 'ID und Name der Prüfung',
  tabelle          STRING    COMMENT 'Geprüfte Tabelle',
  anzahl_geprueft  BIGINT    COMMENT 'Anzahl geprüfte Datensätze',
  anzahl_fehler    BIGINT    COMMENT 'Anzahl Datensätze, die die Regel verletzen'
) COMMENT 'Protokoll der Datenqualitätsprüfungen im Silver-Layer';

-- COMMAND ----------

INSERT INTO post_dwh_silver.dq_protokoll
WITH lauf AS (SELECT max(_geladen_am) AS ts FROM post_dwh_bronze.sendung)
SELECT (SELECT ts FROM lauf) AS lauf_zeitpunkt, 'P1 Referentielle Integrität Empfänger' AS pruefung, 'sendung' AS tabelle,
       COUNT(*) AS anzahl_geprueft, count_if(NOT dq_empfaenger_ok) AS anzahl_fehler
FROM post_dwh_silver.sendung
UNION ALL
SELECT (SELECT ts FROM lauf), 'P2 Plausibilität Gewicht', 'sendung', COUNT(*), count_if(NOT dq_gewicht_ok)
FROM post_dwh_silver.sendung
UNION ALL
SELECT (SELECT ts FROM lauf), 'P3 Plausibilität Zustelldatum', 'sendung', COUNT(*), count_if(NOT dq_datum_ok)
FROM post_dwh_silver.sendung
UNION ALL
SELECT (SELECT ts FROM lauf), 'P4 Abstimmung Auftragstotal', 'auftrag', COUNT(*), count_if(NOT dq_betrag_ok)
FROM post_dwh_silver.auftrag
UNION ALL
SELECT (SELECT ts FROM lauf), 'Info: Ort vereinheitlicht', 'kunde', COUNT(*), count_if(ort <> ort_original)
FROM post_dwh_silver.kunde
UNION ALL
SELECT (SELECT ts FROM lauf), 'Info: E-Mail fehlt oder ungültig', 'kunde', COUNT(*), count_if(NOT email_gueltig)
FROM post_dwh_silver.kunde;

-- COMMAND ----------

-- Ergebnis des aktuellen Laufs (erwartet: P1 = 12, P2 = 153, P3 = 7, P4 = 43)
SELECT pruefung, tabelle, anzahl_geprueft, anzahl_fehler,
       ROUND(100.0 * anzahl_fehler / anzahl_geprueft, 2) AS fehler_prozent
FROM post_dwh_silver.dq_protokoll
WHERE lauf_zeitpunkt = (SELECT max(lauf_zeitpunkt) FROM post_dwh_silver.dq_protokoll)
ORDER BY pruefung;
