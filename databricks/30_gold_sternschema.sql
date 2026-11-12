-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 30 – Gold: Sternschema aufbauen
-- MAGIC
-- MAGIC Baut aus den Silver-Tabellen das Sternschema für das Geschäftsobjekt **Sendung**:
-- MAGIC
-- MAGIC * Dimensionen: `dim_datum`, `dim_kunde`, `dim_produkt`, `dim_kanal`, `dim_zielland`
-- MAGIC * Fakten: `fact_sendung` (Grain: 1 Zeile = 1 Sendung)
-- MAGIC
-- MAGIC Als Schlüssel dienen die IDs der operativen DB, für das Datum der Schlüssel `YYYYMMDD` (SCD Typ 1, Full Load).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS post_dwh_gold
  COMMENT 'DWH Gold-Layer: Sternschema Sendung für Auswertungen und Dashboard';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimension Datum
-- MAGIC Wird als Kalender generiert (ein Eintrag pro Tag), damit auch Tage ohne Sendungen vorhanden sind.

-- COMMAND ----------

CREATE OR REPLACE TABLE post_dwh_gold.dim_datum AS
SELECT
  CAST(date_format(datum, 'yyyyMMdd') AS INT)   AS datum_key,
  datum,
  year(datum)                                   AS jahr,
  quarter(datum)                                AS quartal,
  concat(year(datum), '-Q', quarter(datum))     AS jahr_quartal,
  month(datum)                                  AS monat,
  element_at(array('Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli',
                   'August', 'September', 'Oktober', 'November', 'Dezember'), month(datum)) AS monat_name,
  trunc(datum, 'MM')                            AS monat_start,
  element_at(array('Sonntag', 'Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag'),
             dayofweek(datum))                  AS wochentag
FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS datum);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Übrige Dimensionen

-- COMMAND ----------

CREATE OR REPLACE TABLE post_dwh_gold.dim_kunde AS
SELECT
  kunde_id,
  kundentyp,
  coalesce(firmenname, concat_ws(' ', vorname, nachname)) AS name,
  ort,
  land_code
FROM post_dwh_silver.kunde;

CREATE OR REPLACE TABLE post_dwh_gold.dim_produkt AS
SELECT produkt_id, produkt_name, sendungsart, zone, service, laufzeit_soll_werktage
FROM post_dwh_silver.produkt;

CREATE OR REPLACE TABLE post_dwh_gold.dim_kanal AS
SELECT kanal_id, kanal_name, kanal_typ
FROM post_dwh_silver.verkaufskanal;

-- Zielland inkl. Eintrag "Unbekannt" für Sendungen mit fehlendem Empfänger (DQ-Prüfung P1)
CREATE OR REPLACE TABLE post_dwh_gold.dim_zielland AS
SELECT land_code, land_name, zone FROM post_dwh_silver.land
UNION ALL
SELECT 'XX', 'Unbekannt', 'UNBEKANNT';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Faktentabelle Sendung
-- MAGIC * `laufzeit_werktage`: Werktage (Mo–Fr) zwischen Aufgabe und Zustellung; Feiertage werden vereinfacht nicht abgezogen.
-- MAGIC * `puenktlich_flag`: 1 = bis zum Soll-Datum zugestellt, 0 = verspätet, NULL = nicht (gültig) zugestellt.
-- MAGIC   Der Durchschnitt ergibt direkt die Pünktlichkeitsquote.
-- MAGIC * Briefe haben keine Sendungsnummer und kein erfasstes Zustelldatum: `tracking_nr`, `status`, `laufzeit_werktage`
-- MAGIC   und `puenktlich_flag` sind bei Briefen NULL. Die Pünktlichkeit wird deshalb nur für Pakete ausgewiesen.

-- COMMAND ----------

CREATE OR REPLACE TABLE post_dwh_gold.fact_sendung AS
SELECT
  s.auftrag_id,
  s.position_nr,
  CAST(date_format(s.aufgabe_datum, 'yyyyMMdd') AS INT)  AS datum_key,
  a.kunde_id,
  s.produkt_id,
  a.kanal_id,
  s.ziel_land_code                                      AS land_code,
  s.tracking_nr,
  s.status,
  1                                                     AS anzahl_sendungen,
  s.preis_chf                                           AS umsatz_chf,
  s.gewicht_g,
  CASE
    WHEN s.zustell_datum IS NULL THEN NULL
    WHEN s.zustell_datum = s.aufgabe_datum THEN 0
    ELSE size(filter(sequence(date_add(s.aufgabe_datum, 1), s.zustell_datum), d -> dayofweek(d) NOT IN (1, 7)))
  END                                                   AS laufzeit_werktage,
  CASE
    WHEN s.status = 'ZUGESTELLT' AND s.zustell_datum IS NOT NULL
      THEN CASE WHEN s.zustell_datum <= s.soll_zustell_datum THEN 1 ELSE 0 END
  END                                                   AS puenktlich_flag
FROM post_dwh_silver.sendung s
JOIN post_dwh_silver.auftrag a ON s.auftrag_id = a.auftrag_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Kontrolle
-- MAGIC Jede Sendung aus Silver muss genau einmal in den Fakten sein, und jeder Fremdschlüssel muss eine Dimension finden
-- MAGIC (alle Werte in den Spalten `ohne_…` = 0).

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM post_dwh_silver.sendung)                                           AS sendungen_silver,
  COUNT(*)                                                                                 AS sendungen_fakten,
  count_if(d.datum_key IS NULL)                                                            AS ohne_datum,
  count_if(k.kunde_id IS NULL)                                                             AS ohne_kunde,
  count_if(p.produkt_id IS NULL)                                                           AS ohne_produkt,
  count_if(c.kanal_id IS NULL)                                                             AS ohne_kanal,
  count_if(z.land_code IS NULL)                                                            AS ohne_zielland,
  SUM(f.umsatz_chf)                                                                        AS umsatz_total_chf,
  ROUND(AVG(f.puenktlich_flag) * 100, 1)                                                   AS puenktlichkeit_prozent
FROM post_dwh_gold.fact_sendung f
LEFT JOIN post_dwh_gold.dim_datum d    ON f.datum_key = d.datum_key
LEFT JOIN post_dwh_gold.dim_kunde k    ON f.kunde_id = k.kunde_id
LEFT JOIN post_dwh_gold.dim_produkt p  ON f.produkt_id = p.produkt_id
LEFT JOIN post_dwh_gold.dim_kanal c    ON f.kanal_id = c.kanal_id
LEFT JOIN post_dwh_gold.dim_zielland z ON f.land_code = z.land_code;
