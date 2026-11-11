-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10 – Bronze: Rohdaten laden
-- MAGIC
-- MAGIC Kopiert alle 7 Tabellen der operativen Datenbank `post_oltp` **unverändert** in das Schema `post_dwh_bronze`
-- MAGIC und ergänzt den Ladezeitpunkt `_geladen_am`.
-- MAGIC
-- MAGIC * Ladeart: Full Load (`CREATE OR REPLACE TABLE … AS SELECT`), das Notebook kann beliebig oft laufen.
-- MAGIC * Voraussetzung: `sql/00_setup_alles` wurde ausgeführt.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS post_dwh_bronze
  COMMENT 'DWH Bronze-Layer: unveränderte Kopie der operativen Datenbank post_oltp';

-- COMMAND ----------

CREATE OR REPLACE TABLE post_dwh_bronze.land          AS SELECT *, current_timestamp() AS _geladen_am FROM post_oltp.land;
CREATE OR REPLACE TABLE post_dwh_bronze.verkaufskanal AS SELECT *, current_timestamp() AS _geladen_am FROM post_oltp.verkaufskanal;
CREATE OR REPLACE TABLE post_dwh_bronze.produkt       AS SELECT *, current_timestamp() AS _geladen_am FROM post_oltp.produkt;
CREATE OR REPLACE TABLE post_dwh_bronze.kunde         AS SELECT *, current_timestamp() AS _geladen_am FROM post_oltp.kunde;
CREATE OR REPLACE TABLE post_dwh_bronze.empfaenger    AS SELECT *, current_timestamp() AS _geladen_am FROM post_oltp.empfaenger;
CREATE OR REPLACE TABLE post_dwh_bronze.auftrag       AS SELECT *, current_timestamp() AS _geladen_am FROM post_oltp.auftrag;
CREATE OR REPLACE TABLE post_dwh_bronze.sendung       AS SELECT *, current_timestamp() AS _geladen_am FROM post_oltp.sendung;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ladekontrolle
-- MAGIC Die Zeilenzahlen in Quelle und Bronze müssen übereinstimmen (Spalte `ok` = true).

-- COMMAND ----------

SELECT tabelle, quelle, bronze, quelle = bronze AS ok
FROM (
            SELECT 'land' AS tabelle, (SELECT COUNT(*) FROM post_oltp.land) AS quelle, (SELECT COUNT(*) FROM post_dwh_bronze.land) AS bronze
  UNION ALL SELECT 'verkaufskanal', (SELECT COUNT(*) FROM post_oltp.verkaufskanal), (SELECT COUNT(*) FROM post_dwh_bronze.verkaufskanal)
  UNION ALL SELECT 'produkt',       (SELECT COUNT(*) FROM post_oltp.produkt),       (SELECT COUNT(*) FROM post_dwh_bronze.produkt)
  UNION ALL SELECT 'kunde',         (SELECT COUNT(*) FROM post_oltp.kunde),         (SELECT COUNT(*) FROM post_dwh_bronze.kunde)
  UNION ALL SELECT 'empfaenger',    (SELECT COUNT(*) FROM post_oltp.empfaenger),    (SELECT COUNT(*) FROM post_dwh_bronze.empfaenger)
  UNION ALL SELECT 'auftrag',       (SELECT COUNT(*) FROM post_oltp.auftrag),       (SELECT COUNT(*) FROM post_dwh_bronze.auftrag)
  UNION ALL SELECT 'sendung',       (SELECT COUNT(*) FROM post_oltp.sendung),       (SELECT COUNT(*) FROM post_dwh_bronze.sendung)
);
