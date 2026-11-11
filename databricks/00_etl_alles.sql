-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 – Kompletter ETL-Lauf (Bronze → Silver → Gold → Dashboard-Sichten)
-- MAGIC
-- MAGIC Führt die ETL-Notebooks in der richtigen Reihenfolge aus (Dauer auf Serverless ca. 1–2 Minuten).
-- MAGIC Voraussetzung: Die operative Datenbank wurde mit `sql/00_setup_alles` aufgesetzt.

-- COMMAND ----------

-- MAGIC %run ./10_bronze_laden

-- COMMAND ----------

-- MAGIC %run ./20_silver_bereinigen

-- COMMAND ----------

-- MAGIC %run ./30_gold_sternschema

-- COMMAND ----------

-- MAGIC %run ./40_dashboard_views
