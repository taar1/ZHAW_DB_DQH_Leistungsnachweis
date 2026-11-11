-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 – Komplettes Setup der operativen Datenbank
-- MAGIC
-- MAGIC Führt alle Skripte in der richtigen Reihenfolge aus (Dauer auf Serverless ca. 1–2 Minuten).
-- MAGIC Voraussetzung: Alle Dateien aus dem Ordner `sql/` liegen als **Notebooks** im selben Workspace-Ordner
-- MAGIC (z.B. über einen Git-Ordner oder Import, siehe `INSTALL.md`).
-- MAGIC
-- MAGIC Das Schema `post_oltp` wird im aktuell gewählten Katalog (Free Edition: `workspace`) neu erstellt.

-- COMMAND ----------

-- MAGIC %run ./01_create_schema

-- COMMAND ----------

-- MAGIC %run ./02_insert_stammdaten

-- COMMAND ----------

-- MAGIC %run ./03_insert_kunden_empfaenger

-- COMMAND ----------

-- MAGIC %run ./04_insert_auftraege_sendungen

-- COMMAND ----------

-- MAGIC %run ./05_pruefabfragen
