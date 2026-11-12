-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 40 – Dashboard: Sichten für die Fragestellungen F1–F3
-- MAGIC
-- MAGIC Erstellt im Schema `post_dwh_gold` je Fragestellung eine Sicht (View). Das Dashboard verwendet diese Sichten
-- MAGIC als Datasets (`SELECT * FROM post_dwh_gold.v_…`). So liegt die Logik versioniert im Repository und nicht nur im
-- MAGIC Dashboard.
-- MAGIC
-- MAGIC | Sicht | Frage | Visualisierung |
-- MAGIC |---|---|---|
-- MAGIC | `v_kpi_uebersicht` | – | Kennzahlen-Kacheln (Counter) |
-- MAGIC | `v_f1_brief_paket_monat` | F1 Brief vs. Paket | Linie: Sendungen pro Monat, Farbe = Sendungsart |
-- MAGIC | `v_f2_umsatz_kanal` | F2 Verkaufskanäle | Balken: Umsatz pro Kanal, Farbe = Kundentyp |
-- MAGIC | `v_f2_digital_quartal` | F2 Verkaufskanäle | Linie: Anteil digitaler Kanäle pro Quartal |
-- MAGIC | `v_f3_puenktlichkeit_service` | F3 Pünktlichkeit (Pakete) | Balken: Quote pro Zone und Service |
-- MAGIC | `v_f3_puenktlichkeit_monat` | F3 Pünktlichkeit (Pakete) | Linie: Quote pro Monat, Farbe = Zone |

-- COMMAND ----------

CREATE OR REPLACE VIEW post_dwh_gold.v_kpi_uebersicht AS
SELECT
  SUM(f.anzahl_sendungen)                                                         AS sendungen,
  SUM(f.umsatz_chf)                                                               AS umsatz_chf,
  ROUND(SUM(f.umsatz_chf) / SUM(f.anzahl_sendungen), 2)                           AS umsatz_pro_sendung_chf,
  ROUND(100 * SUM(CASE WHEN k.kanal_typ = 'DIGITAL' THEN f.umsatz_chf ELSE 0 END) / SUM(f.umsatz_chf), 1) AS digital_anteil_prozent,
  ROUND(100 * AVG(f.puenktlich_flag), 1)                                          AS puenktlichkeit_prozent
FROM post_dwh_gold.fact_sendung f
JOIN post_dwh_gold.dim_kanal k ON f.kanal_id = k.kanal_id;

-- COMMAND ----------

-- F1: Sendungen und Umsatz pro Monat und Sendungsart
CREATE OR REPLACE VIEW post_dwh_gold.v_f1_brief_paket_monat AS
SELECT d.monat_start, d.jahr, p.sendungsart,
       SUM(f.anzahl_sendungen) AS sendungen,
       SUM(f.umsatz_chf)       AS umsatz_chf
FROM post_dwh_gold.fact_sendung f
JOIN post_dwh_gold.dim_datum d   ON f.datum_key = d.datum_key
JOIN post_dwh_gold.dim_produkt p ON f.produkt_id = p.produkt_id
GROUP BY d.monat_start, d.jahr, p.sendungsart;

-- COMMAND ----------

-- F2a: Umsatz pro Verkaufskanal und Kundentyp
CREATE OR REPLACE VIEW post_dwh_gold.v_f2_umsatz_kanal AS
SELECT c.kanal_name, c.kanal_typ, k.kundentyp, d.jahr,
       SUM(f.anzahl_sendungen) AS sendungen,
       SUM(f.umsatz_chf)       AS umsatz_chf
FROM post_dwh_gold.fact_sendung f
JOIN post_dwh_gold.dim_kanal c ON f.kanal_id = c.kanal_id
JOIN post_dwh_gold.dim_kunde k ON f.kunde_id = k.kunde_id
JOIN post_dwh_gold.dim_datum d ON f.datum_key = d.datum_key
GROUP BY c.kanal_name, c.kanal_typ, k.kundentyp, d.jahr;

-- COMMAND ----------

-- F2b: Anteil der digitalen Kanäle am Umsatz pro Quartal
CREATE OR REPLACE VIEW post_dwh_gold.v_f2_digital_quartal AS
SELECT d.jahr_quartal,
       ROUND(100 * SUM(CASE WHEN c.kanal_typ = 'DIGITAL' THEN f.umsatz_chf ELSE 0 END) / SUM(f.umsatz_chf), 1) AS digital_anteil_prozent
FROM post_dwh_gold.fact_sendung f
JOIN post_dwh_gold.dim_kanal c ON f.kanal_id = c.kanal_id
JOIN post_dwh_gold.dim_datum d ON f.datum_key = d.datum_key
GROUP BY d.jahr_quartal;

-- COMMAND ----------

-- F3a: Pünktlichkeitsquote pro Zone und Service (nur Pakete, Briefe haben kein Tracking)
CREATE OR REPLACE VIEW post_dwh_gold.v_f3_puenktlichkeit_service AS
SELECT p.zone, p.service, concat(p.zone, ' ', p.service) AS zone_service,
       COUNT(f.puenktlich_flag)                AS zugestellt,
       ROUND(100 * AVG(f.puenktlich_flag), 1)  AS puenktlichkeit_prozent
FROM post_dwh_gold.fact_sendung f
JOIN post_dwh_gold.dim_produkt p ON f.produkt_id = p.produkt_id
WHERE f.puenktlich_flag IS NOT NULL
GROUP BY p.zone, p.service;

-- COMMAND ----------

-- F3b: Pünktlichkeitsquote pro Monat und Zone (nur Pakete; Dezember-Effekt)
CREATE OR REPLACE VIEW post_dwh_gold.v_f3_puenktlichkeit_monat AS
SELECT d.monat_start, p.zone,
       COUNT(f.puenktlich_flag)                AS zugestellt,
       ROUND(100 * AVG(f.puenktlich_flag), 1)  AS puenktlichkeit_prozent
FROM post_dwh_gold.fact_sendung f
JOIN post_dwh_gold.dim_datum d   ON f.datum_key = d.datum_key
JOIN post_dwh_gold.dim_produkt p ON f.produkt_id = p.produkt_id
WHERE f.puenktlich_flag IS NOT NULL
GROUP BY d.monat_start, p.zone;

-- COMMAND ----------

-- Kontrolle: Kennzahlen-Übersicht
SELECT * FROM post_dwh_gold.v_kpi_uebersicht;
