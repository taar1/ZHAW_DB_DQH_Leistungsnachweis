-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 – Stammdaten laden
-- MAGIC Länder, Verkaufskanäle und Produkte.
-- MAGIC
-- MAGIC Generiert durch `scripts/generate_data.py` (Seed 20261130). Nicht von Hand ändern.

-- COMMAND ----------

-- Tabelle land: 24 Zeilen
TRUNCATE TABLE post_oltp.land;

-- COMMAND ----------

INSERT INTO post_oltp.land (land_code, land_name, zone) VALUES
  ('CH', 'Schweiz', 'CH'),
  ('LI', 'Liechtenstein', 'CH'),
  ('DE', 'Deutschland', 'EUROPA'),
  ('AT', 'Österreich', 'EUROPA'),
  ('FR', 'Frankreich', 'EUROPA'),
  ('IT', 'Italien', 'EUROPA'),
  ('NL', 'Niederlande', 'EUROPA'),
  ('ES', 'Spanien', 'EUROPA'),
  ('BE', 'Belgien', 'EUROPA'),
  ('PT', 'Portugal', 'EUROPA'),
  ('SE', 'Schweden', 'EUROPA'),
  ('PL', 'Polen', 'EUROPA'),
  ('GB', 'Vereinigtes Königreich', 'EUROPA'),
  ('US', 'Vereinigte Staaten', 'WELT'),
  ('CA', 'Kanada', 'WELT'),
  ('AU', 'Australien', 'WELT'),
  ('JP', 'Japan', 'WELT'),
  ('CN', 'China', 'WELT'),
  ('BR', 'Brasilien', 'WELT'),
  ('IN', 'Indien', 'WELT'),
  ('SG', 'Singapur', 'WELT'),
  ('AE', 'Vereinigte Arabische Emirate', 'WELT'),
  ('ZA', 'Südafrika', 'WELT'),
  ('TH', 'Thailand', 'WELT');

-- COMMAND ----------

-- Tabelle verkaufskanal: 7 Zeilen
TRUNCATE TABLE post_oltp.verkaufskanal;

-- COMMAND ----------

INSERT INTO post_oltp.verkaufskanal (kanal_id, kanal_name, kanal_typ) VALUES
  (1, 'Postfiliale', 'PHYSISCH'),
  (2, 'Filiale mit Partner', 'PHYSISCH'),
  (3, 'My Post 24-Automat', 'PHYSISCH'),
  (4, 'Abholung pick@home', 'PHYSISCH'),
  (5, 'Online-Porto / WebStamp', 'DIGITAL'),
  (6, 'Post-App', 'DIGITAL'),
  (7, 'Geschäftskundenportal / Versand-API', 'DIGITAL');

-- COMMAND ----------

-- Tabelle produkt: 37 Zeilen
TRUNCATE TABLE post_oltp.produkt;

-- COMMAND ----------

INSERT INTO post_oltp.produkt (produkt_id, produkt_name, sendungsart, zone, service, max_gewicht_g, laufzeit_soll_werktage, preis_chf) VALUES
  (1, 'Standardbrief A-Post (bis B5, 100 g)', 'BRIEF', 'CH', 'A-POST', 100, 1, 1.20),
  (2, 'Standardbrief B-Post (bis B5, 100 g)', 'BRIEF', 'CH', 'B-POST', 100, 3, 1.00),
  (3, 'Grossbrief A-Post (bis B4, 500 g)', 'BRIEF', 'CH', 'A-POST', 500, 1, 2.20),
  (4, 'Grossbrief B-Post (bis B4, 500 g)', 'BRIEF', 'CH', 'B-POST', 500, 3, 1.90),
  (5, 'Maxibrief A-Post (bis B4, 1 kg, dicker als 2 cm bis 5 cm)', 'BRIEF', 'CH', 'A-POST', 1000, 1, 4.70),
  (6, 'Maxibrief B-Post (bis B4, 1 kg, dicker als 2 cm bis 5 cm)', 'BRIEF', 'CH', 'B-POST', 1000, 3, 4.00),
  (7, 'Paket Economy S (bis 2 kg)', 'PAKET', 'CH', 'ECONOMY', 2000, 2, 9.00),
  (8, 'Paket Economy M (bis 10 kg)', 'PAKET', 'CH', 'ECONOMY', 10000, 2, 12.70),
  (9, 'Paket Economy L (bis 30 kg)', 'PAKET', 'CH', 'ECONOMY', 30000, 2, 25.00),
  (10, 'Paket Priority S (bis 2 kg)', 'PAKET', 'CH', 'PRIORITY', 2000, 1, 11.00),
  (11, 'Paket Priority M (bis 10 kg)', 'PAKET', 'CH', 'PRIORITY', 10000, 1, 14.70),
  (12, 'Paket Priority L (bis 30 kg)', 'PAKET', 'CH', 'PRIORITY', 30000, 1, 27.00),
  (13, 'Paket Same Day (bis 30 kg)', 'PAKET', 'CH', 'SAME-DAY', 30000, 0, 49.00),
  (14, 'Brief International Europa Priority (bis 20 g)', 'BRIEF', 'EUROPA', 'PRIORITY', 20, 3, 2.40),
  (15, 'Brief International Europa Economy (bis 20 g)', 'BRIEF', 'EUROPA', 'ECONOMY', 20, 6, 2.20),
  (16, 'Brief International Europa Priority (bis 250 g)', 'BRIEF', 'EUROPA', 'PRIORITY', 250, 3, 6.00),
  (17, 'Brief International Europa Economy (bis 250 g)', 'BRIEF', 'EUROPA', 'ECONOMY', 250, 6, 5.00),
  (18, 'Brief International Europa Priority (bis 2 kg)', 'BRIEF', 'EUROPA', 'PRIORITY', 2000, 3, 18.00),
  (19, 'Brief International Europa Economy (bis 2 kg)', 'BRIEF', 'EUROPA', 'ECONOMY', 2000, 6, 15.00),
  (20, 'Brief International Welt Priority (bis 20 g)', 'BRIEF', 'WELT', 'PRIORITY', 20, 6, 2.70),
  (21, 'Brief International Welt Economy (bis 20 g)', 'BRIEF', 'WELT', 'ECONOMY', 20, 15, 2.50),
  (22, 'Brief International Welt Priority (bis 250 g)', 'BRIEF', 'WELT', 'PRIORITY', 250, 6, 9.00),
  (23, 'Brief International Welt Economy (bis 250 g)', 'BRIEF', 'WELT', 'ECONOMY', 250, 15, 7.50),
  (24, 'Brief International Welt Priority (bis 2 kg)', 'BRIEF', 'WELT', 'PRIORITY', 2000, 6, 32.00),
  (25, 'Brief International Welt Economy (bis 2 kg)', 'BRIEF', 'WELT', 'ECONOMY', 2000, 15, 26.00),
  (26, 'Paket International Europa Priority (bis 2 kg)', 'PAKET', 'EUROPA', 'PRIORITY', 2000, 4, 38.00),
  (27, 'Paket International Europa Economy (bis 2 kg)', 'PAKET', 'EUROPA', 'ECONOMY', 2000, 8, 31.00),
  (28, 'Paket International Europa Priority (bis 10 kg)', 'PAKET', 'EUROPA', 'PRIORITY', 10000, 4, 62.00),
  (29, 'Paket International Europa Economy (bis 10 kg)', 'PAKET', 'EUROPA', 'ECONOMY', 10000, 8, 52.00),
  (30, 'Paket International Europa Priority (bis 30 kg)', 'PAKET', 'EUROPA', 'PRIORITY', 30000, 4, 115.00),
  (31, 'Paket International Europa Economy (bis 30 kg)', 'PAKET', 'EUROPA', 'ECONOMY', 30000, 8, 98.00),
  (32, 'Paket International Welt Priority (bis 2 kg)', 'PAKET', 'WELT', 'PRIORITY', 2000, 7, 52.00),
  (33, 'Paket International Welt Economy (bis 2 kg)', 'PAKET', 'WELT', 'ECONOMY', 2000, 18, 42.00),
  (34, 'Paket International Welt Priority (bis 10 kg)', 'PAKET', 'WELT', 'PRIORITY', 10000, 7, 110.00),
  (35, 'Paket International Welt Economy (bis 10 kg)', 'PAKET', 'WELT', 'ECONOMY', 10000, 18, 88.00),
  (36, 'Paket International Welt Priority (bis 30 kg)', 'PAKET', 'WELT', 'PRIORITY', 30000, 7, 230.00),
  (37, 'Paket International Welt Economy (bis 30 kg)', 'PAKET', 'WELT', 'ECONOMY', 30000, 18, 185.00);
