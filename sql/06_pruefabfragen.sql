-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 – Prüfabfragen
-- MAGIC
-- MAGIC 1. Zeilenzahlen pro Tabelle (Kontrolle, ob das Laden vollständig war)
-- MAGIC 2. Erkennen der absichtlich eingebauten Datenqualitätsprobleme (Grundlage für die DQ-Prüfungen im Silver-Layer)
-- MAGIC 3. Einige fachliche Beispielabfragen

-- COMMAND ----------

-- 1) Zeilenzahlen – erwartete Werte siehe INSTALL.md
SELECT 'land' AS tabelle, COUNT(*) AS anzahl FROM post_oltp.land
UNION ALL SELECT 'verkaufskanal', COUNT(*) FROM post_oltp.verkaufskanal
UNION ALL SELECT 'produkt', COUNT(*) FROM post_oltp.produkt
UNION ALL SELECT 'zusatzleistung', COUNT(*) FROM post_oltp.zusatzleistung
UNION ALL SELECT 'standort', COUNT(*) FROM post_oltp.standort
UNION ALL SELECT 'adresse', COUNT(*) FROM post_oltp.adresse
UNION ALL SELECT 'kunde', COUNT(*) FROM post_oltp.kunde
UNION ALL SELECT 'empfaenger', COUNT(*) FROM post_oltp.empfaenger
UNION ALL SELECT 'auftrag', COUNT(*) FROM post_oltp.auftrag
UNION ALL SELECT 'sendung', COUNT(*) FROM post_oltp.sendung
UNION ALL SELECT 'sendung_zusatzleistung', COUNT(*) FROM post_oltp.sendung_zusatzleistung
UNION ALL SELECT 'tracking_event', COUNT(*) FROM post_oltp.tracking_event;

-- COMMAND ----------

-- 2) Datenqualität – Übersicht aller bekannten Problemtypen
SELECT 'kunde.email fehlt oder ungültig' AS pruefung, COUNT(*) AS anzahl
FROM post_oltp.kunde
WHERE email IS NULL OR NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
UNION ALL
SELECT 'adresse.plz ungültig (CH/LI: 4 Ziffern)', COUNT(*)
FROM post_oltp.adresse
WHERE land_code IN ('CH', 'LI') AND (plz IS NULL OR NOT plz RLIKE '^[0-9]{4}$')
UNION ALL
SELECT 'kunde.telefon nicht im Format +41 xx xxx xx xx', COUNT(*)
FROM post_oltp.kunde
WHERE telefon IS NULL OR NOT telefon RLIKE '^\\+41 [0-9]{2} [0-9]{3} [0-9]{2} [0-9]{2}$'
UNION ALL
SELECT 'kunde.geburtsdatum unplausibel', COUNT(*)
FROM post_oltp.kunde
WHERE geburtsdatum < DATE'1905-01-01' OR geburtsdatum > current_date()
UNION ALL
SELECT 'kunde mögliche Dublette (gleicher Name + Adresse)', COUNT(*) - COUNT(DISTINCT lower(vorname), lower(nachname), adresse_id)
FROM post_oltp.kunde
WHERE kundentyp = 'PRIVAT'
UNION ALL
SELECT 'sendung.gewicht_g fehlt oder 0', COUNT(*)
FROM post_oltp.sendung
WHERE gewicht_g IS NULL OR gewicht_g <= 0
UNION ALL
SELECT 'sendung.gewicht_g > Maximalgewicht Produkt', COUNT(*)
FROM post_oltp.sendung s JOIN post_oltp.produkt p ON s.produkt_id = p.produkt_id
WHERE s.gewicht_g > p.max_gewicht_g
UNION ALL
SELECT 'sendung.zustell_datum < aufgabe_datum', COUNT(*)
FROM post_oltp.sendung
WHERE zustell_datum < aufgabe_datum
UNION ALL
SELECT 'sendung.empfaenger_id ohne Empfänger (referentielle Integrität)', COUNT(*)
FROM post_oltp.sendung s LEFT ANTI JOIN post_oltp.empfaenger e ON s.empfaenger_id = e.empfaenger_id
UNION ALL
SELECT 'auftrag.betrag_total_chf ≠ Summe Sendungen', COUNT(*)
FROM post_oltp.auftrag a
JOIN (SELECT auftrag_id, SUM(preis_chf) AS summe FROM post_oltp.sendung GROUP BY auftrag_id) s
  ON a.auftrag_id = s.auftrag_id
WHERE a.betrag_total_chf <> s.summe;

-- COMMAND ----------

-- 3a) Umsatz und Sendungen pro Jahr, Sendungsart und Zone
SELECT year(s.aufgabe_datum) AS jahr, p.sendungsart, p.zone,
       COUNT(*) AS sendungen, SUM(s.preis_chf) AS umsatz_chf
FROM post_oltp.sendung s
JOIN post_oltp.produkt p ON s.produkt_id = p.produkt_id
GROUP BY ALL
ORDER BY jahr, sendungsart, zone;

-- COMMAND ----------

-- 3b) Umsatz pro Verkaufskanal und Kundentyp
SELECT k.kanal_name, ku.kundentyp, COUNT(DISTINCT a.auftrag_id) AS auftraege, SUM(a.betrag_total_chf) AS umsatz_chf
FROM post_oltp.auftrag a
JOIN post_oltp.verkaufskanal k ON a.kanal_id = k.kanal_id
JOIN post_oltp.kunde ku ON a.kunde_id = ku.kunde_id
GROUP BY ALL
ORDER BY umsatz_chf DESC;

-- COMMAND ----------

-- 3c) Pünktlichkeit (zugestellt bis Soll-Datum) pro Service
SELECT p.zone, p.service,
       COUNT(*) AS zugestellt,
       ROUND(AVG(CASE WHEN s.zustell_datum <= s.soll_zustell_datum THEN 1 ELSE 0 END) * 100, 1) AS puenktlich_prozent
FROM post_oltp.sendung s
JOIN post_oltp.produkt p ON s.produkt_id = p.produkt_id
WHERE s.status = 'ZUGESTELLT' AND s.zustell_datum >= s.aufgabe_datum
GROUP BY ALL
ORDER BY p.zone, p.service;

-- COMMAND ----------

-- 3d) Sendungsverfolgung einer einzelnen Sendung
SELECT s.tracking_nr, e.event_nr, e.event_zeitpunkt, e.event_code, e.event_text, st.standort_name
FROM post_oltp.sendung s
JOIN post_oltp.tracking_event e ON s.auftrag_id = e.auftrag_id AND s.position_nr = e.position_nr
LEFT JOIN post_oltp.standort st ON e.standort_id = st.standort_id
WHERE s.tracking_nr = (SELECT MIN(tracking_nr) FROM post_oltp.sendung WHERE tracking_nr LIKE 'CP%')
ORDER BY e.event_nr;
