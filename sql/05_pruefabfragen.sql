-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 – Prüfabfragen
-- MAGIC
-- MAGIC 1. Zeilenzahlen pro Tabelle (Kontrolle, ob das Laden vollständig war)
-- MAGIC 2. Erkennen der absichtlich eingebauten Datenqualitätsprobleme (Grundlage für die DQ-Prüfungen im Silver-Layer)
-- MAGIC 3. Einige fachliche Beispielabfragen

-- COMMAND ----------

-- 1) Zeilenzahlen – erwartete Werte siehe INSTALL.md
SELECT 'land' AS tabelle, COUNT(*) AS anzahl FROM post_oltp.land
UNION ALL SELECT 'verkaufskanal', COUNT(*) FROM post_oltp.verkaufskanal
UNION ALL SELECT 'produkt', COUNT(*) FROM post_oltp.produkt
UNION ALL SELECT 'kunde', COUNT(*) FROM post_oltp.kunde
UNION ALL SELECT 'empfaenger', COUNT(*) FROM post_oltp.empfaenger
UNION ALL SELECT 'auftrag', COUNT(*) FROM post_oltp.auftrag
UNION ALL SELECT 'sendung', COUNT(*) FROM post_oltp.sendung;

-- COMMAND ----------

-- 2) Datenqualität – Übersicht aller bekannten Problemtypen
SELECT 'kunde.email fehlt oder ungültig' AS pruefung, COUNT(*) AS anzahl
FROM post_oltp.kunde
WHERE email IS NULL OR NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
UNION ALL
SELECT 'kunde.plz ungültig (CH/LI: 4 Ziffern)', COUNT(*)
FROM post_oltp.kunde
WHERE land_code IN ('CH', 'LI') AND (plz IS NULL OR NOT plz RLIKE '^[0-9]{4}$')
UNION ALL
SELECT 'kunde mögliche Dublette (gleicher Name + Strasse)', COUNT(*) - COUNT(DISTINCT lower(vorname), lower(nachname), lower(strasse))
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
SELECT k.kanal_name, k.kanal_typ, ku.kundentyp,
       COUNT(DISTINCT a.auftrag_id) AS auftraege, SUM(a.betrag_total_chf) AS umsatz_chf
FROM post_oltp.auftrag a
JOIN post_oltp.verkaufskanal k ON a.kanal_id = k.kanal_id
JOIN post_oltp.kunde ku ON a.kunde_id = ku.kunde_id
GROUP BY ALL
ORDER BY umsatz_chf DESC;

-- COMMAND ----------

-- 3c) Pünktlichkeit (zugestellt bis Soll-Datum) pro Zone und Service
SELECT p.zone, p.service,
       COUNT(*) AS zugestellt,
       ROUND(AVG(CASE WHEN s.zustell_datum <= s.soll_zustell_datum THEN 1 ELSE 0 END) * 100, 1) AS puenktlich_prozent
FROM post_oltp.sendung s
JOIN post_oltp.produkt p ON s.produkt_id = p.produkt_id
WHERE s.status = 'ZUGESTELLT' AND s.zustell_datum >= s.aufgabe_datum
GROUP BY ALL
ORDER BY p.zone, p.service;

-- COMMAND ----------

-- 3d) Alle Sendungen eines Auftrags (ID-Beziehung Auftrag → Sendung)
SELECT a.auftrag_id, a.auftrag_datum, ku.firmenname, k.kanal_name,
       s.position_nr, s.tracking_nr, p.produkt_name, e.name AS empfaenger, e.land_code, s.preis_chf, s.status
FROM post_oltp.auftrag a
JOIN post_oltp.kunde ku ON a.kunde_id = ku.kunde_id
JOIN post_oltp.verkaufskanal k ON a.kanal_id = k.kanal_id
JOIN post_oltp.sendung s ON s.auftrag_id = a.auftrag_id
JOIN post_oltp.produkt p ON s.produkt_id = p.produkt_id
LEFT JOIN post_oltp.empfaenger e ON s.empfaenger_id = e.empfaenger_id
WHERE a.auftrag_id = (SELECT auftrag_id FROM post_oltp.sendung GROUP BY auftrag_id HAVING COUNT(*) >= 4 ORDER BY auftrag_id LIMIT 1)
ORDER BY s.position_nr;
