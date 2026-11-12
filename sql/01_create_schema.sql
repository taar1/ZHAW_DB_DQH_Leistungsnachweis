-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 – Operative Datenbank `post_oltp` anlegen
-- MAGIC
-- MAGIC Erstellt das Schema und die 7 Tabellen der operativen (OLTP) Datenbank für das Szenario
-- MAGIC **Schweizer Post – Brief- und Paketversand**.
-- MAGIC
-- MAGIC * Läuft im aktuell gewählten Katalog (Databricks Free Edition: `workspace`).
-- MAGIC * **Achtung:** Das Schema wird zuerst gelöscht (`DROP SCHEMA ... CASCADE`) – das Skript ist damit beliebig oft wiederholbar.
-- MAGIC * Primär- und Fremdschlüssel sind in Unity Catalog *informativ* (werden nicht erzwungen). Sie dokumentieren das
-- MAGIC   Modell und erscheinen im Catalog Explorer im ER-Diagramm. Erzwungen werden nur `NOT NULL` und `CHECK`.
-- MAGIC
-- MAGIC Diese Datei ist gleichzeitig ein normales SQL-Skript und ein Databricks-Notebook.

-- COMMAND ----------

DROP SCHEMA IF EXISTS post_oltp CASCADE;

CREATE SCHEMA post_oltp
  COMMENT 'Operative Datenbank (OLTP) Schweizer Post – Brief- und Paketversand (ZHAW DB & DWH Leistungsnachweis)';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Land
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.land (
  land_code    STRING NOT NULL COMMENT 'ISO 3166-1 Alpha-2 Code, z.B. CH, DE, US',
  land_name    STRING NOT NULL COMMENT 'Landesname (deutsch)',
  zone         STRING NOT NULL COMMENT 'Tarifzone der Post: CH (Inland inkl. FL), EUROPA, WELT',
  CONSTRAINT pk_land PRIMARY KEY (land_code)
) COMMENT 'Länder und deren Tarifzone';

ALTER TABLE post_oltp.land ADD CONSTRAINT chk_land_zone CHECK (zone IN ('CH', 'EUROPA', 'WELT'));

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Kunde (Absender, kauft die Postprodukte)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.kunde (
  kunde_id         INT    NOT NULL COMMENT 'Technischer Schlüssel',
  kundentyp        STRING NOT NULL COMMENT 'PRIVAT oder GESCHAEFT',
  vorname          STRING          COMMENT 'Vorname (bei Geschäftskunden: Kontaktperson)',
  nachname         STRING          COMMENT 'Nachname (bei Geschäftskunden: Kontaktperson)',
  firmenname       STRING          COMMENT 'Firmenname (nur Geschäftskunden)',
  email            STRING          COMMENT 'E-Mail-Adresse',
  strasse          STRING          COMMENT 'Strasse und Hausnummer',
  plz              STRING          COMMENT 'Postleitzahl (Text, da ausländische PLZ Buchstaben enthalten)',
  ort              STRING NOT NULL COMMENT 'Ort',
  land_code        STRING NOT NULL COMMENT 'FK auf land',
  registriert_am   DATE   NOT NULL COMMENT 'Datum der Registrierung als Kunde',
  CONSTRAINT pk_kunde PRIMARY KEY (kunde_id),
  CONSTRAINT fk_kunde_land FOREIGN KEY (land_code) REFERENCES post_oltp.land (land_code)
) COMMENT 'Kunden der Post (Privat- und Geschäftskunden)';

ALTER TABLE post_oltp.kunde ADD CONSTRAINT chk_kunde_typ CHECK (kundentyp IN ('PRIVAT', 'GESCHAEFT'));

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Verkaufskanal
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.verkaufskanal (
  kanal_id     INT    NOT NULL COMMENT 'Technischer Schlüssel',
  kanal_name   STRING NOT NULL COMMENT 'Bezeichnung, z.B. Postfiliale, Post-App',
  kanal_typ    STRING NOT NULL COMMENT 'PHYSISCH oder DIGITAL',
  CONSTRAINT pk_verkaufskanal PRIMARY KEY (kanal_id)
) COMMENT 'Kanäle, über die Postprodukte verkauft werden';

ALTER TABLE post_oltp.verkaufskanal ADD CONSTRAINT chk_kanal_typ CHECK (kanal_typ IN ('PHYSISCH', 'DIGITAL'));

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Produkt (Brief- und Paketprodukte nach Zone, Service und Gewichtsklasse)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.produkt (
  produkt_id               INT           NOT NULL COMMENT 'Technischer Schlüssel',
  produkt_name             STRING        NOT NULL COMMENT 'Bezeichnung inkl. Format/Gewichtsklasse',
  sendungsart              STRING        NOT NULL COMMENT 'BRIEF oder PAKET',
  zone                     STRING        NOT NULL COMMENT 'CH, EUROPA oder WELT',
  service                  STRING        NOT NULL COMMENT 'A-POST, B-POST, PRIORITY, ECONOMY, SAME-DAY',
  max_gewicht_g            INT           NOT NULL COMMENT 'Maximalgewicht in Gramm',
  laufzeit_soll_werktage   INT           NOT NULL COMMENT 'Versprochene Laufzeit in Werktagen (0 = gleicher Tag)',
  preis_chf                DECIMAL(8,2)  NOT NULL COMMENT 'Listenpreis in CHF',
  CONSTRAINT pk_produkt PRIMARY KEY (produkt_id)
) COMMENT 'Sortiment der Brief- und Paketprodukte';

ALTER TABLE post_oltp.produkt ADD CONSTRAINT chk_produkt_art CHECK (sendungsart IN ('BRIEF', 'PAKET'));
ALTER TABLE post_oltp.produkt ADD CONSTRAINT chk_produkt_zone CHECK (zone IN ('CH', 'EUROPA', 'WELT'));
ALTER TABLE post_oltp.produkt ADD CONSTRAINT chk_produkt_preis CHECK (preis_chf >= 0);

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Empfänger
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.empfaenger (
  empfaenger_id   INT    NOT NULL COMMENT 'Technischer Schlüssel',
  name            STRING NOT NULL COMMENT 'Name der Person oder Firma',
  strasse         STRING          COMMENT 'Strasse und Hausnummer',
  plz             STRING          COMMENT 'Postleitzahl',
  ort             STRING NOT NULL COMMENT 'Ort',
  land_code       STRING NOT NULL COMMENT 'FK auf land (bestimmt die Zielzone)',
  CONSTRAINT pk_empfaenger PRIMARY KEY (empfaenger_id),
  CONSTRAINT fk_empfaenger_land FOREIGN KEY (land_code) REFERENCES post_oltp.land (land_code)
) COMMENT 'Empfänger von Sendungen (Inland und Ausland)';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Auftrag (ein Kauf eines Kunden über einen Verkaufskanal)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.auftrag (
  auftrag_id         INT           NOT NULL COMMENT 'Technischer Schlüssel',
  kunde_id           INT           NOT NULL COMMENT 'FK auf kunde',
  kanal_id           INT           NOT NULL COMMENT 'FK auf verkaufskanal',
  auftrag_datum      DATE          NOT NULL COMMENT 'Datum des Kaufs',
  betrag_total_chf   DECIMAL(10,2) NOT NULL COMMENT 'Total des Auftrags in CHF (= Summe der Sendungspreise)',
  CONSTRAINT pk_auftrag PRIMARY KEY (auftrag_id),
  CONSTRAINT fk_auftrag_kunde FOREIGN KEY (kunde_id) REFERENCES post_oltp.kunde (kunde_id),
  CONSTRAINT fk_auftrag_kanal FOREIGN KEY (kanal_id) REFERENCES post_oltp.verkaufskanal (kanal_id)
) COMMENT 'Aufträge (Käufe) der Kunden';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Sendung – abhängige Entität von Auftrag (ID-Beziehung):
-- Der Primärschlüssel (auftrag_id, position_nr) enthält den Schlüssel des Auftrags.
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.sendung (
  auftrag_id           INT           NOT NULL COMMENT 'PK-Teil und FK auf auftrag (ID-Beziehung)',
  position_nr          INT           NOT NULL COMMENT 'PK-Teil: laufende Nummer der Sendung im Auftrag',
  tracking_nr          STRING                 COMMENT 'Sendungsnummer, nur bei Paketen (Briefe sind nicht verfolgbar)',
  produkt_id           INT           NOT NULL COMMENT 'FK auf produkt',
  empfaenger_id        INT           NOT NULL COMMENT 'FK auf empfaenger',
  gewicht_g            INT                    COMMENT 'Gewicht in Gramm',
  preis_chf            DECIMAL(8,2)  NOT NULL COMMENT 'Bezahlter Preis in CHF',
  aufgabe_datum        DATE          NOT NULL COMMENT 'Datum der Aufgabe',
  soll_zustell_datum   DATE          NOT NULL COMMENT 'Gemäss Produktlaufzeit versprochenes Zustelldatum',
  zustell_datum        DATE                   COMMENT 'Effektives Zustelldatum aus dem Tracking (nur Pakete; NULL, wenn nicht zugestellt)',
  status               STRING                 COMMENT 'Tracking-Status (nur Pakete): UNTERWEGS, ZUGESTELLT, RETOURNIERT, VERLOREN',
  CONSTRAINT pk_sendung PRIMARY KEY (auftrag_id, position_nr),
  CONSTRAINT fk_sendung_auftrag FOREIGN KEY (auftrag_id) REFERENCES post_oltp.auftrag (auftrag_id),
  CONSTRAINT fk_sendung_produkt FOREIGN KEY (produkt_id) REFERENCES post_oltp.produkt (produkt_id),
  CONSTRAINT fk_sendung_empfaenger FOREIGN KEY (empfaenger_id) REFERENCES post_oltp.empfaenger (empfaenger_id)
) COMMENT 'Einzelne Brief- oder Paketsendungen eines Auftrags';

ALTER TABLE post_oltp.sendung ADD CONSTRAINT chk_sendung_status
  CHECK (status IS NULL OR status IN ('UNTERWEGS', 'ZUGESTELLT', 'RETOURNIERT', 'VERLOREN'));
ALTER TABLE post_oltp.sendung ADD CONSTRAINT chk_sendung_preis CHECK (preis_chf >= 0);
