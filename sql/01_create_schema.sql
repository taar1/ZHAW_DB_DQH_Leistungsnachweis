-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 – Operative Datenbank `post_oltp` anlegen
-- MAGIC
-- MAGIC Erstellt das Schema und alle Tabellen der operativen (OLTP) Datenbank für das Szenario
-- MAGIC **Schweizer Post – Brief- und Paketversand**.
-- MAGIC
-- MAGIC * Läuft im aktuell gewählten Katalog (Databricks Free Edition: `workspace`).
-- MAGIC * **Achtung:** Das Schema wird zuerst gelöscht (`DROP SCHEMA ... CASCADE`) – das Skript ist damit beliebig oft wiederholbar.
-- MAGIC * Primär- und Fremdschlüssel sind in Unity Catalog *informativ* (werden nicht erzwungen). Sie dokumentieren das
-- MAGIC   Modell und erscheinen im Catalog Explorer im ER-Diagramm. Erzwungen werden nur `NOT NULL` und `CHECK`.
-- MAGIC
-- MAGIC Diese Datei ist gleichzeitig ein normales SQL-Skript und ein Databricks-Notebook (Import als Notebook möglich).

-- COMMAND ----------

DROP SCHEMA IF EXISTS post_oltp CASCADE;

CREATE SCHEMA post_oltp
  COMMENT 'Operative Datenbank (OLTP) Schweizer Post – Brief- und Paketversand (ZHAW DB & DWH Leistungsnachweis)';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Stammdaten: Land
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
-- Adresse (wird von Kunde, Empfänger genutzt)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.adresse (
  adresse_id   INT    NOT NULL COMMENT 'Technischer Schlüssel',
  strasse      STRING NOT NULL COMMENT 'Strassenname',
  hausnummer   STRING          COMMENT 'Hausnummer inkl. Zusatz (z.B. 12a)',
  plz          STRING          COMMENT 'Postleitzahl (Text, da ausländische PLZ Buchstaben enthalten)',
  ort          STRING NOT NULL COMMENT 'Ort',
  kanton       STRING          COMMENT 'Kantonskürzel (nur CH), z.B. ZH',
  land_code    STRING NOT NULL COMMENT 'FK auf land',
  CONSTRAINT pk_adresse PRIMARY KEY (adresse_id),
  CONSTRAINT fk_adresse_land FOREIGN KEY (land_code) REFERENCES post_oltp.land (land_code)
) COMMENT 'Postadressen von Kunden und Empfängern';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Kunde (Absender, Käufer der Postprodukte)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.kunde (
  kunde_id         INT           NOT NULL COMMENT 'Technischer Schlüssel',
  kundennummer     STRING        NOT NULL COMMENT 'Fachlicher Schlüssel, z.B. KD10000001',
  kundentyp        STRING        NOT NULL COMMENT 'PRIVAT oder GESCHAEFT',
  geschaeftsprofil STRING                 COMMENT 'Nur Geschäftskunden: ECOMMERCE, BRIEFVERSENDER, GEMISCHT',
  anrede           STRING                 COMMENT 'Herr, Frau, Firma',
  vorname          STRING                 COMMENT 'Vorname (Privatkunde bzw. Kontaktperson)',
  nachname         STRING                 COMMENT 'Nachname (Privatkunde bzw. Kontaktperson)',
  firmenname       STRING                 COMMENT 'Nur Geschäftskunden',
  email            STRING                 COMMENT 'E-Mail-Adresse',
  telefon          STRING                 COMMENT 'Telefonnummer (Format uneinheitlich erfasst)',
  geburtsdatum     DATE                   COMMENT 'Nur Privatkunden',
  rabatt_prozent   DECIMAL(5,2)  NOT NULL COMMENT 'Vertraglicher Rabatt in Prozent (Geschäftskunden)',
  registriert_am   DATE          NOT NULL COMMENT 'Datum der Registrierung als Kunde',
  adresse_id       INT           NOT NULL COMMENT 'FK auf adresse (Domizil-/Rechnungsadresse)',
  CONSTRAINT pk_kunde PRIMARY KEY (kunde_id),
  CONSTRAINT fk_kunde_adresse FOREIGN KEY (adresse_id) REFERENCES post_oltp.adresse (adresse_id)
) COMMENT 'Kunden der Post (Privat- und Geschäftskunden)';

ALTER TABLE post_oltp.kunde ADD CONSTRAINT chk_kunde_typ CHECK (kundentyp IN ('PRIVAT', 'GESCHAEFT'));

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Verkaufskanal
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.verkaufskanal (
  kanal_id      INT    NOT NULL COMMENT 'Technischer Schlüssel',
  kanal_code    STRING NOT NULL COMMENT 'Fachlicher Code, z.B. FILIALE',
  kanal_name    STRING NOT NULL COMMENT 'Bezeichnung',
  kanal_typ     STRING NOT NULL COMMENT 'PHYSISCH oder DIGITAL',
  beschreibung  STRING          COMMENT 'Beschreibung des Kanals',
  CONSTRAINT pk_verkaufskanal PRIMARY KEY (kanal_id)
) COMMENT 'Kanäle, über die Postprodukte verkauft werden';

ALTER TABLE post_oltp.verkaufskanal ADD CONSTRAINT chk_kanal_typ CHECK (kanal_typ IN ('PHYSISCH', 'DIGITAL'));

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Produkt (Versandprodukte, angelehnt an das Sortiment der Schweizer Post)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.produkt (
  produkt_id               INT           NOT NULL COMMENT 'Technischer Schlüssel',
  produkt_code             STRING        NOT NULL COMMENT 'Fachlicher Code, z.B. BRI-CH-A-STD',
  produkt_name             STRING        NOT NULL COMMENT 'Bezeichnung',
  sendungsart              STRING        NOT NULL COMMENT 'BRIEF oder PAKET',
  zone                     STRING        NOT NULL COMMENT 'CH, EUROPA, WELT',
  service                  STRING        NOT NULL COMMENT 'A-POST, B-POST, PRIORITY, ECONOMY, SAME-DAY',
  format                   STRING        NOT NULL COMMENT 'Standardbrief, Grossbrief, Maxibrief, S, M, L, Gewichtsstufe',
  min_gewicht_g            INT           NOT NULL COMMENT 'Mindestgewicht der Gewichtsstufe (Gramm)',
  max_gewicht_g            INT           NOT NULL COMMENT 'Maximalgewicht (Gramm)',
  max_dicke_mm             INT                    COMMENT 'Maximale Dicke (nur Briefe)',
  laufzeit_soll_werktage   INT           NOT NULL COMMENT 'Zugesicherte Laufzeit in Werktagen (0 = gleicher Tag)',
  preis_chf                DECIMAL(10,2) NOT NULL COMMENT 'Listenpreis in CHF',
  aktiv                    BOOLEAN       NOT NULL COMMENT 'Produkt aktuell im Verkauf',
  CONSTRAINT pk_produkt PRIMARY KEY (produkt_id)
) COMMENT 'Versandprodukte (Briefe und Pakete Inland/Ausland)';

ALTER TABLE post_oltp.produkt ADD CONSTRAINT chk_produkt_art CHECK (sendungsart IN ('BRIEF', 'PAKET'));
ALTER TABLE post_oltp.produkt ADD CONSTRAINT chk_produkt_preis CHECK (preis_chf >= 0);

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Zusatzleistung (Einschreiben, Signature, Nachnahme, ...)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.zusatzleistung (
  zusatzleistung_id  INT           NOT NULL COMMENT 'Technischer Schlüssel',
  code               STRING        NOT NULL COMMENT 'Kürzel, z.B. R = Einschreiben',
  bezeichnung        STRING        NOT NULL COMMENT 'Bezeichnung',
  anwendbar_fuer     STRING        NOT NULL COMMENT 'BRIEF, PAKET',
  preis_chf          DECIMAL(10,2) NOT NULL COMMENT 'Zuschlag in CHF',
  CONSTRAINT pk_zusatzleistung PRIMARY KEY (zusatzleistung_id)
) COMMENT 'Zusatzleistungen, die zu einer Sendung gebucht werden können';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Standort (Filialen, Automaten, Sortierzentren, Zustellstellen, Partnerpost Ausland)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.standort (
  standort_id    INT    NOT NULL COMMENT 'Technischer Schlüssel',
  standort_name  STRING NOT NULL COMMENT 'Bezeichnung',
  standort_typ   STRING NOT NULL COMMENT 'FILIALE, PARTNERFILIALE, AUTOMAT, BRIEFZENTRUM, PAKETZENTRUM, AUSTAUSCHSTELLE, ZUSTELLSTELLE, AUSLAND',
  plz            STRING          COMMENT 'Postleitzahl',
  ort            STRING          COMMENT 'Ort',
  kanton         STRING          COMMENT 'Kanton (nur CH)',
  land_code      STRING NOT NULL COMMENT 'FK auf land',
  CONSTRAINT pk_standort PRIMARY KEY (standort_id),
  CONSTRAINT fk_standort_land FOREIGN KEY (land_code) REFERENCES post_oltp.land (land_code)
) COMMENT 'Betriebsstandorte der Post und Partner';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Empfänger
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.empfaenger (
  empfaenger_id  INT    NOT NULL COMMENT 'Technischer Schlüssel',
  vorname        STRING          COMMENT 'Vorname',
  nachname       STRING          COMMENT 'Nachname',
  firmenname     STRING          COMMENT 'Firma (falls Geschäftsempfänger)',
  adresse_id     INT    NOT NULL COMMENT 'FK auf adresse',
  CONSTRAINT pk_empfaenger PRIMARY KEY (empfaenger_id),
  CONSTRAINT fk_empfaenger_adresse FOREIGN KEY (adresse_id) REFERENCES post_oltp.adresse (adresse_id)
) COMMENT 'Empfänger von Sendungen';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Auftrag (Kauf von Versandleistungen durch einen Kunden über einen Kanal)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.auftrag (
  auftrag_id         INT           NOT NULL COMMENT 'Technischer Schlüssel',
  kunde_id           INT           NOT NULL COMMENT 'FK auf kunde',
  kanal_id           INT           NOT NULL COMMENT 'FK auf verkaufskanal',
  standort_id        INT                    COMMENT 'FK auf standort (nur physische Kanäle)',
  auftrag_zeitpunkt  TIMESTAMP     NOT NULL COMMENT 'Zeitpunkt des Kaufs',
  zahlungsart        STRING        NOT NULL COMMENT 'BAR, KARTE, TWINT, POSTFINANCE, RECHNUNG',
  betrag_total_chf   DECIMAL(10,2) NOT NULL COMMENT 'Gesamtbetrag des Auftrags in CHF',
  CONSTRAINT pk_auftrag PRIMARY KEY (auftrag_id),
  CONSTRAINT fk_auftrag_kunde    FOREIGN KEY (kunde_id)    REFERENCES post_oltp.kunde (kunde_id),
  CONSTRAINT fk_auftrag_kanal    FOREIGN KEY (kanal_id)    REFERENCES post_oltp.verkaufskanal (kanal_id),
  CONSTRAINT fk_auftrag_standort FOREIGN KEY (standort_id) REFERENCES post_oltp.standort (standort_id)
) COMMENT 'Aufträge (Kaufvorgänge) von Kunden';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Sendung (Auftragsposition) – schwache Entität, ID-abhängig von Auftrag
-- PK = (auftrag_id, position_nr)  ->  ID-Beziehung zu auftrag
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.sendung (
  auftrag_id         INT           NOT NULL COMMENT 'PK-Teil und FK auf auftrag (ID-Beziehung)',
  position_nr        INT           NOT NULL COMMENT 'PK-Teil: laufende Nummer innerhalb des Auftrags',
  tracking_nr        STRING        NOT NULL COMMENT 'Sendungsnummer (alternativer Schlüssel), z.B. 99.60.123456.12345678 oder RR123456785CH',
  produkt_id         INT           NOT NULL COMMENT 'FK auf produkt',
  empfaenger_id      INT           NOT NULL COMMENT 'FK auf empfaenger',
  gewicht_g          INT                    COMMENT 'Gemessenes Gewicht in Gramm',
  laenge_cm          DECIMAL(6,1)           COMMENT 'Länge in cm',
  breite_cm          DECIMAL(6,1)           COMMENT 'Breite in cm',
  hoehe_cm           DECIMAL(6,1)           COMMENT 'Höhe/Dicke in cm',
  preis_chf          DECIMAL(10,2) NOT NULL COMMENT 'Effektiv verrechneter Preis inkl. Zusatzleistungen und Rabatt',
  aufgabe_datum      DATE          NOT NULL COMMENT 'Datum der Aufgabe',
  soll_zustell_datum DATE          NOT NULL COMMENT 'Zugesichertes Zustelldatum gemäss Produktlaufzeit',
  zustell_datum      DATE                   COMMENT 'Effektives Zustelldatum (NULL wenn nicht zugestellt)',
  status             STRING        NOT NULL COMMENT 'UNTERWEGS, ZUGESTELLT, RETOURNIERT, VERLOREN',
  CONSTRAINT pk_sendung PRIMARY KEY (auftrag_id, position_nr),
  CONSTRAINT fk_sendung_auftrag    FOREIGN KEY (auftrag_id)    REFERENCES post_oltp.auftrag (auftrag_id),
  CONSTRAINT fk_sendung_produkt    FOREIGN KEY (produkt_id)    REFERENCES post_oltp.produkt (produkt_id),
  CONSTRAINT fk_sendung_empfaenger FOREIGN KEY (empfaenger_id) REFERENCES post_oltp.empfaenger (empfaenger_id)
) COMMENT 'Einzelne Sendungen (Positionen eines Auftrags) inkl. Trackingnummer und Empfänger';

ALTER TABLE post_oltp.sendung ADD CONSTRAINT chk_sendung_status CHECK (status IN ('UNTERWEGS', 'ZUGESTELLT', 'RETOURNIERT', 'VERLOREN'));

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Sendung_Zusatzleistung (m:n zwischen Sendung und Zusatzleistung)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.sendung_zusatzleistung (
  auftrag_id         INT NOT NULL COMMENT 'PK-Teil, FK auf sendung',
  position_nr        INT NOT NULL COMMENT 'PK-Teil, FK auf sendung',
  zusatzleistung_id  INT NOT NULL COMMENT 'PK-Teil, FK auf zusatzleistung',
  CONSTRAINT pk_sendung_zusatzleistung PRIMARY KEY (auftrag_id, position_nr, zusatzleistung_id),
  CONSTRAINT fk_sz_sendung        FOREIGN KEY (auftrag_id, position_nr) REFERENCES post_oltp.sendung (auftrag_id, position_nr),
  CONSTRAINT fk_sz_zusatzleistung FOREIGN KEY (zusatzleistung_id)       REFERENCES post_oltp.zusatzleistung (zusatzleistung_id)
) COMMENT 'Gebuchte Zusatzleistungen pro Sendung';

-- COMMAND ----------

-- ---------------------------------------------------------------------------
-- Tracking_Event – schwache Entität, ID-abhängig von Sendung
-- PK = (auftrag_id, position_nr, event_nr)
-- ---------------------------------------------------------------------------
CREATE TABLE post_oltp.tracking_event (
  auftrag_id        INT       NOT NULL COMMENT 'PK-Teil, FK auf sendung',
  position_nr       INT       NOT NULL COMMENT 'PK-Teil, FK auf sendung',
  event_nr          INT       NOT NULL COMMENT 'PK-Teil: laufende Nummer des Ereignisses je Sendung',
  event_zeitpunkt   TIMESTAMP NOT NULL COMMENT 'Zeitpunkt des Ereignisses',
  event_code        STRING    NOT NULL COMMENT 'z.B. ERFASST, AUFGABE, SORTIERUNG, EXPORT, IMPORT, IN_ZUSTELLUNG, ZUGESTELLT',
  event_text        STRING    NOT NULL COMMENT 'Klartext wie in der Sendungsverfolgung',
  standort_id       INT                COMMENT 'FK auf standort (NULL bei rein elektronischen Ereignissen)',
  CONSTRAINT pk_tracking_event PRIMARY KEY (auftrag_id, position_nr, event_nr),
  CONSTRAINT fk_te_sendung  FOREIGN KEY (auftrag_id, position_nr) REFERENCES post_oltp.sendung (auftrag_id, position_nr),
  CONSTRAINT fk_te_standort FOREIGN KEY (standort_id)             REFERENCES post_oltp.standort (standort_id)
) COMMENT 'Ereignisse der Sendungsverfolgung (Tracking)';
