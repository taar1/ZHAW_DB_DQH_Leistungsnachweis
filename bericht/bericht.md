---
title: "Leistungsnachweis DB & DWH – Variante 1"
subtitle: "Operative Datenbank, Datawarehouse und Auswertung für den Brief- und Paketversand der Schweizer Post"
author:
  - "Dominik Erbsland"
  - "Joel Liechti"
date: "30. November 2026"
lang: de-CH
---

# Leistungsnachweis DB & DWH – Variante 1

**Operative Datenbank, Datawarehouse und Auswertung für den Brief- und Paketversand der Schweizer Post**

| | |
|---|---|
| Modul | Datenbanken & Datawarehouse (DB & DWH), ZHAW School of Engineering |
| Team | Dominik Erbsland, Joel Liechti |
| Abgabe | 30.11.2026 |
| Repository | <https://github.com/taar1/ZHAW_DB_DQH_Leistungsnachweis> |
| Werkzeuge | Databricks Free Edition (Serverless, Unity Catalog), Python, Mermaid, [Power BI] |

> **Hinweis zur Arbeit am Bericht:** Stellen mit **TODO** müssen wir noch ergänzen (Screenshots, Resultate aus
> Databricks, Interpretation). Die Diagramme liegen als Mermaid-Quellen in `docs/diagramme/` und sind hier
> eingebettet. Bei Änderungen beide Stellen nachführen.

---

## Inhaltsverzeichnis

1. [Ausgangslage und Geschäftsanwendung](#1-ausgangslage-und-geschäftsanwendung) (Aufgabe 1)
2. [Entitäten-Relationen-Modell](#2-entitäten-relationen-modell) (Aufgabe 2)
3. [Umsetzung der operativen Datenbank in Databricks](#3-umsetzung-der-operativen-datenbank-in-databricks) (Aufgabe 3)
4. [Beispieldaten](#4-beispieldaten) (Aufgabe 4)
5. [Informationsbedarf und Steuerungskennzahlen](#5-informationsbedarf-und-steuerungskennzahlen) (Aufgabe 5)
6. [Multidimensionales Datenmodell](#6-multidimensionales-datenmodell) (Aufgabe 6)
7. [Datawarehouse in der Medaillon-Architektur](#7-datawarehouse-in-der-medaillon-architektur) (Aufgabe 7)
8. [Datenqualitätsprüfungen im ETL-Prozess](#8-datenqualitätsprüfungen-im-etl-prozess) (Aufgabe 8)
9. [Auswertungsdashboard](#9-auswertungsdashboard) (Aufgabe 9)
10. [Fazit und Reflexion](#10-fazit-und-reflexion)
11. [Anhang](#11-anhang)

---

## 1. Ausgangslage und Geschäftsanwendung

### 1.1 Beruflicher Kontext

Wir arbeiten beide bei der **Schweizerischen Post**. Die Post verkauft Privat- und Geschäftskunden Versandleistungen
für Briefe und Pakete, im Inland wie ins Ausland. Verkauft wird über physische Kanäle (Postfilialen,
Filialen mit Partner, My Post 24-Automaten, Abholung pick@home) und über digitale Kanäle (Online-Porto/WebStamp,
Post-App, Geschäftskundenportal bzw. Versandschnittstelle). Jede verkaufte Sendung erhält eine Sendungsnummer
(Trackingnummer), über die Absender und Empfänger den Weg der Sendung verfolgen können.

Für den Leistungsnachweis modellieren wir einen **vereinfachten Ausschnitt** dieses Geschäfts. Produkte und Preise
sind an das öffentliche Sortiment der Post angelehnt, aber vereinfacht. Alle Kunden-, Empfänger- und Adressdaten sind
frei erfunden (synthetisch generiert).

### 1.2 Beschreibung der Geschäftsanwendung „Versand & Sendungsverfolgung“

Die operative Anwendung unterstützt folgende Geschäftsprozesse:

1. **Kundenverwaltung:** Privatkunden und Geschäftskunden werden mit Domiziladresse, Kontaktdaten und (bei
   Geschäftskunden) einem vertraglichen Rabatt erfasst.
2. **Verkauf (Auftrag):** Ein Kunde kauft über einen **Verkaufskanal** eine oder mehrere Versandleistungen. Ein Kauf
   ist ein *Auftrag*. Erfolgt er physisch, wird zusätzlich die Aufgabestelle (Filiale, Automat) festgehalten.
3. **Sendungserfassung:** Jede Position des Auftrags ist eine **Sendung**. Sie verweist auf genau ein **Produkt**
   (z.B. *Paket Priority M*), einen **Empfänger** und erhält eine eindeutige Trackingnummer. Optional werden
   **Zusatzleistungen** gebucht (Einschreiben, Signature, Assurance, Nachnahme, Samstagszustellung, Sperrgut).
4. **Sendungsverfolgung:** Während der Beförderung entstehen **Tracking-Ereignisse** (Aufgabe, Sortierung im
   Brief-/Paketzentrum, Export/Import, In Zustellung, Zugestellt, Retoure). Jedes Ereignis ist einem **Standort**
   zugeordnet.

Die von der Aufgabenstellung geforderten Entitätstypen sind abgedeckt:

| Gefordert | Umsetzung im Modell |
|---|---|
| Kunde | `KUNDE` (Privat- und Geschäftskunden) mit `ADRESSE` |
| Produkt | `PRODUKT` (Briefe/Pakete, Inland/Europa/Welt, Service, Gewichts-/Formatklassen) und `ZUSATZLEISTUNG` |
| Verkaufskanal | `VERKAUFSKANAL` (7 physische und digitale Kanäle) |
| Beziehungen | Kunde *erteilt* Auftrag *über* Verkaufskanal; Auftrag *umfasst* Sendungen eines Produkts |

### 1.3 Produktsortiment (vereinfacht)

| Sendungsart | Zone | Services | Format- bzw. Gewichtsklassen | Preisbereich CHF |
|---|---|---|---|---|
| Brief | Inland (CH + FL) | A-Post (1 Werktag), B-Post (3 Werktage) | Standardbrief (B5, 100 g, 2 cm), Grossbrief (B4, 500 g, 2 cm), Maxibrief (B4, 1 kg, **dicker als 2 cm** bis 5 cm) | 1.00 – 4.70 |
| Paket | Inland (CH + FL) | Economy (2 WT), Priority (1 WT), Same Day | S (bis 2 kg), M (bis 10 kg), L (bis 30 kg) | 9.00 – 49.00 |
| Brief | Europa | Priority, Economy | bis 20 g, bis 250 g, bis 2 kg | 2.20 – 18.00 |
| Brief | Welt | Priority, Economy | bis 20 g, bis 250 g, bis 2 kg | 2.50 – 32.00 |
| Paket | Europa | Priority, Economy | bis 2 kg, bis 10 kg, bis 30 kg | 31.00 – 115.00 |
| Paket | Welt | Priority, Economy | bis 2 kg, bis 10 kg, bis 30 kg | 42.00 – 230.00 |

Insgesamt sind es **37 Produkte** und **6 Zusatzleistungen**. Liechtenstein gehört tariflich zum Inland.

---

## 2. Entitäten-Relationen-Modell

### 2.1 ER-Diagramm

Quelle: [`docs/diagramme/erm_oltp.mmd`](../docs/diagramme/erm_oltp.mmd). Notation: Crow's Foot; `PK` = Primärschlüssel,
`FK` = Fremdschlüssel, `UK` = alternativer (fachlicher) Schlüssel.

```mermaid
erDiagram
    LAND ||--o{ ADRESSE : "liegt in"
    LAND ||--o{ STANDORT : "liegt in"
    ADRESSE ||--o{ KUNDE : "ist Domizil von"
    ADRESSE ||--o{ EMPFAENGER : "ist Zustelladresse von"
    KUNDE ||--o{ AUFTRAG : "erteilt"
    VERKAUFSKANAL ||--o{ AUFTRAG : "wird verkauft über"
    STANDORT |o--o{ AUFTRAG : "Aufgabestelle (nur physisch)"
    AUFTRAG ||--|{ SENDUNG : "umfasst (ID-Beziehung)"
    PRODUKT ||--o{ SENDUNG : "wird versandt als"
    EMPFAENGER ||--o{ SENDUNG : "empfängt"
    SENDUNG ||--o{ SENDUNG_ZUSATZLEISTUNG : "hat"
    ZUSATZLEISTUNG ||--o{ SENDUNG_ZUSATZLEISTUNG : "wird gebucht in"
    SENDUNG ||--|{ TRACKING_EVENT : "wird verfolgt durch (ID-Beziehung)"
    STANDORT |o--o{ TRACKING_EVENT : "Ort des Ereignisses"

    LAND {
        string land_code PK "ISO-Code"
        string land_name
        string zone "CH / EUROPA / WELT"
    }
    ADRESSE {
        int adresse_id PK
        string strasse
        string hausnummer
        string plz
        string ort
        string kanton
        string land_code FK
    }
    KUNDE {
        int kunde_id PK
        string kundennummer UK
        string kundentyp "PRIVAT / GESCHAEFT"
        string geschaeftsprofil
        string anrede
        string vorname
        string nachname
        string firmenname
        string email
        string telefon
        date geburtsdatum
        decimal rabatt_prozent
        date registriert_am
        int adresse_id FK
    }
    VERKAUFSKANAL {
        int kanal_id PK
        string kanal_code UK
        string kanal_name
        string kanal_typ "PHYSISCH / DIGITAL"
        string beschreibung
    }
    PRODUKT {
        int produkt_id PK
        string produkt_code UK
        string produkt_name
        string sendungsart "BRIEF / PAKET"
        string zone "CH / EUROPA / WELT"
        string service "A-POST, B-POST, PRIORITY, ECONOMY, SAME-DAY"
        string format
        int min_gewicht_g
        int max_gewicht_g
        int max_dicke_mm
        int laufzeit_soll_werktage
        decimal preis_chf
        boolean aktiv
    }
    ZUSATZLEISTUNG {
        int zusatzleistung_id PK
        string code UK
        string bezeichnung
        string anwendbar_fuer
        decimal preis_chf
    }
    STANDORT {
        int standort_id PK
        string standort_name
        string standort_typ
        string plz
        string ort
        string kanton
        string land_code FK
    }
    EMPFAENGER {
        int empfaenger_id PK
        string vorname
        string nachname
        string firmenname
        int adresse_id FK
    }
    AUFTRAG {
        int auftrag_id PK
        int kunde_id FK
        int kanal_id FK
        int standort_id FK "optional"
        timestamp auftrag_zeitpunkt
        string zahlungsart
        decimal betrag_total_chf
    }
    SENDUNG {
        int auftrag_id PK,FK "ID-Beziehung zu AUFTRAG"
        int position_nr PK
        string tracking_nr UK
        int produkt_id FK
        int empfaenger_id FK
        int gewicht_g
        decimal laenge_cm
        decimal breite_cm
        decimal hoehe_cm
        decimal preis_chf
        date aufgabe_datum
        date soll_zustell_datum
        date zustell_datum
        string status
    }
    SENDUNG_ZUSATZLEISTUNG {
        int auftrag_id PK,FK
        int position_nr PK,FK
        int zusatzleistung_id PK,FK
    }
    TRACKING_EVENT {
        int auftrag_id PK,FK "ID-Beziehung zu SENDUNG"
        int position_nr PK,FK
        int event_nr PK
        timestamp event_zeitpunkt
        string event_code
        string event_text
        int standort_id FK "optional"
    }
```

> **TODO:** Für das PDF das Diagramm als Bild exportieren (z.B. mermaid.live → PNG) und hier einfügen.
> Ergänzend kann man einen Screenshot des ER-Diagramms aus dem Databricks *Catalog Explorer*
> (Schema `post_oltp` → Tabelle → *Relationships*) einfügen.

### 2.2 Entitätstypen

| Entitätstyp | Bedeutung | Schlüssel |
|---|---|---|
| `LAND` | Land mit Tarifzone (CH, EUROPA, WELT) | `land_code` (ISO 3166) |
| `ADRESSE` | Postadresse (für Kunden und Empfänger wiederverwendet) | `adresse_id` |
| `KUNDE` | Absender/Käufer; Privat- oder Geschäftskunde | `kunde_id`; fachlich `kundennummer` |
| `VERKAUFSKANAL` | Kanal des Verkaufs (physisch/digital) | `kanal_id`; fachlich `kanal_code` |
| `PRODUKT` | Versandprodukt mit Zone, Service, Format, Gewichtslimite, Soll-Laufzeit und Preis | `produkt_id`; fachlich `produkt_code` |
| `ZUSATZLEISTUNG` | Buchbare Zusatzleistung (z.B. Einschreiben) | `zusatzleistung_id` |
| `STANDORT` | Filiale, Automat, Brief-/Paketzentrum, Zustellstelle, Partnerpost im Ausland | `standort_id` |
| `EMPFAENGER` | Empfänger einer Sendung | `empfaenger_id` |
| `AUFTRAG` | Kaufvorgang eines Kunden über einen Kanal | `auftrag_id` |
| `SENDUNG` | Position eines Auftrags = eine physische Sendung | (`auftrag_id`, `position_nr`); alternativ `tracking_nr` |
| `SENDUNG_ZUSATZLEISTUNG` | Assoziation Sendung ↔ Zusatzleistung (m:n) | (`auftrag_id`, `position_nr`, `zusatzleistung_id`) |
| `TRACKING_EVENT` | Ereignis der Sendungsverfolgung | (`auftrag_id`, `position_nr`, `event_nr`) |

### 2.3 Beziehungen und Kardinalitäten

| Beziehung | Kardinalität | Erläuterung |
|---|---|---|
| Kunde – Auftrag | 1 : 0..n | Ein Kunde kann beliebig viele Aufträge erteilen; jeder Auftrag gehört zu genau einem Kunden. |
| Verkaufskanal – Auftrag | 1 : 0..n | Jeder Auftrag wird über genau einen Kanal abgewickelt. |
| Standort – Auftrag | 0..1 : 0..n | Nur bei physischen Kanälen gibt es eine Aufgabestelle. |
| Auftrag – Sendung | 1 : 1..n | **ID-Beziehung**, siehe 2.4. |
| Produkt – Sendung | 1 : 0..n | Jede Sendung ist genau ein Produkt. Daraus ergibt sich die geforderte **Kunde–Produkt-Beziehung** (m:n, aufgelöst über Auftrag und Sendung). |
| Empfänger – Sendung | 1 : 0..n | Ein Empfänger kann mehrere Sendungen erhalten. |
| Sendung – Zusatzleistung | m : n | Aufgelöst über `SENDUNG_ZUSATZLEISTUNG`. |
| Sendung – Tracking_Event | 1 : 1..n | **ID-Beziehung**; jede Sendung hat mindestens das Ereignis „Aufgabe“. |
| Standort – Tracking_Event | 0..1 : 0..n | Rein elektronische Ereignisse (z.B. „elektronisch angemeldet“) haben keinen Standort. |
| Land – Adresse / Standort | 1 : 0..n | Die Zone des Landes bestimmt, welche Produkte zulässig sind. |

### 2.4 Detaillierte ID-Beziehung: Auftrag → Sendung → Tracking_Event

Eine **ID-Beziehung** (identifizierende Beziehung) liegt vor, wenn eine *schwache Entität* ohne die übergeordnete
Entität nicht eindeutig identifiziert werden kann. Ihr Primärschlüssel enthält deshalb den Primärschlüssel der
übergeordneten Entität.

**Auftrag → Sendung**

- Eine Sendung existiert nur im Rahmen eines Auftrags (*Existenzabhängigkeit*): ohne Kauf keine Sendung.
- Die Sendung wird über die laufende **Positionsnummer innerhalb des Auftrags** identifiziert:
  `PK(SENDUNG) = (auftrag_id, position_nr)`. `auftrag_id` ist gleichzeitig Teil des Primärschlüssels und
  Fremdschlüssel auf `AUFTRAG`.
- Kardinalität 1 : 1..n, denn jeder Auftrag hat mindestens eine Sendung.
- Wird ein Auftrag storniert, fallen auch alle seine Sendungen weg (kaskadierendes Löschen ist fachlich korrekt).

| auftrag_id | position_nr | tracking_nr | Produkt |
|---:|---:|---|---|
| 216 | 1 | 98.01.100559.10011470 | Standardbrief A-Post |
| 216 | 2 | 99.34.100559.10011507 | Paket Economy M |

`position_nr = 1` ist für sich allein nicht eindeutig, erst zusammen mit `auftrag_id`.

**Sendung → Tracking_Event** (zweistufige ID-Beziehung)

- Ein Tracking-Ereignis gehört immer zu genau einer Sendung und wird über eine laufende `event_nr` je Sendung
  nummeriert: `PK(TRACKING_EVENT) = (auftrag_id, position_nr, event_nr)`.
- Der zusammengesetzte Fremdschlüssel `(auftrag_id, position_nr)` verweist auf `SENDUNG`.

**Alternativer Schlüssel:** Die `tracking_nr` ist ebenfalls eindeutig und könnte als Primärschlüssel dienen. Wir haben
uns bewusst für die ID-Beziehung entschieden. Sie bildet die fachliche Abhängigkeit ab, und Sendungen können erfasst
werden, bevor die Trackingnummer bekannt ist (z.B. bei nachträglicher Etikettierung).

---

## 3. Umsetzung der operativen Datenbank in Databricks

Die operative Datenbank ist im Unity Catalog als Schema **`workspace.post_oltp`** umgesetzt
(DDL: [`sql/01_create_schema.sql`](../sql/01_create_schema.sql)). Alle Tabellen sind **Delta-Tabellen**.

Umsetzungsentscheide:

- **Datentypen:** Geldbeträge `DECIMAL(10,2)` (keine Rundungsfehler wie bei `DOUBLE`), Zeitpunkte `TIMESTAMP`,
  Postleitzahlen `STRING` (ausländische PLZ enthalten Buchstaben/Leerzeichen, z.B. „SW1A 1AA“).
- **Schlüssel:** `PRIMARY KEY`- und `FOREIGN KEY`-Constraints sind deklariert. In Databricks sind sie
  **informativ** und werden nicht erzwungen. Sie dokumentieren das Modell, werden vom Optimizer genutzt und erscheinen
  im Catalog Explorer. Die referentielle Integrität prüfen wir deshalb selbst im ETL (siehe Kapitel 8).
- **Erzwungene Regeln:** `NOT NULL` und `CHECK`-Constraints (z.B. `kundentyp IN ('PRIVAT','GESCHAEFT')`,
  `preis_chf >= 0`, gültige Sendungsstatus) werden von Delta Lake bei jedem Insert geprüft.
- **Dokumentation im Katalog:** Jede Tabelle und Spalte hat einen `COMMENT`.

Auszug aus der DDL:

```sql
CREATE TABLE post_oltp.sendung (
  auftrag_id         INT           NOT NULL COMMENT 'PK-Teil und FK auf auftrag (ID-Beziehung)',
  position_nr        INT           NOT NULL COMMENT 'PK-Teil: laufende Nummer innerhalb des Auftrags',
  tracking_nr        STRING        NOT NULL COMMENT 'Sendungsnummer (alternativer Schlüssel)',
  produkt_id         INT           NOT NULL,
  empfaenger_id      INT           NOT NULL,
  ...
  CONSTRAINT pk_sendung PRIMARY KEY (auftrag_id, position_nr),
  CONSTRAINT fk_sendung_auftrag FOREIGN KEY (auftrag_id) REFERENCES post_oltp.auftrag (auftrag_id),
  ...
);
```

> **TODO:** Screenshot Catalog Explorer mit Schema `post_oltp` und den 12 Tabellen einfügen.

---

## 4. Beispieldaten

### 4.1 Vorgehen

Die Beispieldaten erzeugt ein Python-Skript ([`scripts/generate_data.py`](../scripts/generate_data.py)). Es
schreibt SQL-INSERT-Skripte (`sql/02_…` bis `sql/05_…`). Der Generator arbeitet mit einem **fixen Zufalls-Seed**,
die Daten sind also reproduzierbar. Er simuliert nicht einfach zufällige Zeilen, sondern den Geschäftsprozess:

- Kunden registrieren sich über die Zeit. Aufträge entstehen erst nach der Registrierung.
- Jeder Kunde hat einen **bevorzugten Kanal** (Geschäftskunden meist das Geschäftskundenportal, Privatkunden oft die
  Filiale oder die App). Digitale Kanäle gewinnen über die Jahre Anteile.
- Geschäftskunden haben ein **Profil** (E-Commerce = viele Pakete, Briefversender = viele Briefe/Einschreiben, gemischt).
- **Saisonalität:** Spitzen im November (Black Friday) und Dezember (Weihnachten), wenig Aufträge an Sonntagen
  und Feiertagen. Leichtes Wachstum über die Zeit, Briefanteil rückläufig.
- Das **Produkt** passt zur Zone des Empfängers (ein Paket nach Deutschland ist immer ein Europa-Produkt), und das
  **Gewicht** liegt in der Gewichtsklasse des Produkts.
- Der **Preis** der Sendung ergibt sich aus Listenpreis + Zusatzleistungen − Rabatt, gerundet auf 5 Rappen.
- **Tracking-Ereignisse** entstehen gemäss Produktlaufzeit in Werktagen. Rund 6 % der Sendungen sind verspätet,
  in der Vorweihnachtszeit mehr. Dazu kommen Retouren (Empfänger nicht angetroffen) und wenige verlorene Sendungen.
- **Trackingnummern** folgen dem Aufbau der Post: Inland `99.60.123456.12345678` (Paket) bzw. `98.xx…` (Brief),
  International im UPU-S10-Format inkl. Prüfziffer, z.B. `RR123456785CH`.

Zeitraum: **01.01.2024 – 09.11.2026**, Datenstand (Stichtag) 10.11.2026.

### 4.2 Datenmengen

| Tabelle | Zeilen | | Tabelle | Zeilen |
|---|---:|---|---|---:|
| land | 24 | | empfaenger | 3'000 |
| verkaufskanal | 7 | | adresse | 3'650 |
| produkt | 37 | | auftrag | 5'887 |
| zusatzleistung | 6 | | sendung | 12'561 |
| standort | 175 | | sendung_zusatzleistung | 1'934 |
| kunde | 658 (528 privat, 130 geschäftlich) | | tracking_event | 53'157 |

### 4.3 Bewusst eingebaute Datenqualitätsprobleme

Operative Systeme enthalten in der Praxis immer fehlerhafte Daten. Damit die Datenqualitätsprüfungen im DWH
(Kapitel 8) etwas finden, haben wir folgende Probleme absichtlich eingebaut:

| Nr. | Problem | Tabelle.Spalte | Anzahl | Typische Ursache in der Praxis |
|---|---|---|---:|---|
| DQ1 | E-Mail fehlt oder ungültiges Format | `kunde.email` | 27 | Freitexterfassung am Schalter |
| DQ2 | Ungültige Schweizer PLZ (nicht 4-stellig) | `adresse.plz` | 6 | Tippfehler, „CH-8001“ |
| DQ3 | Uneinheitliche Ortsschreibweise (Zuerich, ZÜRICH, Genf) | `adresse.ort` | 18 | Mehrsprachigkeit, fehlende Normierung |
| DQ4 | Telefonnummern in verschiedenen Formaten bzw. fehlend | `kunde.telefon` | 183 | Kein Eingabeformat vorgegeben |
| DQ5 | Kunden-Dubletten (gleiche Person, zweite Kundennummer) | `kunde` | 8 | Neuregistrierung statt Login |
| DQ6 | Unplausibles Geburtsdatum | `kunde.geburtsdatum` | 1 | Platzhalter 01.01.1900 |
| DQ7 | Gewicht grösser als Produktlimite | `sendung.gewicht_g` | 110 | Falsches Produkt gewählt, Waage |
| DQ8 | Gewicht fehlt oder 0 | `sendung.gewicht_g` | 27 | Nicht gewogen |
| DQ9 | Zustelldatum vor Aufgabedatum | `sendung.zustell_datum` | 22 | Falsche Systemzeit eines Scanners |
| DQ10 | Empfänger existiert nicht (verwaister Fremdschlüssel) | `sendung.empfaenger_id` | 19 | FK nicht erzwungen, Löschung |
| DQ11 | Auftragstotal ≠ Summe der Sendungspreise | `auftrag.betrag_total_chf` | 49 | Manuelle Korrektur, Rundung |

Die Abfragen zum Erkennen dieser Probleme stehen in [`sql/06_pruefabfragen.sql`](../sql/06_pruefabfragen.sql).

---

## 5. Informationsbedarf und Steuerungskennzahlen

### 5.1 Anspruchsgruppen

| Rolle | Informationsbedarf |
|---|---|
| Geschäftsleitung Logistik | Entwicklung von Mengen und Umsatz, Strukturwandel Brief → Paket |
| Produktmanagement | Welche Produkte, Services und Zusatzleistungen werden genutzt? |
| Vertrieb / Key Account | Umsatz pro Kanal und Kundensegment, wichtigste Geschäftskunden |
| Betrieb / Qualitätsmanagement | Pünktlichkeit (Laufzeit-Einhaltung), Retouren, Verluste |

### 5.2 Steuerungskennzahlen (KPIs)

| KPI | Definition | Granularität |
|---|---|---|
| Sendungsmenge | Anzahl Sendungen | Tag/Monat, Produkt, Kanal, Zone |
| Umsatz CHF | Σ `preis_chf` (nach Rabatt) | wie oben |
| Ø Umsatz pro Sendung | Umsatz / Sendungsmenge | Produkt, Kundensegment |
| Rabattvolumen CHF | Σ (Listenpreis + Zusatzleistungen − effektiver Preis) | Kunde |
| Digitalisierungsgrad | Anteil Aufträge über digitale Kanäle | Monat, Kundentyp |
| Pünktlichkeitsquote | Anteil zugestellter Sendungen mit `zustell_datum ≤ soll_zustell_datum` | Service, Zone, Zielkanton, Monat |
| Ø Laufzeit | Werktage zwischen Aufgabe und Zustellung | Produkt, Zone |
| Retourenquote | Anteil Sendungen mit Status RETOURNIERT | Produkt, Kunde |
| Zusatzleistungsquote | Anteil Sendungen mit mindestens einer Zusatzleistung | Produkt, Kundentyp |

### 5.3 Fragestellungen

Diese Fragen soll das Datawarehouse beantworten. **F1 – F3 (mindestens) werden im Dashboard umgesetzt.**

- **F1 – Strukturwandel Brief/Paket:** Wie entwickeln sich Sendungsmenge und Umsatz pro Sendungsart (Brief/Paket) und
  Zone (Inland/Europa/Welt) von Monat zu Monat? Welche Saisonalität gibt es?
- **F2 – Verkaufskanäle:** Welcher Umsatz fällt pro Verkaufskanal und Kundentyp an, und wie verändert sich der Anteil
  digitaler Kanäle über die Zeit?
- **F3 – Pünktlichkeit:** Wie hoch ist die Pünktlichkeitsquote pro Service (A-Post, B-Post, Priority, Economy) und
  Zielregion? Sinkt sie in der Vorweihnachtszeit?
- **F4 – Top-Geschäftskunden:** Welche 10 Geschäftskunden erzielen den höchsten Umsatz, und welchen Anteil am
  Gesamtumsatz haben sie (ABC-Analyse)?
- **F5 – Internationaler Versand:** In welche Länder wird am meisten versandt, und welcher Anteil entfällt auf Priority?

---

## 6. Multidimensionales Datenmodell

### 6.1 Wahl des Geschäftsobjekts

Wir wählen die **Sendung** als Geschäftsobjekt. Sie ist das zentrale Ereignis, an dem sich alle Fragen aus
Kapitel 5 messen lassen (Menge, Umsatz, Laufzeit, Pünktlichkeit).

**Granularität (Grain):** Eine Zeile der Faktentabelle entspricht **einer Sendung**. Das ist die feinste verfügbare
Granularität. Damit lassen sich alle Kennzahlen beliebig aggregieren.

### 6.2 Sternschema

Quelle: [`docs/diagramme/sternschema_sendung.mmd`](../docs/diagramme/sternschema_sendung.mmd)

```mermaid
erDiagram
    DIM_DATUM ||--o{ FACT_SENDUNG : "Aufgabedatum"
    DIM_KUNDE ||--o{ FACT_SENDUNG : "Absender"
    DIM_PRODUKT ||--o{ FACT_SENDUNG : "Produkt"
    DIM_KANAL ||--o{ FACT_SENDUNG : "Verkaufskanal"
    DIM_ZIELGEBIET ||--o{ FACT_SENDUNG : "Empfängerort"
    DIM_STATUS ||--o{ FACT_SENDUNG : "Sendungsstatus"

    FACT_SENDUNG {
        bigint sendung_key PK "Grain: 1 Zeile = 1 Sendung"
        int datum_key FK
        int kunde_key FK
        int produkt_key FK
        int kanal_key FK
        int zielgebiet_key FK
        int status_key FK
        string tracking_nr "degenerierte Dimension"
        int anzahl_sendungen "Mass (immer 1)"
        decimal umsatz_chf "Mass, additiv"
        decimal listenpreis_chf "Mass, additiv"
        decimal rabatt_chf "Mass, additiv"
        int gewicht_g "Mass, additiv"
        int anzahl_zusatzleistungen "Mass, additiv"
        int laufzeit_werktage "Mass, nicht additiv (Durchschnitt)"
        int verspaetung_werktage "Mass, nicht additiv"
        int puenktlich_flag "1 = bis Soll-Datum zugestellt"
    }
    DIM_DATUM {
        int datum_key PK "YYYYMMDD"
        date datum
        int jahr
        int quartal
        int monat
        string monat_name
        int kalenderwoche
        string wochentag
        boolean ist_werktag
        boolean ist_weihnachtszeit
    }
    DIM_KUNDE {
        int kunde_key PK
        string kundennummer
        string kundentyp "Hierarchie: Kundentyp > Profil > Kunde"
        string geschaeftsprofil
        string name "Firma bzw. Vor- und Nachname"
        string ort
        string kanton
        string region
        string altersgruppe
        decimal rabatt_prozent
    }
    DIM_PRODUKT {
        int produkt_key PK
        string produkt_code
        string produkt_name
        string sendungsart "Hierarchie: Sendungsart > Zone > Service > Produkt"
        string zone
        string service
        string format
        int laufzeit_soll_werktage
        decimal listenpreis_chf
    }
    DIM_KANAL {
        int kanal_key PK
        string kanal_code
        string kanal_name
        string kanal_typ "Hierarchie: Kanaltyp > Kanal"
    }
    DIM_ZIELGEBIET {
        int zielgebiet_key PK
        string plz
        string ort
        string kanton
        string region "Hierarchie: Zone > Land > Region > Kanton > Ort"
        string land_code
        string land_name
        string zone
    }
    DIM_STATUS {
        int status_key PK
        string status
        boolean ist_abgeschlossen
    }
```

### 6.3 Dimensionen und Hierarchien

| Dimension | Hierarchie (grob → fein) | Wichtige Attribute |
|---|---|---|
| Datum (Aufgabedatum) | Jahr → Quartal → Monat → Tag; Jahr → Kalenderwoche → Tag | Wochentag, Werktag ja/nein |
| Kunde | Kundentyp → Geschäftsprofil → Kunde; Region → Kanton → Kunde | Rabatt, Altersgruppe |
| Produkt | Sendungsart → Zone → Service → Produkt | Format, Soll-Laufzeit, Listenpreis |
| Verkaufskanal | Kanaltyp (physisch/digital) → Kanal | |
| Zielgebiet | Zone → Land → Region → Kanton → Ort/PLZ | |
| Status | abgeschlossen ja/nein → Status | |

### 6.4 Kennzahlen (Fakten)

| Kennzahl | Additivität | Bemerkung |
|---|---|---|
| `anzahl_sendungen` | additiv | immer 1, vereinfacht das Zählen |
| `umsatz_chf`, `listenpreis_chf`, `rabatt_chf` | additiv | |
| `gewicht_g` | additiv | |
| `anzahl_zusatzleistungen` | additiv | aus `sendung_zusatzleistung` voraggregiert |
| `laufzeit_werktage`, `verspaetung_werktage` | nicht additiv | nur Durchschnitt/Median sinnvoll |
| `puenktlich_flag` | semi-additiv | Summe/Anzahl = Pünktlichkeitsquote |

**Designentscheide**

- `tracking_nr` bleibt als **degenerierte Dimension** in der Faktentabelle (für den Drill-through zur einzelnen Sendung).
- Zusatzleistungen sind als Zähler in den Fakten abgebildet. Eine Bridge-Tabelle wäre eine mögliche Erweiterung.
- Der Absender-Ort liegt in `dim_kunde`, der Empfänger-Ort in `dim_zielgebiet`. So lassen sich Versandströme
  (von → nach) analysieren.
- **Historisierung:** Für `dim_kunde` wäre eine SCD Typ 2 sinnvoll (Umzug, Rabattänderung). Im Rahmen dieser Arbeit
  setzen wir SCD Typ 1 (Überschreiben) um. *(TODO: definitiv entscheiden)*

---

## 7. Datawarehouse in der Medaillon-Architektur

Quelle: [`docs/diagramme/medaillon_architektur.mmd`](../docs/diagramme/medaillon_architektur.mmd)

```mermaid
flowchart LR
    SRC[("Operative DB<br/>post_oltp")] -->|Extract| B["Bronze<br/>post_dwh_bronze<br/>1:1-Kopie + Lademetadaten"]
    B -->|"Clean + DQ"| S["Silver<br/>post_dwh_silver<br/>bereinigt, standardisiert,<br/>DQ-Flags, Quarantäne"]
    S -->|"Transform"| G["Gold<br/>post_dwh_gold<br/>Sternschema + KPI-Views"]
    G --> D["Dashboard"]
```

| Schicht | Schema | Inhalt | Ladeart |
|---|---|---|---|
| Bronze | `post_dwh_bronze` | Unveränderte Kopie aller OLTP-Tabellen, ergänzt um `_ladezeitpunkt` und `_quelle` | Full Load (Overwrite) |
| Silver | `post_dwh_silver` | Bereinigte, typisierte, standardisierte Daten; jede Zeile mit DQ-Ergebnis; fehlerhafte Zeilen in Quarantäne-Tabellen | Full Load bzw. `MERGE` |
| Gold | `post_dwh_gold` | Sternschema (Fakten und Dimensionen) sowie aggregierte Sichten für das Dashboard | Full Load |

> **TODO (Team):** ETL-Notebooks in `databricks/` erstellen und hier beschreiben:
> - `10_bronze_laden` – Kopie per `CREATE OR REPLACE TABLE … AS SELECT *, current_timestamp() AS _ladezeitpunkt …`
> - `20_silver_bereinigen` – Standardisierung (Telefon → E.164, Ort-Mapping, Trim/Upper), DQ-Prüfungen, Quarantäne
> - `30_gold_sternschema` – Dimensionen mit Surrogatschlüsseln, `dim_datum` per `sequence()`, `fact_sendung`
> - Ablauf als Databricks *Job* (optional) und Screenshots der Lineage im Catalog Explorer.

---

## 8. Datenqualitätsprüfungen im ETL-Prozess

Gefordert sind mindestens zwei sinnvolle Prüfungen. Wir sehen folgende Prüfungen im Silver-Layer vor. Die Spalte
*Reaktion* beschreibt, was mit fehlerhaften Datensätzen passiert.

| ID | Prüfung (Dimension der Datenqualität) | Regel | Reaktion |
|---|---|---|---|
| P1 | **Referentielle Integrität** (Konsistenz) | Jede `sendung.empfaenger_id` existiert in `empfaenger` | Sendung in Quarantäne. In der Faktentabelle wird sie dem Dummy-Mitglied „Unbekannt“ (Key −1) zugeordnet, damit der Umsatz nicht verloren geht. |
| P2 | **Plausibilität** Gewicht (Korrektheit) | `gewicht_g > 0` und `gewicht_g ≤ produkt.max_gewicht_g` | Flag `dq_gewicht_ok = false`. Sendung zählt im Umsatz, wird aber nicht für Gewichtsauswertungen verwendet. |
| P3 | **Plausibilität** Zeitreihenfolge (Konsistenz) | `zustell_datum ≥ aufgabe_datum` | Laufzeit/Pünktlichkeit = NULL, Datensatz in Quarantäne-Report |
| P4 | **Formatprüfung** (Gültigkeit) | E-Mail per Regex, CH-PLZ 4-stellig, Telefon normierbar | Wert normieren, sonst NULL setzen und flaggen |
| P5 | **Abstimmung** (Vollständigkeit/Konsistenz) | `auftrag.betrag_total_chf = Σ sendung.preis_chf` | Abweichung protokollieren; im DWH gilt die Summe der Sendungen |
| P6 | **Eindeutigkeit** | `tracking_nr` eindeutig; Kunden-Dubletten (Name + Adresse) | Dubletten markieren (`master_kunde_id`) |
| P7 | **Vollständigkeit** Ladeprüfung | Zeilenzahl Bronze = Zeilenzahl Quelle | Job bricht ab, wenn Abweichung |

Ergebnisse jeder Prüfung schreiben wir in eine Protokolltabelle `post_dwh_silver.dq_protokoll`
(Ladezeitpunkt, Prüfung, Tabelle, Anzahl geprüft, Anzahl fehlerhaft).

> **TODO (Team):** Mindestens P1 und P2 (besser P1–P5) umsetzen. Resultate (Anzahl gefundener Fehler, vgl. Tabelle in
> 4.3) und Screenshot des DQ-Protokolls einfügen. Optional: Delta *Expectations* in einer Lakeflow/DLT-Pipeline.

---

## 9. Auswertungsdashboard

> **TODO (Team):** Dashboard in **Databricks AI/BI Dashboard** (oder Power BI) auf Basis von `post_dwh_gold` erstellen.
> Für jede Frage: Screenshot, verwendete Abfrage und Interpretation (2–4 Sätze).

| Frage | Visualisierung (Vorschlag) |
|---|---|
| F1 Strukturwandel Brief/Paket | Liniendiagramm Sendungen pro Monat, Farbe = Sendungsart; gestapelte Balken Umsatz nach Zone |
| F2 Verkaufskanäle | Balken Umsatz pro Kanal (Farbe Kundentyp); Linie Anteil digitaler Kanäle pro Quartal |
| F3 Pünktlichkeit | Balken Pünktlichkeitsquote pro Service; Linie pro Monat (Dezember-Effekt sichtbar) |
| F4 Top-Geschäftskunden | Tabelle Top 10 + Pareto-Diagramm |
| F5 International | Karte bzw. Balken nach Land, Anteil Priority |

Erste Beobachtungen aus der operativen DB (Prüfabfragen, vor DWH):

- Pakete machen gut 60 % der Sendungen, aber rund 90 % des Umsatzes aus.
- Deutlicher Peak im November/Dezember (Dezember ~2.5× so viele Sendungen wie Juni).
- Die Pünktlichkeit liegt im Inland bei rund 94–97 % (Same Day ca. 91 %), international bei 78–87 %.

---

## 10. Fazit und Reflexion

> **TODO (Team):** Am Schluss schreiben, z.B.:
> - Was hat gut funktioniert (Generator, Git-Workflow mit zwei Free-Edition-Workspaces)?
> - Grenzen: synthetische Daten, informative Constraints in Databricks, kein inkrementelles Laden
> - Mögliche Erweiterungen: SCD Typ 2, Tracking-Faktentabelle (Ereignis-Grain), inkrementelles Laden mit Change Data Feed

---

## 11. Anhang

### 11.1 Dateien der Abgabe

| Datei / Ordner | Inhalt |
|---|---|
| `bericht.pdf` | Dieser Bericht |
| `post_leistungsnachweis.dbc` | Databricks-Export: SQL-Notebooks (`sql/`) und ETL-Notebooks (`databricks/`) |
| `*.pbix` (optional) | Power-BI-Dashboard |
| `INSTALL.md` | Installationsanleitung |
| `docs/diagramme/*.mmd` | Mermaid-Quellen der Diagramme |
| `scripts/generate_data.py` | Datengenerator |

### 11.2 Installationsanleitung

Siehe [`INSTALL.md`](../INSTALL.md). Kurzfassung: `.dbc` in Databricks importieren, Notebook `sql/00_setup_alles`
ausführen, danach die ETL-Notebooks in `databricks/` in nummerierter Reihenfolge.

### 11.3 Verwendung von KI-Werkzeugen

> **TODO:** Gemäss ZHAW-Richtlinien deklarieren, wofür KI-Werkzeuge verwendet wurden (z.B. Datengenerator,
> Entwurf der Berichtsstruktur) und wie die Resultate geprüft wurden.
