#!/usr/bin/env python3
"""
Datengenerator für die operative Datenbank `post_oltp` (Szenario Schweizer Post).

Erzeugt deterministisch (fixer Seed) realistische Beispieldaten und schreibt sie als
SQL-INSERT-Skripte nach ../sql/. Die Dateien sind gleichzeitig Databricks-Notebooks
(Header "-- Databricks notebook source", Zellen getrennt durch "-- COMMAND ----------").

Aufruf (aus dem Repo-Root):   python scripts/generate_data.py
Benötigt nur die Python-Standardbibliothek (>= 3.9).

Die Produktpreise sind an das Sortiment der Schweizer Post angelehnt, aber vereinfacht
und nicht verbindlich. Alle Personen, Firmen und Adressen sind frei erfunden.

Zusätzlich werden bewusst einige Datenqualitätsprobleme eingebaut (siehe DQ_* unten),
damit im ETL-Prozess (Silver-Layer) sinnvolle Datenqualitätsprüfungen möglich sind.
"""

from __future__ import annotations

import bisect
import sys
import random
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

SEED = 20261130
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
random.seed(SEED)

OUT_DIR = Path(__file__).resolve().parent.parent / "sql"
SCHEMA = "post_oltp"

START_DATE = date(2024, 1, 1)
END_DATE = date(2026, 11, 6)       # letzter Auftragstag
STICHTAG = date(2026, 11, 10)      # Datenstand / Extraktionsdatum

N_PRIVAT = 520
N_GESCHAEFT = 130
N_EMPF_CH = 2400
N_EMPF_EUROPA = 380
N_EMPF_WELT = 220

ROWS_PER_INSERT = 1000
MAX_FILE_BYTES = 3_500_000  # Dateien bleiben unter dem Notebook-Importlimit

# Rate der absichtlich eingebauten Datenqualitätsprobleme
DQ_EMAIL_UNGUELTIG = 0.02
DQ_EMAIL_NULL = 0.02
DQ_PLZ_UNGUELTIG = 0.012
DQ_ORT_SCHREIBWEISE = 0.02
DQ_DUPLIKAT_KUNDEN = 8
DQ_GEBURTSDATUM = 0.006
DQ_GEWICHT_ZU_HOCH = 0.01
DQ_GEWICHT_NULL = 0.003
DQ_ZUSTELLUNG_VOR_AUFGABE = 0.002
DQ_BETRAG_FALSCH = 0.008
DQ_EMPFAENGER_FEHLT = 0.001

dq_log: Counter = Counter()

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------


def chf(x) -> Decimal:
    """Auf 5 Rappen runden (Schweizer Rundung)."""
    d = Decimal(str(x))
    return (d * 20).quantize(Decimal("1"), rounding=ROUND_HALF_UP) / 20


def sql_val(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, datetime):
        return f"TIMESTAMP'{v:%Y-%m-%d %H:%M:%S}'"
    if isinstance(v, date):
        return f"DATE'{v:%Y-%m-%d}'"
    if isinstance(v, Decimal):
        return f"{v:.2f}"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def ascii_slug(s: str) -> str:
    s = s.lower().replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return "".join(c for c in s if c.isalnum() or c in ".-")


def wchoice(options: dict):
    keys = list(options)
    return random.choices(keys, weights=[options[k] for k in keys], k=1)[0]


HOLIDAYS = set()
for y in (2024, 2025, 2026, 2027):
    HOLIDAYS |= {date(y, 1, 1), date(y, 1, 2), date(y, 8, 1), date(y, 12, 25), date(y, 12, 26)}
HOLIDAYS |= {date(2024, 3, 29), date(2024, 4, 1), date(2024, 5, 9), date(2024, 5, 20),
             date(2025, 4, 18), date(2025, 4, 21), date(2025, 5, 29), date(2025, 6, 9),
             date(2026, 4, 3), date(2026, 4, 6), date(2026, 5, 14), date(2026, 5, 25)}


def is_werktag(d: date) -> bool:
    return d.weekday() < 5 and d not in HOLIDAYS


def add_werktage(d: date, n: int) -> date:
    while n > 0:
        d += timedelta(days=1)
        if is_werktag(d):
            n -= 1
    return d


def next_werktag(d: date) -> date:
    while not is_werktag(d):
        d += timedelta(days=1)
    return d


# ---------------------------------------------------------------------------
# Stammdaten
# ---------------------------------------------------------------------------

LAENDER = [
    ("CH", "Schweiz", "CH"), ("LI", "Liechtenstein", "CH"),
    ("DE", "Deutschland", "EUROPA"), ("AT", "Österreich", "EUROPA"), ("FR", "Frankreich", "EUROPA"),
    ("IT", "Italien", "EUROPA"), ("NL", "Niederlande", "EUROPA"), ("ES", "Spanien", "EUROPA"),
    ("BE", "Belgien", "EUROPA"), ("PT", "Portugal", "EUROPA"), ("SE", "Schweden", "EUROPA"),
    ("PL", "Polen", "EUROPA"), ("GB", "Vereinigtes Königreich", "EUROPA"),
    ("US", "Vereinigte Staaten", "WELT"), ("CA", "Kanada", "WELT"), ("AU", "Australien", "WELT"),
    ("JP", "Japan", "WELT"), ("CN", "China", "WELT"), ("BR", "Brasilien", "WELT"),
    ("IN", "Indien", "WELT"), ("SG", "Singapur", "WELT"), ("AE", "Vereinigte Arabische Emirate", "WELT"),
    ("ZA", "Südafrika", "WELT"), ("TH", "Thailand", "WELT"),
]
LAND_ZONE = {c: z for c, _, z in LAENDER}
LAND_NAME = {c: n for c, n, _ in LAENDER}

# (PLZ, Ort, Kanton, Sprache, Gewicht ~ Bevölkerung)
ORTE_CH = [
    ("8001", "Zürich", "ZH", "de", 9), ("8004", "Zürich", "ZH", "de", 7), ("8050", "Zürich", "ZH", "de", 6),
    ("8400", "Winterthur", "ZH", "de", 6), ("8600", "Dübendorf", "ZH", "de", 2), ("8610", "Uster", "ZH", "de", 2),
    ("8800", "Thalwil", "ZH", "de", 2), ("8902", "Urdorf", "ZH", "de", 1), ("8700", "Küsnacht ZH", "ZH", "de", 1),
    ("8180", "Bülach", "ZH", "de", 2), ("3011", "Bern", "BE", "de", 6), ("3007", "Bern", "BE", "de", 4),
    ("3600", "Thun", "BE", "de", 3), ("2502", "Biel/Bienne", "BE", "de", 3), ("3400", "Burgdorf", "BE", "de", 1),
    ("3800", "Interlaken", "BE", "de", 1), ("4051", "Basel", "BS", "de", 6), ("4057", "Basel", "BS", "de", 3),
    ("4410", "Liestal", "BL", "de", 2), ("4133", "Pratteln", "BL", "de", 1), ("1201", "Genève", "GE", "fr", 6),
    ("1205", "Genève", "GE", "fr", 4), ("1003", "Lausanne", "VD", "fr", 6), ("1400", "Yverdon-les-Bains", "VD", "fr", 2),
    ("1800", "Vevey", "VD", "fr", 1), ("1110", "Morges", "VD", "fr", 1), ("6003", "Luzern", "LU", "de", 5),
    ("6020", "Emmenbrücke", "LU", "de", 2), ("6300", "Zug", "ZG", "de", 3), ("6340", "Baar", "ZG", "de", 1),
    ("9000", "St. Gallen", "SG", "de", 5), ("9500", "Wil SG", "SG", "de", 1), ("8640", "Rapperswil SG", "SG", "de", 2),
    ("5000", "Aarau", "AG", "de", 3), ("5400", "Baden", "AG", "de", 3), ("5600", "Lenzburg", "AG", "de", 1),
    ("4800", "Zofingen", "AG", "de", 1), ("8200", "Schaffhausen", "SH", "de", 2), ("8500", "Frauenfeld", "TG", "de", 2),
    ("8280", "Kreuzlingen", "TG", "de", 1), ("7000", "Chur", "GR", "de", 2), ("7500", "St. Moritz", "GR", "de", 1),
    ("7270", "Davos Platz", "GR", "de", 1), ("6900", "Lugano", "TI", "it", 4), ("6500", "Bellinzona", "TI", "it", 2),
    ("6600", "Locarno", "TI", "it", 1), ("1950", "Sion", "VS", "fr", 2), ("3900", "Brig", "VS", "de", 1),
    ("1920", "Martigny", "VS", "fr", 1), ("2000", "Neuchâtel", "NE", "fr", 2), ("2300", "La Chaux-de-Fonds", "NE", "fr", 1),
    ("1700", "Fribourg", "FR", "fr", 2), ("3280", "Murten", "FR", "de", 1), ("4500", "Solothurn", "SO", "de", 2),
    ("4600", "Olten", "SO", "de", 2), ("6430", "Schwyz", "SZ", "de", 1), ("8840", "Einsiedeln", "SZ", "de", 1),
    ("6460", "Altdorf UR", "UR", "de", 1), ("6060", "Sarnen", "OW", "de", 1), ("6370", "Stans", "NW", "de", 1),
    ("8750", "Glarus", "GL", "de", 1), ("9050", "Appenzell", "AI", "de", 1), ("9100", "Herisau", "AR", "de", 1),
    ("2800", "Delémont", "JU", "fr", 1),
]
ORTE_LI = [("9490", "Vaduz", None, "de", 1), ("9494", "Schaan", None, "de", 1)]

STRASSEN = {
    "de": ["Bahnhofstrasse", "Hauptstrasse", "Dorfstrasse", "Kirchweg", "Schulstrasse", "Seestrasse",
           "Bergstrasse", "Industriestrasse", "Gartenstrasse", "Poststrasse", "Rosenweg", "Lindenstrasse",
           "Birkenweg", "Weinbergstrasse", "Feldstrasse", "Sonnenbergstrasse", "Mühlegasse", "Ahornweg",
           "Rütistrasse", "Zürcherstrasse", "Bernstrasse", "Alpenblickweg"],
    "fr": ["Rue du Lac", "Avenue de la Gare", "Rue du Marché", "Chemin des Vignes", "Rue de Lausanne",
           "Avenue des Alpes", "Rue du Temple", "Chemin du Bois", "Rue de la Poste", "Grand-Rue"],
    "it": ["Via Cantonale", "Via Nassa", "Via San Gottardo", "Piazza Grande", "Via Lugano",
           "Via ai Monti", "Via della Posta", "Via Stazione"],
}

VORNAMEN = {
    "de": {"m": ["Daniel", "Thomas", "Peter", "Lukas", "Marco", "Stefan", "Andreas", "Michael", "Simon", "Reto",
                 "Joel", "Noah", "David", "Patrick", "Beat", "Urs", "Martin", "Fabian", "Jonas", "Adrian"],
           "f": ["Anna", "Sandra", "Nicole", "Laura", "Sarah", "Andrea", "Claudia", "Lea", "Monika", "Barbara",
                 "Julia", "Mia", "Corinne", "Petra", "Ursula", "Sabine", "Lara", "Nina", "Jasmin", "Ruth"]},
    "fr": {"m": ["Pierre", "Jean", "Nicolas", "Luc", "Olivier", "Julien", "Antoine", "Philippe", "Yves", "Mathieu"],
           "f": ["Marie", "Sophie", "Céline", "Isabelle", "Chloé", "Camille", "Nathalie", "Valérie", "Léa", "Manon"]},
    "it": {"m": ["Marco", "Luca", "Giovanni", "Matteo", "Alessandro", "Paolo", "Stefano", "Davide"],
           "f": ["Giulia", "Francesca", "Chiara", "Elena", "Sara", "Laura", "Martina", "Paola"]},
}
NACHNAMEN = {
    "de": ["Müller", "Meier", "Schmid", "Keller", "Weber", "Huber", "Schneider", "Meyer", "Steiner", "Fischer",
           "Gerber", "Brunner", "Baumann", "Frei", "Zimmermann", "Moser", "Widmer", "Wyss", "Graf", "Roth",
           "Suter", "Baumgartner", "Bachmann", "Kälin", "Lüthi", "Bühler", "Hofmann", "Marti", "Studer", "Kunz"],
    "fr": ["Favre", "Rochat", "Bonvin", "Morel", "Perrin", "Dubois", "Rey", "Chevalley", "Girard", "Python",
           "Monnier", "Jaquet"],
    "it": ["Bernasconi", "Rossi", "Ferrari", "Bianchi", "Fontana", "Rusconi", "Galli", "Colombo", "Pedrazzini"],
}

AUSLAND_ORTE = {
    "DE": [("10115", "Berlin"), ("80331", "München"), ("20095", "Hamburg"), ("50667", "Köln"),
           ("70173", "Stuttgart"), ("79098", "Freiburg im Breisgau"), ("78462", "Konstanz")],
    "AT": [("1010", "Wien"), ("6020", "Innsbruck"), ("5020", "Salzburg"), ("6900", "Bregenz")],
    "FR": [("75001", "Paris"), ("69001", "Lyon"), ("68100", "Mulhouse"), ("74000", "Annecy")],
    "IT": [("20121", "Milano"), ("00184", "Roma"), ("22100", "Como"), ("10121", "Torino")],
    "NL": [("1012 AB", "Amsterdam"), ("3011 AD", "Rotterdam")],
    "ES": [("28001", "Madrid"), ("08001", "Barcelona"), ("46001", "Valencia")],
    "BE": [("1000", "Bruxelles"), ("2000", "Antwerpen")],
    "PT": [("1100-148", "Lisboa"), ("4000-322", "Porto")],
    "SE": [("111 20", "Stockholm"), ("411 03", "Göteborg")],
    "PL": [("00-001", "Warszawa"), ("30-001", "Kraków")],
    "GB": [("SW1A 1AA", "London"), ("M1 1AE", "Manchester"), ("EH1 1YZ", "Edinburgh")],
    "US": [("10001", "New York NY"), ("94103", "San Francisco CA"), ("60601", "Chicago IL"), ("98101", "Seattle WA")],
    "CA": [("M5V 2T6", "Toronto ON"), ("H2Y 1C6", "Montréal QC"), ("V6B 1A1", "Vancouver BC")],
    "AU": [("2000", "Sydney NSW"), ("3000", "Melbourne VIC")],
    "JP": [("100-0001", "Tokyo"), ("530-0001", "Osaka")],
    "CN": [("200000", "Shanghai"), ("100000", "Beijing")],
    "BR": [("01000-000", "São Paulo"), ("20000-000", "Rio de Janeiro")],
    "IN": [("400001", "Mumbai"), ("560001", "Bengaluru")],
    "SG": [("018989", "Singapore")],
    "AE": [(None, "Dubai"), (None, "Abu Dhabi")],
    "ZA": [("8001", "Cape Town"), ("2001", "Johannesburg")],
    "TH": [("10200", "Bangkok")],
}
AUSLAND_STRASSEN = {
    "DE": ["Hauptstraße", "Bahnhofstraße", "Lindenallee", "Gartenweg"], "AT": ["Hauptplatz", "Mariahilfer Straße", "Kirchengasse"],
    "FR": ["Rue de la République", "Boulevard Voltaire", "Avenue Jean Jaurès"], "IT": ["Via Roma", "Corso Italia", "Via Garibaldi"],
    "NL": ["Keizersgracht", "Kerkstraat"], "ES": ["Calle Mayor", "Gran Vía"], "BE": ["Rue Neuve", "Meir"],
    "PT": ["Rua Augusta", "Avenida da Liberdade"], "SE": ["Drottninggatan", "Storgatan"], "PL": ["ul. Marszałkowska", "ul. Floriańska"],
    "GB": ["High Street", "Station Road", "Church Lane"], "US": ["Main Street", "Broadway", "Oak Avenue", "Market Street"],
    "CA": ["King Street", "Rue Sainte-Catherine"], "AU": ["George Street", "Collins Street"], "JP": ["Chiyoda", "Umeda"],
    "CN": ["Nanjing Road", "Wangfujing Street"], "BR": ["Avenida Paulista", "Rua Augusta"], "IN": ["MG Road", "Marine Drive"],
    "SG": ["Orchard Road"], "AE": ["Sheikh Zayed Road", "Al Wasl Road"], "ZA": ["Long Street", "Main Road"], "TH": ["Sukhumvit Road"],
}
AUSLAND_NAMEN = {
    "DE": (["Lena", "Felix", "Hannah", "Jan", "Katrin", "Tobias"], ["Schmidt", "Wagner", "Becker", "Hoffmann", "Krause"]),
    "AT": (["Lisa", "Florian", "Katharina", "Markus"], ["Gruber", "Pichler", "Steiner", "Leitner"]),
    "FR": (["Camille", "Hugo", "Manon", "Louis"], ["Martin", "Bernard", "Petit", "Durand", "Leroy"]),
    "IT": (["Giulia", "Lorenzo", "Alessia", "Francesco"], ["Russo", "Esposito", "Romano", "Greco"]),
    "GB": (["Oliver", "Emily", "Harry", "Charlotte"], ["Smith", "Jones", "Taylor", "Brown"]),
    "US": (["James", "Olivia", "Ethan", "Ava", "Michael"], ["Johnson", "Williams", "Miller", "Davis", "Garcia"]),
    "JP": (["Hiroshi", "Yuki", "Haruto", "Sakura"], ["Tanaka", "Suzuki", "Sato", "Takahashi"]),
    "CN": (["Wei", "Li", "Fang", "Jun"], ["Wang", "Zhang", "Liu", "Chen"]),
    "IN": (["Priya", "Arjun", "Ananya", "Rahul"], ["Sharma", "Patel", "Kumar", "Singh"]),
    "BR": (["Lucas", "Ana", "Gabriel", "Beatriz"], ["Silva", "Santos", "Oliveira", "Souza"]),
}
GENERISCH_NAMEN = (["Anna", "Daniel", "Sofia", "Lukas", "Emma", "Carlos", "Nina", "Peter", "Maria", "Alex"],
                   ["Andersson", "Jansen", "Kowalski", "Fernandes", "Peeters", "Nielsen", "Brown", "Novak", "Ali", "Lee"])

BRANCHEN = ["Druckerei", "Weinhandel", "Versand", "Elektro", "Sport", "Optik", "Bücher", "Uhren", "Käserei",
            "Möbel", "Velo", "Mode", "Kosmetik", "Treuhand", "Apotheke", "Spielwaren", "Kaffee", "Schokolade",
            "Garten", "Bürobedarf", "Tee", "Outdoor", "Haushalt", "Naturkosmetik"]
FIRMEN_PREFIX = ["Alpen", "Berg", "See", "Helvetia", "Edelweiss", "Rhein", "Jura", "Säntis", "Pilatus",
                 "Limmat", "Aare", "Gotthard", "Rigi", "Bodensee", "Emmentaler", "Matterhorn", "Linden"]
RECHTSFORM = ["AG", "GmbH", "GmbH", "AG", "& Co.", ""]

# Verkaufskanäle
KANAELE = [
    (1, "FILIALE", "Postfiliale", "PHYSISCH", "Aufgabe und Bezahlung am Schalter einer Postfiliale"),
    (2, "PARTNERFILIALE", "Filiale mit Partner", "PHYSISCH", "Postschalter in einem Partnergeschäft (z.B. Dorfladen)"),
    (3, "MYPOST24", "My Post 24-Automat", "PHYSISCH", "Paketautomat für Aufgabe und Abholung rund um die Uhr"),
    (4, "PICKATHOME", "Abholung pick@home", "PHYSISCH", "Abholung der Sendungen beim Kunden zu Hause bzw. im Geschäft"),
    (5, "WEBSTAMP", "Online-Porto / WebStamp", "DIGITAL", "Porto online kaufen und selbst ausdrucken"),
    (6, "POSTAPP", "Post-App", "DIGITAL", "Frankierung und Paketetikette über die Post-App"),
    (7, "BUSINESS_PORTAL", "Geschäftskundenportal / Versand-API", "DIGITAL", "Etikettenerstellung über Geschäftskundenportal oder Schnittstelle"),
]
KANAL_ID = {code: kid for kid, code, *_ in KANAELE}

# Produkte: (code, name, art, zone, service, format, min_g, max_g, max_dicke_mm, laufzeit, preis)
PRODUKTE_DEF = [
    ("BRI-CH-A-STD", "Standardbrief A-Post (bis B5, 100 g)", "BRIEF", "CH", "A-POST", "Standardbrief", 1, 100, 20, 1, 1.20),
    ("BRI-CH-B-STD", "Standardbrief B-Post (bis B5, 100 g)", "BRIEF", "CH", "B-POST", "Standardbrief", 1, 100, 20, 3, 1.00),
    ("BRI-CH-A-GRO", "Grossbrief A-Post (bis B4, 500 g)", "BRIEF", "CH", "A-POST", "Grossbrief", 1, 500, 20, 1, 2.20),
    ("BRI-CH-B-GRO", "Grossbrief B-Post (bis B4, 500 g)", "BRIEF", "CH", "B-POST", "Grossbrief", 1, 500, 20, 3, 1.90),
    ("BRI-CH-A-MAX", "Maxibrief A-Post (bis B4, 1 kg, dicker als 2 cm bis 5 cm)", "BRIEF", "CH", "A-POST", "Maxibrief", 1, 1000, 50, 1, 4.70),
    ("BRI-CH-B-MAX", "Maxibrief B-Post (bis B4, 1 kg, dicker als 2 cm bis 5 cm)", "BRIEF", "CH", "B-POST", "Maxibrief", 1, 1000, 50, 3, 4.00),
    ("PAK-CH-ECO-S", "Paket Economy S (bis 2 kg)", "PAKET", "CH", "ECONOMY", "S", 1, 2000, None, 2, 9.00),
    ("PAK-CH-ECO-M", "Paket Economy M (bis 10 kg)", "PAKET", "CH", "ECONOMY", "M", 2001, 10000, None, 2, 12.70),
    ("PAK-CH-ECO-L", "Paket Economy L (bis 30 kg)", "PAKET", "CH", "ECONOMY", "L", 10001, 30000, None, 2, 25.00),
    ("PAK-CH-PRI-S", "Paket Priority S (bis 2 kg)", "PAKET", "CH", "PRIORITY", "S", 1, 2000, None, 1, 11.00),
    ("PAK-CH-PRI-M", "Paket Priority M (bis 10 kg)", "PAKET", "CH", "PRIORITY", "M", 2001, 10000, None, 1, 14.70),
    ("PAK-CH-PRI-L", "Paket Priority L (bis 30 kg)", "PAKET", "CH", "PRIORITY", "L", 10001, 30000, None, 1, 27.00),
    ("PAK-CH-SAMEDAY", "Paket Same Day (bis 30 kg)", "PAKET", "CH", "SAME-DAY", "bis 30 kg", 1, 30000, None, 0, 49.00),
]
for zone, zname, pri, eco in [("EUROPA", "Europa", (2.40, 6.00, 18.00), (2.20, 5.00, 15.00)),
                               ("WELT", "Welt", (2.70, 9.00, 32.00), (2.50, 7.50, 26.00))]:
    zk = "EU" if zone == "EUROPA" else "WE"
    for (fmt, lo, hi, dicke), p_pri, p_eco in zip(
            [("bis 20 g", 1, 20, 20), ("bis 250 g", 21, 250, 20), ("bis 2 kg", 251, 2000, 50)], pri, eco):
        k = fmt.replace("bis ", "").replace(" ", "")
        lz_p, lz_e = (3, 6) if zone == "EUROPA" else (6, 15)
        PRODUKTE_DEF.append((f"BRI-{zk}-PRI-{k}", f"Brief International {zname} Priority ({fmt})", "BRIEF", zone, "PRIORITY", fmt, lo, hi, dicke, lz_p, p_pri))
        PRODUKTE_DEF.append((f"BRI-{zk}-ECO-{k}", f"Brief International {zname} Economy ({fmt})", "BRIEF", zone, "ECONOMY", fmt, lo, hi, dicke, lz_e, p_eco))
for zone, zname, pri, eco in [("EUROPA", "Europa", (38.0, 62.0, 115.0), (31.0, 52.0, 98.0)),
                               ("WELT", "Welt", (52.0, 110.0, 230.0), (42.0, 88.0, 185.0))]:
    zk = "EU" if zone == "EUROPA" else "WE"
    for (fmt, lo, hi), p_pri, p_eco in zip([("bis 2 kg", 1, 2000), ("bis 10 kg", 2001, 10000), ("bis 30 kg", 10001, 30000)], pri, eco):
        k = fmt.replace("bis ", "").replace(" ", "")
        lz_p, lz_e = (4, 8) if zone == "EUROPA" else (7, 18)
        PRODUKTE_DEF.append((f"PAK-{zk}-PRI-{k}", f"Paket International {zname} Priority ({fmt})", "PAKET", zone, "PRIORITY", fmt, lo, hi, None, lz_p, p_pri))
        PRODUKTE_DEF.append((f"PAK-{zk}-ECO-{k}", f"Paket International {zname} Economy ({fmt})", "PAKET", zone, "ECONOMY", fmt, lo, hi, None, lz_e, p_eco))

PRODUKTE = []
for i, p in enumerate(PRODUKTE_DEF, start=1):
    code, name, art, zone, service, fmt, lo, hi, dicke, lz, preis = p
    PRODUKTE.append(dict(produkt_id=i, produkt_code=code, produkt_name=name, sendungsart=art, zone=zone,
                         service=service, format=fmt, min_gewicht_g=lo, max_gewicht_g=hi, max_dicke_mm=dicke,
                         laufzeit_soll_werktage=lz, preis_chf=chf(preis), aktiv=True))
PROD_BY_CODE = {p["produkt_code"]: p for p in PRODUKTE}

ZUSATZ = [
    (1, "R", "Einschreiben", "BRIEF", 5.30),
    (2, "SI", "Signature (Unterschrift bei Zustellung)", "PAKET", 1.50),
    (3, "AS", "Assurance (Wertdeklaration bis CHF 5000)", "PAKET", 4.50),
    (4, "BLN", "Nachnahme", "PAKET", 18.00),
    (5, "SA", "Samstagszustellung", "PAKET", 17.00),
    (6, "SP", "Sperrgut", "PAKET", 45.00),
]
ZUSATZ_PREIS = {z[0]: chf(z[4]) for z in ZUSATZ}

# ---------------------------------------------------------------------------
# Standorte
# ---------------------------------------------------------------------------
standorte = []


def add_standort(name, typ, plz, ort, kanton, land="CH"):
    sid = len(standorte) + 1
    standorte.append(dict(standort_id=sid, standort_name=name, standort_typ=typ, plz=plz, ort=ort, kanton=kanton, land_code=land))
    return sid


BZ = {"OST": add_standort("Briefzentrum Zürich-Mülligen", "BRIEFZENTRUM", "8010", "Zürich", "ZH"),
      "MITTE": add_standort("Briefzentrum Härkingen", "BRIEFZENTRUM", "4620", "Härkingen", "SO"),
      "WEST": add_standort("Briefzentrum Eclépens", "BRIEFZENTRUM", "1312", "Eclépens", "VD")}
PZ = {"OST": add_standort("Paketzentrum Frauenfeld", "PAKETZENTRUM", "8500", "Frauenfeld", "TG"),
      "MITTE": add_standort("Paketzentrum Härkingen", "PAKETZENTRUM", "4620", "Härkingen", "SO"),
      "WEST": add_standort("Paketzentrum Daillens", "PAKETZENTRUM", "1306", "Daillens", "VD")}
REG_PZ = {"GR": add_standort("Regionales Paketzentrum Untervaz", "PAKETZENTRUM", "7204", "Untervaz", "GR"),
          "VS": add_standort("Regionales Paketzentrum Vétroz", "PAKETZENTRUM", "1963", "Vétroz", "VS"),
          "TI": add_standort("Regionales Paketzentrum Cadenazzo", "PAKETZENTRUM", "6593", "Cadenazzo", "TI"),
          "BE": add_standort("Regionales Paketzentrum Ostermundigen", "PAKETZENTRUM", "3072", "Ostermundigen", "BE")}
AUSTAUSCH = add_standort("Internationale Austauschstelle Zürich-Mülligen", "AUSTAUSCHSTELLE", "8010", "Zürich", "ZH")

REGION = {}
for k in ["ZH", "SH", "TG", "SG", "AR", "AI", "GR", "GL", "SZ", "ZG"]:
    REGION[k] = "OST"
for k in ["BE", "SO", "BS", "BL", "AG", "LU", "UR", "OW", "NW", "TI"]:
    REGION[k] = "MITTE"
for k in ["GE", "VD", "VS", "NE", "JU", "FR"]:
    REGION[k] = "WEST"
REGION[None] = "OST"  # Liechtenstein

HAUPTORT = {"ZH": ("8000", "Zürich"), "BE": ("3000", "Bern"), "LU": ("6000", "Luzern"), "UR": ("6460", "Altdorf UR"),
            "SZ": ("6430", "Schwyz"), "OW": ("6060", "Sarnen"), "NW": ("6370", "Stans"), "GL": ("8750", "Glarus"),
            "ZG": ("6300", "Zug"), "FR": ("1700", "Fribourg"), "SO": ("4500", "Solothurn"), "BS": ("4000", "Basel"),
            "BL": ("4410", "Liestal"), "SH": ("8200", "Schaffhausen"), "AR": ("9100", "Herisau"), "AI": ("9050", "Appenzell"),
            "SG": ("9000", "St. Gallen"), "GR": ("7000", "Chur"), "AG": ("5000", "Aarau"), "TG": ("8500", "Frauenfeld"),
            "TI": ("6500", "Bellinzona"), "VD": ("1000", "Lausanne"), "VS": ("1950", "Sion"), "NE": ("2000", "Neuchâtel"),
            "GE": ("1200", "Genève"), "JU": ("2800", "Delémont")}
ZUSTELLSTELLE = {k: add_standort(f"Distributionsbasis {o}", "ZUSTELLSTELLE", p, o, k) for k, (p, o) in HAUPTORT.items()}
ZUSTELLSTELLE[None] = add_standort("Distributionsbasis Vaduz", "ZUSTELLSTELLE", "9490", "Vaduz", None, "LI")

FILIALEN_BY_KANTON = defaultdict(list)
PARTNER_BY_KANTON = defaultdict(list)
AUTOMATEN_BY_KANTON = defaultdict(list)
seen_orte = set()
for plz, ort, kt, _, w in ORTE_CH + ORTE_LI:
    if ort in seen_orte:
        continue
    seen_orte.add(ort)
    land = "LI" if kt is None else "CH"
    FILIALEN_BY_KANTON[kt].append(add_standort(f"Filiale {ort}", "FILIALE", plz, ort, kt, land))
    if w >= 2:
        AUTOMATEN_BY_KANTON[kt].append(add_standort(f"My Post 24 {ort} Bahnhof", "AUTOMAT", plz, ort, kt, land))
    if w <= 2 and random.random() < 0.6:
        PARTNER_BY_KANTON[kt].append(add_standort(f"Partnerfiliale Dorfladen {ort}", "PARTNERFILIALE", plz, ort, kt, land))

AUSLAND_STANDORT = {}
for c, n, z in LAENDER:
    if z != "CH":
        AUSLAND_STANDORT[c] = add_standort(f"Partnerpost {n}", "AUSLAND", None, None, None, c)

# ---------------------------------------------------------------------------
# Adressen, Kunden, Empfänger
# ---------------------------------------------------------------------------
adressen = []


def ort_variante(ort):
    """DQ: abweichende Schreibweise eines Ortes."""
    variants = {"Zürich": ["Zuerich", "ZÜRICH", "Zurich"], "Genève": ["Geneve", "Genf", "GENEVE"],
                "Neuchâtel": ["Neuchatel", "Neuenburg"], "Luzern": ["Lucerne", "LUZERN"], "Bern": ["BERN", "Berne"],
                "Basel": ["Bâle", "BASEL"], "St. Gallen": ["St.Gallen", "Sankt Gallen", "St Gallen"]}
    return random.choice(variants.get(ort, [ort.upper()]))


def neue_adresse_ch(dq=False, ort_tuple=None):
    plz, ort, kt, sprache, _ = ort_tuple or random.choices(ORTE_CH + ORTE_LI, weights=[o[4] for o in ORTE_CH + ORTE_LI])[0]
    land = "LI" if kt is None else "CH"
    strasse = random.choice(STRASSEN[sprache])
    nr = str(random.randint(1, 120)) + (random.choice(["a", "b", ""]) if random.random() < 0.08 else "")
    if dq and random.random() < DQ_PLZ_UNGUELTIG:
        plz = random.choice([plz[:3], plz + "0", "CH-" + plz, plz[:2] + "O" + plz[3:]])
        dq_log["adresse.plz ungültig"] += 1
    if dq and random.random() < DQ_ORT_SCHREIBWEISE:
        ort = ort_variante(ort)
        dq_log["adresse.ort abweichende Schreibweise"] += 1
    aid = len(adressen) + 1
    adressen.append(dict(adresse_id=aid, strasse=strasse, hausnummer=nr, plz=plz, ort=ort, kanton=kt, land_code=land))
    return aid, sprache


def neue_adresse_ausland(land):
    plz, ort = random.choice(AUSLAND_ORTE[land])
    strasse = random.choice(AUSLAND_STRASSEN[land])
    nr = str(random.randint(1, 250))
    aid = len(adressen) + 1
    adressen.append(dict(adresse_id=aid, strasse=strasse, hausnummer=nr, plz=plz, ort=ort, kanton=None, land_code=land))
    return aid


ADR = {}  # adresse_id -> dict (wird später befüllt)

EMAIL_DOMAINS = {"bluewin.ch": 30, "gmail.com": 30, "gmx.ch": 12, "outlook.com": 8, "hispeed.ch": 6, "sunrise.ch": 6, "protonmail.com": 3}


def telefon_privat():
    vorwahl = random.choice(["76", "77", "78", "79", "79", "78"])
    n = f"{random.randint(100, 999)} {random.randint(10, 99)} {random.randint(10, 99)}"
    return format_telefon(vorwahl, n)


def telefon_festnetz():
    vorwahl = random.choice(["44", "31", "61", "22", "21", "41", "71", "52", "56", "62", "91"])
    n = f"{random.randint(100, 999)} {random.randint(10, 99)} {random.randint(10, 99)}"
    return format_telefon(vorwahl, n)


def format_telefon(vorwahl, n):
    """DQ: Telefonnummern in uneinheitlichen Formaten."""
    r = random.random()
    if r < 0.70:
        return f"+41 {vorwahl} {n}"
    if r < 0.82:
        dq_log["kunde.telefon abweichendes Format"] += 1
        return f"0{vorwahl}{n.replace(' ', '')}"
    if r < 0.92:
        dq_log["kunde.telefon abweichendes Format"] += 1
        return f"0{vorwahl} {n}"
    if r < 0.96:
        dq_log["kunde.telefon abweichendes Format"] += 1
        return f"0041{vorwahl}{n.replace(' ', '')}"
    dq_log["kunde.telefon fehlt"] += 1
    return None


def make_email(local, domain):
    r = random.random()
    if r < DQ_EMAIL_NULL:
        dq_log["kunde.email fehlt"] += 1
        return None
    if r < DQ_EMAIL_NULL + DQ_EMAIL_UNGUELTIG:
        dq_log["kunde.email ungültig"] += 1
        return random.choice([f"{local}{domain}", f"{local}@{domain.split('.')[0]}", f"{local}@@{domain}", f"{local} @{domain}"])
    return f"{local}@{domain}"


kunden = []
kunde_meta = {}  # kunde_id -> Zusatzinfos für die Simulation


def random_date(a: date, b: date) -> date:
    return a + timedelta(days=random.randint(0, (b - a).days))


for i in range(N_PRIVAT + N_GESCHAEFT):
    kid = i + 1
    geschaeft = i >= N_PRIVAT
    aid, sprache = neue_adresse_ch(dq=True)
    sex = random.choice("mf")
    vn = random.choice(VORNAMEN[sprache][sex])
    nn = random.choice(NACHNAMEN[sprache])
    if geschaeft:
        profil = wchoice({"ECOMMERCE": 50, "BRIEFVERSENDER": 30, "GEMISCHT": 20})
        if random.random() < 0.4:
            firma = f"{nn} {random.choice(BRANCHEN)} {random.choice(RECHTSFORM)}".strip()
        else:
            firma = f"{random.choice(FIRMEN_PREFIX)} {random.choice(BRANCHEN)} {random.choice(RECHTSFORM)}".strip()
        domain = ascii_slug(firma.replace("& Co.", "").replace(" AG", "").replace(" GmbH", "")).replace(" ", "")
        domain = "-".join(ascii_slug(w) for w in firma.replace("& Co.", "").replace(" AG", "").replace(" GmbH", "").split()) + ".ch"
        email = make_email(random.choice(["info", "versand", "logistik", "buchhaltung"]), domain)
        tel = telefon_festnetz()
        rabatt = Decimal(random.choice(["0", "5", "5", "10", "10", "15", "20"]))
        geb = None
        anrede = "Firma"
        reg = random_date(date(2010, 1, 1), date(2026, 3, 31))
    else:
        profil = None
        firma = None
        local = f"{ascii_slug(vn)}.{ascii_slug(nn)}" if random.random() < 0.7 else f"{ascii_slug(vn)[0]}{ascii_slug(nn)}{random.randint(1, 99)}"
        email = make_email(local, wchoice(EMAIL_DOMAINS))
        tel = telefon_privat()
        rabatt = Decimal("0")
        geb = random_date(date(1940, 1, 1), date(2006, 12, 31))
        if random.random() < DQ_GEBURTSDATUM:
            geb = random.choice([date(1900, 1, 1), date(2099, 1, 1), date(2027, 5, 17)])
            dq_log["kunde.geburtsdatum unplausibel"] += 1
        anrede = "Herr" if sex == "m" else "Frau"
        reg = random_date(date(2012, 1, 1), date(2026, 8, 31))
    kunden.append(dict(kunde_id=kid, kundennummer=f"KD{10000000 + kid * 7 + random.randint(0, 6)}", kundentyp="GESCHAEFT" if geschaeft else "PRIVAT",
                       geschaeftsprofil=profil, anrede=anrede, vorname=vn, nachname=nn, firmenname=firma, email=email,
                       telefon=tel, geburtsdatum=geb, rabatt_prozent=rabatt, registriert_am=reg, adresse_id=aid))

# DQ: Dubletten – gleiche Person nochmals als neuer Kunde erfasst (z.B. nach Umzug/Neuregistrierung)
for orig in random.sample([k for k in kunden if k["kundentyp"] == "PRIVAT"], DQ_DUPLIKAT_KUNDEN):
    kid = len(kunden) + 1
    dup = dict(orig)
    dup["kunde_id"] = kid
    dup["kundennummer"] = f"KD{10000000 + kid * 7 + random.randint(0, 6)}"
    dup["email"] = (orig["email"] or "").upper() or None
    dup["registriert_am"] = random_date(date(2024, 1, 1), date(2026, 6, 30))
    kunden.append(dup)
    dq_log["kunde Dublette"] += 1

ADR = {a["adresse_id"]: a for a in adressen}

for k in kunden:
    geschaeft = k["kundentyp"] == "GESCHAEFT"
    profil = k["geschaeftsprofil"]
    if geschaeft:
        aktivitaet = random.paretovariate(1.6)  # wenige sehr grosse Versender
        n_auftraege = int(min(90, 5 + aktivitaet * 7))
        bevorzugt = wchoice({"BUSINESS_PORTAL": 60, "WEBSTAMP": 15, "FILIALE": 12, "PICKATHOME": 13})
    else:
        aktivitaet = random.paretovariate(2.2)
        n_auftraege = int(min(45, random.randint(1, 6) * aktivitaet))
        bevorzugt = wchoice({"FILIALE": 40, "PARTNERFILIALE": 10, "POSTAPP": 20, "WEBSTAMP": 15, "MYPOST24": 12, "PICKATHOME": 3})
    kunde_meta[k["kunde_id"]] = dict(n_auftraege=n_auftraege, bevorzugt=bevorzugt, profil=profil,
                                    kanton=ADR[k["adresse_id"]]["kanton"], geschaeft=geschaeft)

empfaenger = []
EMPF_BY_ZONE = defaultdict(list)


def neuer_empfaenger(zone):
    eid = len(empfaenger) + 1
    if zone == "CH":
        aid, sprache = neue_adresse_ch(dq=False)
        sex = random.choice("mf")
        vn, nn = random.choice(VORNAMEN[sprache][sex]), random.choice(NACHNAMEN[sprache])
        land = ADR_LAND = adressen[aid - 1]["land_code"]
    else:
        laender = [c for c, _, z in LAENDER if z == zone]
        gew = {"DE": 30, "AT": 12, "FR": 14, "IT": 12, "GB": 8, "NL": 5, "ES": 5, "BE": 3, "PT": 4, "SE": 3, "PL": 3,
               "US": 35, "CA": 10, "AU": 8, "JP": 7, "CN": 8, "BR": 5, "IN": 7, "SG": 5, "AE": 6, "ZA": 4, "TH": 5}
        land = random.choices(laender, weights=[gew[c] for c in laender])[0]
        aid = neue_adresse_ausland(land)
        firsts, lasts = AUSLAND_NAMEN.get(land, GENERISCH_NAMEN)
        vn, nn = random.choice(firsts), random.choice(lasts)
    firma = None
    if random.random() < 0.12:
        firma = f"{random.choice(FIRMEN_PREFIX if zone == 'CH' else ['Global', 'Euro', 'Nova', 'Prime', 'Blue'])} {random.choice(['Trading', 'Handel', 'Services', 'Solutions', 'Shop'])} {random.choice(['AG', 'GmbH', 'Ltd', 'SA', 'Inc'])}"
        if random.random() < 0.5:
            vn = nn = None
    empfaenger.append(dict(empfaenger_id=eid, vorname=vn, nachname=nn, firmenname=firma, adresse_id=aid))
    EMPF_BY_ZONE[zone].append(eid)
    return eid


for _ in range(N_EMPF_CH):
    neuer_empfaenger("CH")
for _ in range(N_EMPF_EUROPA):
    neuer_empfaenger("EUROPA")
for _ in range(N_EMPF_WELT):
    neuer_empfaenger("WELT")
ADR = {a["adresse_id"]: a for a in adressen}
EMPF = {e["empfaenger_id"]: e for e in empfaenger}

# ---------------------------------------------------------------------------
# Zeit-Gewichtung (Saisonalität, Wochentage, Trend)
# ---------------------------------------------------------------------------
TAGE = []
CUM = []
MONAT_FAKTOR = {1: 0.85, 2: 0.9, 3: 1.0, 4: 0.95, 5: 0.95, 6: 0.9, 7: 0.8, 8: 0.85, 9: 1.0, 10: 1.05, 11: 1.35, 12: 1.75}
WT_FAKTOR = [1.1, 1.05, 1.0, 1.0, 1.1, 0.55, 0.12]
total = 0.0
d = START_DATE
while d <= END_DATE:
    w = MONAT_FAKTOR[d.month] * WT_FAKTOR[d.weekday()] * (1 + 0.04 * (d - START_DATE).days / 365)
    if d in HOLIDAYS:
        w *= 0.15
    if d.month == 11 and 24 <= d.day <= 30 and d.weekday() >= 3:  # Black Friday / Cyber Monday Effekt
        w *= 1.6
    total += w
    TAGE.append(d)
    CUM.append(total)
    d += timedelta(days=1)


def sample_tag(von: date) -> date:
    i0 = max(0, (von - START_DATE).days)
    lo = CUM[i0 - 1] if i0 > 0 else 0.0
    u = random.uniform(lo, CUM[-1])
    return TAGE[bisect.bisect_left(CUM, u)]


# ---------------------------------------------------------------------------
# Tracking-Nummern
# ---------------------------------------------------------------------------
_tracking_seq = 0


def s10(prefix: str, nummer: int) -> str:
    """UPU S10 Sendungsnummer inkl. Prüfziffer, z.B. RR123456785CH."""
    digits = f"{nummer:08d}"
    s = sum(int(c) * w for c, w in zip(digits, [8, 6, 4, 2, 3, 5, 9, 7]))
    check = 11 - (s % 11)
    check = 0 if check == 10 else 5 if check == 11 else check
    return f"{prefix}{digits}{check}CH"


def neue_tracking_nr(produkt, kunde_id, einschreiben):
    global _tracking_seq
    _tracking_seq += 1
    if produkt["zone"] == "CH":
        art = "99" if produkt["sendungsart"] == "PAKET" else "98"
        code = {"PRIORITY": "60", "ECONOMY": "34", "SAME-DAY": "00", "A-POST": "40" if einschreiben else "01", "B-POST": "02"}[produkt["service"]]
        return f"{art}.{code}.{(100000 + kunde_id * 13) % 1000000:06d}.{(10000000 + _tracking_seq * 37) % 100000000:08d}"
    if produkt["sendungsart"] == "PAKET":
        prefix = "CP" if produkt["service"] == "PRIORITY" else "CV"
    else:
        prefix = "RR" if einschreiben else ("LA" if produkt["service"] == "PRIORITY" else "LE")
    return s10(prefix, 10000000 + _tracking_seq * 7)


# ---------------------------------------------------------------------------
# Simulation der Aufträge, Sendungen und Tracking-Events
# ---------------------------------------------------------------------------
auftraege, sendungen, sendung_zusatz, events = [], [], [], []

PRIVAT_FAVORITEN = {}


def kanal_waehlen(meta, jahr):
    if random.random() < 0.6:
        return meta["bevorzugt"]
    if meta["geschaeft"]:
        return wchoice({"BUSINESS_PORTAL": 50, "WEBSTAMP": 20, "FILIALE": 15, "PICKATHOME": 10, "PARTNERFILIALE": 5})
    digital = {2024: 1.0, 2025: 1.25, 2026: 1.5}[jahr]
    return wchoice({"FILIALE": 38 / digital, "PARTNERFILIALE": 12 / digital, "POSTAPP": 18 * digital,
                    "WEBSTAMP": 14 * digital, "MYPOST24": 12, "PICKATHOME": 4})


def produkt_waehlen(meta, kanal, jahr):
    profil = meta["profil"]
    if kanal in ("MYPOST24", "PICKATHOME"):
        p_brief = 0.0
    elif profil == "ECOMMERCE":
        p_brief = 0.12
    elif profil == "BRIEFVERSENDER":
        p_brief = 0.88
    elif profil == "GEMISCHT":
        p_brief = 0.5
    else:
        p_brief = 0.62
    p_brief *= {2024: 1.0, 2025: 0.95, 2026: 0.9}[jahr]  # Briefmengen rückläufig
    brief = random.random() < p_brief
    if profil == "ECOMMERCE":
        zone = wchoice({"CH": 82, "EUROPA": 13, "WELT": 5})
    else:
        zone = wchoice({"CH": 88, "EUROPA": 8, "WELT": 4})
    if kanal == "MYPOST24":
        zone = "CH"
    if brief:
        if zone == "CH":
            fmt = wchoice({"STD": 70, "GRO": 20, "MAX": 10})
            svc = wchoice({"A": 45, "B": 55}) if profil != "BRIEFVERSENDER" else wchoice({"A": 30, "B": 70})
            return PROD_BY_CODE[f"BRI-CH-{svc}-{fmt}"]
        zk = "EU" if zone == "EUROPA" else "WE"
        k = wchoice({"20g": 60, "250g": 30, "2kg": 10})
        return PROD_BY_CODE[f"BRI-{zk}-{wchoice({'PRI': 55, 'ECO': 45})}-{k}"]
    if zone == "CH":
        if random.random() < 0.03:
            return PROD_BY_CODE["PAK-CH-SAMEDAY"]
        svc = wchoice({"ECO": 60, "PRI": 40}) if profil != "ECOMMERCE" else wchoice({"ECO": 45, "PRI": 55})
        return PROD_BY_CODE[f"PAK-CH-{svc}-{wchoice({'S': 52, 'M': 37, 'L': 11})}"]
    zk = "EU" if zone == "EUROPA" else "WE"
    return PROD_BY_CODE[f"PAK-{zk}-{wchoice({'PRI': 45, 'ECO': 55})}-{wchoice({'2kg': 50, '10kg': 35, '30kg': 15})}"]


def gewicht_und_masse(p, sperrgut):
    lo = p["min_gewicht_g"]
    hi = p["max_gewicht_g"]
    if p["sendungsart"] == "BRIEF":
        if p["format"] == "Standardbrief":
            g = random.randint(5, 100) if random.random() < 0.2 else random.randint(5, 40)
            l, b, h = 22.9, 11.4, random.choice([0.2, 0.3, 0.5, 0.8, 1.2])
        elif p["format"] == "Grossbrief" or p["format"] == "bis 250 g":
            g = random.randint(max(lo, 30), hi)
            l, b, h = 32.4, 22.9, random.choice([0.5, 1.0, 1.5, 2.0])
        elif p["format"] == "Maxibrief" or p["format"] == "bis 2 kg":
            g = random.randint(max(lo, 150), hi)
            l, b, h = 35.3, 25.0, random.choice([2.5, 3.0, 4.0, 5.0])
        else:  # bis 20 g
            g = random.randint(3, 20)
            l, b, h = 22.0, 11.0, 0.3
    else:
        g = int(random.triangular(lo, hi, lo + (hi - lo) * 0.3))
        g = max(lo, g)
        if g <= 2000:
            l, b, h = random.choice([(30, 20, 10), (35, 25, 8), (25, 17, 12), (40, 30, 10)])
        elif g <= 10000:
            l, b, h = random.choice([(40, 30, 20), (50, 35, 25), (60, 40, 30)])
        else:
            l, b, h = random.choice([(60, 40, 40), (80, 50, 40), (100, 60, 50)])
        if sperrgut:
            l, b, h = random.choice([(150, 40, 30), (120, 80, 60), (200, 30, 30)])
    return g, float(l), float(b), float(h)


def zeitpunkt(d: date, kanal: str) -> datetime:
    if kanal in ("FILIALE", "PARTNERFILIALE"):
        hi = 12 if d.weekday() == 5 else 18
        t = time(random.randint(8, hi - 1), random.randint(0, 59), random.randint(0, 59))
    elif kanal == "PICKATHOME":
        t = time(random.randint(8, 16), random.randint(0, 59), random.randint(0, 59))
    else:
        h = min(23, max(0, int(random.gauss(14, 4.5))))
        t = time(h, random.randint(0, 59), random.randint(0, 59))
    return datetime.combine(d, t)


def standort_fuer(kanal, kanton):
    pool = {"FILIALE": FILIALEN_BY_KANTON, "PARTNERFILIALE": PARTNER_BY_KANTON, "MYPOST24": AUTOMATEN_BY_KANTON}.get(kanal)
    if pool is None:
        return None
    if pool.get(kanton):
        return random.choice(pool[kanton])
    return random.choice([s for v in pool.values() for s in v])


def zahlungsart(kanal, geschaeft):
    if geschaeft and kanal in ("BUSINESS_PORTAL", "PICKATHOME", "WEBSTAMP"):
        return "RECHNUNG"
    if kanal in ("FILIALE", "PARTNERFILIALE"):
        return wchoice({"BAR": 30, "KARTE": 35, "TWINT": 25, "POSTFINANCE": 10})
    if kanal == "MYPOST24":
        return wchoice({"KARTE": 60, "TWINT": 40})
    return wchoice({"KARTE": 45, "TWINT": 40, "POSTFINANCE": 15})


def ts_add(t: datetime, d: date, h_lo, h_hi) -> datetime:
    """Zeitpunkt am Tag d (nicht früher als t)."""
    cand = datetime.combine(d, time(random.randint(h_lo, h_hi), random.randint(0, 59)))
    return max(cand, t + timedelta(minutes=random.randint(20, 90)))


def simuliere_events(s, p, auftrag, kanal, abs_kanton):
    """Erzeugt die Tracking-Events einer Sendung und setzt Zustelldatum/Status."""
    evs = []
    t = auftrag["auftrag_zeitpunkt"]
    emp_adr = ADR[EMPF[s["empfaenger_id"]]["adresse_id"]]
    ziel_kanton = emp_adr["kanton"]
    brief = p["sendungsart"] == "BRIEF"
    ab_region = REGION.get(abs_kanton, "MITTE")

    if KANAELE[KANAL_ID[kanal] - 1][3] == "DIGITAL":
        evs.append((t, "ERFASST", "Sendung elektronisch angemeldet", None))
        aufgabe_tag = t.date() if t.hour < 16 and is_werktag(t.date()) else next_werktag(t.date() + timedelta(days=1))
        if random.random() < 0.15:
            aufgabe_tag = next_werktag(aufgabe_tag + timedelta(days=1))
        aufgabe_tag = min(aufgabe_tag, date(2026, 11, 9))  # spätestens letzter Werktag vor dem Stichtag
        aufgabe_ort = standort_fuer("FILIALE", abs_kanton) if kanal != "BUSINESS_PORTAL" else (BZ[ab_region] if brief else PZ[ab_region])
        t = ts_add(t, aufgabe_tag, 9, 17)
    else:
        aufgabe_ort = auftrag["standort_id"] or standort_fuer("FILIALE", abs_kanton)
        aufgabe_tag = t.date()
        t = t + timedelta(minutes=random.randint(1, 5))
    evs.append((t, "AUFGABE", "Sendung aufgegeben", aufgabe_ort))
    s["aufgabe_datum"] = aufgabe_tag

    lz = p["laufzeit_soll_werktage"]
    start = aufgabe_tag if is_werktag(aufgabe_tag) else next_werktag(aufgabe_tag)
    soll = add_werktage(start, lz) if lz > 0 else start
    s["soll_zustell_datum"] = soll

    # Pünktlichkeit
    if p["zone"] == "CH":
        p_punkt = {"A-POST": 0.965, "B-POST": 0.975, "PRIORITY": 0.955, "ECONOMY": 0.965, "SAME-DAY": 0.93}[p["service"]]
        if ziel_kanton in ("GR", "VS", "TI", "UR"):
            p_punkt -= 0.02
    else:
        p_punkt = 0.86 if p["service"] == "PRIORITY" else 0.80
    if soll.month == 12 and soll.day <= 24:
        p_punkt -= 0.07  # Weihnachtsspitze
    verspaetung = 0 if random.random() < p_punkt else (random.randint(1, 3) if p["zone"] == "CH" else random.randint(1, 10))
    ist = add_werktage(soll, verspaetung) if verspaetung else soll

    r = random.random()
    schicksal = "RETOURE" if r < (0.004 if brief else 0.015) else "VERLOREN" if r < (0.0045 if brief else 0.0165) else "OK"

    # Sortierung
    sort_tag = aufgabe_tag if lz == 0 else start
    sort_ort = BZ[ab_region] if brief else PZ[ab_region]
    if sort_ort != aufgabe_ort and lz > 0:  # Same Day geht per Kurier direkt zum Empfänger
        t = ts_add(t, sort_tag, 17, 22)
        evs.append((t, "SORTIERUNG", "Ankunft im Brief-/Paketzentrum, Sendung sortiert", sort_ort))
    if p["zone"] == "CH":
        if not brief and ziel_kanton in REG_PZ and REG_PZ[ziel_kanton] != sort_ort and lz > 0 and ist > sort_tag:
            t = ts_add(t, ist, 2, 5)  # frühmorgens am Zustelltag
            evs.append((t, "SORTIERUNG", "Ankunft im regionalen Paketzentrum", REG_PZ[ziel_kanton]))
        zust_ort = ZUSTELLSTELLE.get(ziel_kanton, ZUSTELLSTELLE["ZH"])
    else:
        exp_tag = add_werktage(start, 1) if ist > start else start
        exp_tag = min(exp_tag, ist)
        t = ts_add(t, exp_tag, *((1, 3) if exp_tag == ist else (6, 20)))
        evs.append((t, "EXPORT", "Sendung hat die Schweiz verlassen", AUSTAUSCH))
        imp_tag = max(exp_tag, ist - timedelta(days=random.randint(1, 3)))
        t = ts_add(t, imp_tag, *((3, 5) if imp_tag == ist else (6, 20)))
        evs.append((t, "IMPORT", "Ankunft im Bestimmungsland", AUSLAND_STANDORT[emp_adr["land_code"]]))
        zust_ort = AUSLAND_STANDORT[emp_adr["land_code"]]

    if schicksal == "VERLOREN":
        if ist + timedelta(days=21) < STICHTAG:
            s["status"] = "VERLOREN"
            s["zustell_datum"] = None
            return [e for e in evs if e[0].date() < STICHTAG]
        schicksal = "OK"

    if lz == 0 and ist == t.date():
        ziel_ts = t + timedelta(minutes=random.randint(45, 150))  # Same Day: Kurier fährt direkt los
    else:
        ziel_ts = datetime.combine(ist, time(random.randint(7, 9), random.randint(0, 59)))
    if ziel_ts.date() >= STICHTAG:
        s["status"] = "UNTERWEGS"
        s["zustell_datum"] = None
        return [e for e in evs if e[0] < datetime.combine(STICHTAG, time(0, 0))]

    t = max(ziel_ts, t + timedelta(minutes=30))
    if not brief:
        evs.append((t, "IN_ZUSTELLUNG", "Sendung ist in Zustellung", zust_ort))
    if schicksal == "RETOURE":
        t = t + timedelta(hours=random.randint(2, 6))
        evs.append((t, "ZUSTELLVERSUCH", "Empfänger nicht angetroffen, Abholungseinladung hinterlegt", zust_ort))
        ret_tag = add_werktage(t.date(), 7)
        if add_werktage(ret_tag, 2) >= STICHTAG:
            s["status"] = "UNTERWEGS"
            s["zustell_datum"] = None
            return evs
        t = datetime.combine(ret_tag, time(random.randint(8, 17), random.randint(0, 59)))
        evs.append((t, "RETOURE", "Frist abgelaufen, Sendung wird an Absender zurückgesandt", zust_ort))
        t = datetime.combine(add_werktage(ret_tag, 2), time(random.randint(8, 17), random.randint(0, 59)))
        evs.append((t, "ZURUECK_AN_ABSENDER", "Sendung an Absender zugestellt", ZUSTELLSTELLE.get(abs_kanton, ZUSTELLSTELLE["ZH"])))
        s["status"] = "RETOURNIERT"
        s["zustell_datum"] = None
        return evs
    t = t + timedelta(hours=random.randint(0, 2) if lz == 0 else random.randint(1, 6), minutes=random.randint(0, 59))
    evs.append((t, "ZUGESTELLT", "Sendung zugestellt", zust_ort))
    s["status"] = "ZUGESTELLT"
    s["zustell_datum"] = t.date()
    return evs


auftrag_id = 0
for k in kunden:
    meta = kunde_meta[k["kunde_id"]]
    von = max(START_DATE, k["registriert_am"])
    if von > END_DATE:
        continue
    tage = sorted(sample_tag(von) for _ in range(meta["n_auftraege"]))
    if not meta["geschaeft"]:
        PRIVAT_FAVORITEN[k["kunde_id"]] = [random.choice(EMPF_BY_ZONE["CH"]) for _ in range(random.randint(1, 4))]
    for tag in tage:
        jahr = tag.year
        kanal = kanal_waehlen(meta, jahr)
        if not is_werktag(tag) and kanal in ("PICKATHOME", "BUSINESS_PORTAL") and random.random() < 0.7:
            tag = next_werktag(tag)
            if tag > END_DATE:
                continue
        if tag.weekday() == 6 and kanal in ("FILIALE", "PARTNERFILIALE"):
            kanal = random.choice(["POSTAPP", "WEBSTAMP", "MYPOST24"])
        auftrag_id += 1
        ts = zeitpunkt(tag, kanal)
        a = dict(auftrag_id=auftrag_id, kunde_id=k["kunde_id"], kanal_id=KANAL_ID[kanal],
                 standort_id=standort_fuer(kanal, meta["kanton"]), auftrag_zeitpunkt=ts,
                 zahlungsart=zahlungsart(kanal, meta["geschaeft"]), betrag_total_chf=None)
        if meta["geschaeft"]:
            n_pos = min(20, max(1, int(random.expovariate(1 / (3.0 if meta["profil"] == "ECOMMERCE" else 1.8))) + 1))
        else:
            n_pos = wchoice({1: 70, 2: 18, 3: 7, 4: 3, 5: 2})
        summe = Decimal("0")
        for pos in range(1, n_pos + 1):
            p = produkt_waehlen(meta, kanal, jahr)
            brief = p["sendungsart"] == "BRIEF"
            zl = []
            if brief:
                p_r = 0.25 if meta["profil"] == "BRIEFVERSENDER" else 0.07
                if random.random() < p_r:
                    zl.append(1)
            else:
                if random.random() < 0.10:
                    zl.append(2)
                if random.random() < 0.04:
                    zl.append(3)
                if p["zone"] == "CH" and random.random() < 0.02:
                    zl.append(4)
                if p["zone"] == "CH" and p["service"] == "PRIORITY" and random.random() < 0.02:
                    zl.append(5)
                if p["format"] in ("L", "bis 30 kg") and random.random() < 0.04:
                    zl.append(6)
            if not meta["geschaeft"] and p["zone"] == "CH" and random.random() < 0.6:
                emp = random.choice(PRIVAT_FAVORITEN[k["kunde_id"]])
            else:
                emp = random.choice(EMPF_BY_ZONE[p["zone"]])
            g, l, b, h = gewicht_und_masse(p, 6 in zl)
            listenpreis = p["preis_chf"] + sum(ZUSATZ_PREIS[z] for z in zl)
            preis = chf(listenpreis * (1 - k["rabatt_prozent"] / 100))
            s = dict(auftrag_id=auftrag_id, position_nr=pos, tracking_nr=neue_tracking_nr(p, k["kunde_id"], 1 in zl),
                     produkt_id=p["produkt_id"], empfaenger_id=emp, gewicht_g=g, laenge_cm=l, breite_cm=b, hoehe_cm=h,
                     preis_chf=preis, aufgabe_datum=None, soll_zustell_datum=None, zustell_datum=None, status=None)
            evs = simuliere_events(s, p, a, kanal, meta["kanton"])
            evs = [e for e in evs if e[0] < datetime.combine(STICHTAG, time(0, 0))]
            if s["status"] in ("ZUGESTELLT", "RETOURNIERT") and evs[-1][1] not in ("ZUGESTELLT", "ZURUECK_AN_ABSENDER"):
                s["status"], s["zustell_datum"] = "UNTERWEGS", None

            # --- DQ-Probleme in Sendungen ---
            r = random.random()
            if r < DQ_GEWICHT_ZU_HOCH:
                s["gewicht_g"] = int(p["max_gewicht_g"] * random.uniform(1.05, 1.8))
                dq_log["sendung.gewicht_g > Produkt-Maximalgewicht"] += 1
            elif r < DQ_GEWICHT_ZU_HOCH + DQ_GEWICHT_NULL:
                s["gewicht_g"] = random.choice([None, 0])
                dq_log["sendung.gewicht_g fehlt oder 0"] += 1
            if s["zustell_datum"] and random.random() < DQ_ZUSTELLUNG_VOR_AUFGABE:
                s["zustell_datum"] = s["aufgabe_datum"] - timedelta(days=random.randint(1, 30))
                dq_log["sendung.zustell_datum < aufgabe_datum"] += 1
            if random.random() < DQ_EMPFAENGER_FEHLT:
                s["empfaenger_id"] = len(empfaenger) + random.randint(1, 500)
                dq_log["sendung.empfaenger_id ohne Empfänger (verwaister FK)"] += 1

            sendungen.append(s)
            for z in zl:
                sendung_zusatz.append(dict(auftrag_id=auftrag_id, position_nr=pos, zusatzleistung_id=z))
            for nr, (et, code, text, sid) in enumerate(sorted(evs, key=lambda e: e[0]), start=1):
                events.append(dict(auftrag_id=auftrag_id, position_nr=pos, event_nr=nr, event_zeitpunkt=et,
                                   event_code=code, event_text=text, standort_id=sid))
            summe += preis
        if random.random() < DQ_BETRAG_FALSCH:
            summe = summe + chf(random.choice([-1, 1]) * random.uniform(0.5, 20))
            summe = max(Decimal("0"), summe)
            dq_log["auftrag.betrag_total_chf ≠ Summe der Sendungen"] += 1
        a["betrag_total_chf"] = summe
        auftraege.append(a)

# ---------------------------------------------------------------------------
# SQL schreiben
# ---------------------------------------------------------------------------
HEADER = "-- Databricks notebook source\n"
CELL = "\n-- COMMAND ----------\n\n"


def md_cell(title, lines):
    out = "-- MAGIC %md\n-- MAGIC # " + title + "\n"
    for ln in lines:
        out += "-- MAGIC " + ln + "\n" if ln else "-- MAGIC\n"
    return out


def insert_statements(table, rows, cols):
    stmts = []
    for i in range(0, len(rows), ROWS_PER_INSERT):
        chunk = rows[i:i + ROWS_PER_INSERT]
        values = ",\n".join("  (" + ", ".join(sql_val(r[c]) for c in cols) + ")" for r in chunk)
        stmts.append(f"INSERT INTO {SCHEMA}.{table} ({', '.join(cols)}) VALUES\n{values};\n")
    return stmts


def write_files(basename, title, beschreibung, tables):
    """Schreibt ein oder mehrere (bei Überschreitung von MAX_FILE_BYTES) Dateien."""
    cells = []
    for table, rows, cols in tables:
        cells.append(f"-- Tabelle {table}: {len(rows)} Zeilen\nTRUNCATE TABLE {SCHEMA}.{table};\n")
        cells.extend(insert_statements(table, rows, cols))
    files = []
    current = []
    size = 0
    for c in cells:
        if current and size + len(c.encode()) > MAX_FILE_BYTES:
            files.append(current)
            current, size = [], 0
        current.append(c)
        size += len(c.encode())
    files.append(current)
    written = []
    for n, fcells in enumerate(files, start=1):
        suffix = "" if len(files) == 1 else f"_teil{n}"
        path = OUT_DIR / f"{basename}{suffix}.sql"
        t = title if len(files) == 1 else f"{title} (Teil {n}/{len(files)})"
        content = HEADER + md_cell(t, beschreibung + ["", f"Generiert durch `scripts/generate_data.py` (Seed {SEED}). Nicht von Hand ändern."])
        content += CELL + CELL.join(fcells)
        path.write_text(content, encoding="utf-8", newline="\n")
        written.append(path)
    return written


def cols(rows):
    return list(rows[0].keys())


OUT_DIR.mkdir(parents=True, exist_ok=True)
for old in OUT_DIR.glob("0[2-5]_*.sql"):
    old.unlink()

land_rows = [dict(land_code=c, land_name=n, zone=z) for c, n, z in LAENDER]
kanal_rows = [dict(kanal_id=a, kanal_code=b, kanal_name=c, kanal_typ=d, beschreibung=e) for a, b, c, d, e in KANAELE]
zusatz_rows = [dict(zusatzleistung_id=a, code=b, bezeichnung=c, anwendbar_fuer=d, preis_chf=chf(e)) for a, b, c, d, e in ZUSATZ]

files = []
files += write_files("02_insert_stammdaten", "02 – Stammdaten laden",
                     ["Länder, Verkaufskanäle, Produkte, Zusatzleistungen und Standorte."],
                     [("land", land_rows, cols(land_rows)), ("verkaufskanal", kanal_rows, cols(kanal_rows)),
                      ("produkt", PRODUKTE, cols(PRODUKTE)), ("zusatzleistung", zusatz_rows, cols(zusatz_rows)),
                      ("standort", standorte, cols(standorte))])
files += write_files("03_insert_kunden_empfaenger", "03 – Adressen, Kunden und Empfänger laden",
                     ["Adressen, Kunden (Privat/Geschäft) und Empfänger."],
                     [("adresse", adressen, cols(adressen)), ("kunde", kunden, cols(kunden)),
                      ("empfaenger", empfaenger, cols(empfaenger))])
files += write_files("04_insert_auftraege_sendungen", "04 – Aufträge und Sendungen laden",
                     ["Aufträge, Sendungen (inkl. Trackingnummer und Empfänger) und gebuchte Zusatzleistungen."],
                     [("auftrag", auftraege, cols(auftraege)), ("sendung", sendungen, cols(sendungen)),
                      ("sendung_zusatzleistung", sendung_zusatz, cols(sendung_zusatz))])
files += write_files("05_insert_tracking", "05 – Tracking-Events laden",
                     ["Ereignisse der Sendungsverfolgung."],
                     [("tracking_event", events, cols(events))])

# ---------------------------------------------------------------------------
# Zusammenfassung
# ---------------------------------------------------------------------------
print("Erzeugte Dateien:")
for f in files:
    print(f"  {f.relative_to(OUT_DIR.parent)}  ({f.stat().st_size / 1024:,.0f} KB)")
print("\nZeilen pro Tabelle:")
for name, rows in [("land", land_rows), ("verkaufskanal", kanal_rows), ("produkt", PRODUKTE), ("zusatzleistung", zusatz_rows),
                   ("standort", standorte), ("adresse", adressen), ("kunde", kunden), ("empfaenger", empfaenger),
                   ("auftrag", auftraege), ("sendung", sendungen), ("sendung_zusatzleistung", sendung_zusatz),
                   ("tracking_event", events)]:
    print(f"  {name:<24}{len(rows):>8,}")
print("\nEingebaute Datenqualitätsprobleme:")
for k, v in sorted(dq_log.items()):
    print(f"  {k:<55}{v:>6}")
print("\nStatus Sendungen:", dict(Counter(s["status"] for s in sendungen)))
