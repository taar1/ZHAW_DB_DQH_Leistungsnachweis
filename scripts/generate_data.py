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
from datetime import date, datetime, timedelta
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

# ---------------------------------------------------------------------------
# Kunden und Empfänger (Adresse direkt in der Tabelle)
# ---------------------------------------------------------------------------


def ort_variante(ort):
    """DQ: abweichende Schreibweise eines Ortes."""
    variants = {"Zürich": ["Zuerich", "ZÜRICH", "Zurich"], "Genève": ["Geneve", "Genf", "GENEVE"],
                "Neuchâtel": ["Neuchatel", "Neuenburg"], "Luzern": ["Lucerne", "LUZERN"], "Bern": ["BERN", "Berne"],
                "Basel": ["Bâle", "BASEL"], "St. Gallen": ["St.Gallen", "Sankt Gallen", "St Gallen"]}
    return random.choice(variants.get(ort, [ort.upper()]))


def adresse_ch(dq=False):
    """Liefert (Adressfelder, Kanton, Sprache) für eine Adresse in der Schweiz/Liechtenstein."""
    plz, ort, kt, sprache, _ = random.choices(ORTE_CH + ORTE_LI, weights=[o[4] for o in ORTE_CH + ORTE_LI])[0]
    land = "LI" if kt is None else "CH"
    nr = str(random.randint(1, 120)) + (random.choice(["a", "b", ""]) if random.random() < 0.08 else "")
    strasse = f"{random.choice(STRASSEN[sprache])} {nr}"
    if dq and random.random() < DQ_PLZ_UNGUELTIG:
        plz = random.choice([plz[:3], plz + "0", "CH-" + plz, plz[:2] + "O" + plz[3:]])
        dq_log["kunde.plz ungültig"] += 1
    if dq and random.random() < DQ_ORT_SCHREIBWEISE:
        ort = ort_variante(ort)
        dq_log["kunde.ort abweichende Schreibweise"] += 1
    return dict(strasse=strasse, plz=plz, ort=ort, land_code=land), kt, sprache


def adresse_ausland(land):
    plz, ort = random.choice(AUSLAND_ORTE[land])
    strasse = f"{random.choice(AUSLAND_STRASSEN[land])} {random.randint(1, 250)}"
    return dict(strasse=strasse, plz=plz, ort=ort, land_code=land)


EMAIL_DOMAINS = {"bluewin.ch": 30, "gmail.com": 30, "gmx.ch": 12, "outlook.com": 8, "hispeed.ch": 6, "sunrise.ch": 6, "protonmail.com": 3}


def make_email(local, domain):
    r = random.random()
    if r < DQ_EMAIL_NULL:
        dq_log["kunde.email fehlt"] += 1
        return None
    if r < DQ_EMAIL_NULL + DQ_EMAIL_UNGUELTIG:
        dq_log["kunde.email ungültig"] += 1
        return random.choice([f"{local}{domain}", f"{local}@{domain.split('.')[0]}", f"{local}@@{domain}", f"{local} @{domain}"])
    return f"{local}@{domain}"


def random_date(a: date, b: date) -> date:
    return a + timedelta(days=random.randint(0, (b - a).days))


kunden = []
kunde_meta = {}  # kunde_id -> Zusatzinfos für die Simulation (nicht in der DB)

for i in range(N_PRIVAT + N_GESCHAEFT):
    kid = i + 1
    geschaeft = i >= N_PRIVAT
    adr, kanton, sprache = adresse_ch(dq=True)
    vn = random.choice(VORNAMEN[sprache][random.choice("mf")])
    nn = random.choice(NACHNAMEN[sprache])
    if geschaeft:
        profil = wchoice({"ECOMMERCE": 50, "BRIEFVERSENDER": 30, "GEMISCHT": 20})
        if random.random() < 0.4:
            firma = f"{nn} {random.choice(BRANCHEN)} {random.choice(RECHTSFORM)}".strip()
        else:
            firma = f"{random.choice(FIRMEN_PREFIX)} {random.choice(BRANCHEN)} {random.choice(RECHTSFORM)}".strip()
        domain = "-".join(ascii_slug(w) for w in firma.replace("& Co.", "").replace(" AG", "").replace(" GmbH", "").split()) + ".ch"
        email = make_email(random.choice(["info", "versand", "logistik", "buchhaltung"]), domain)
        reg = random_date(date(2010, 1, 1), date(2026, 3, 31))
    else:
        profil = None
        firma = None
        local = f"{ascii_slug(vn)}.{ascii_slug(nn)}" if random.random() < 0.7 else f"{ascii_slug(vn)[0]}{ascii_slug(nn)}{random.randint(1, 99)}"
        email = make_email(local, wchoice(EMAIL_DOMAINS))
        reg = random_date(date(2012, 1, 1), date(2026, 8, 31))
    kunden.append(dict(kunde_id=kid, kundentyp="GESCHAEFT" if geschaeft else "PRIVAT", vorname=vn, nachname=nn,
                       firmenname=firma, email=email, **adr, registriert_am=reg))
    kunde_meta[kid] = dict(profil=profil, kanton=kanton, geschaeft=geschaeft)

# DQ: Dubletten – gleiche Person nochmals als neuer Kunde erfasst (z.B. Neuregistrierung)
for orig in random.sample([k for k in kunden if k["kundentyp"] == "PRIVAT"], DQ_DUPLIKAT_KUNDEN):
    kid = len(kunden) + 1
    dup = dict(orig)
    dup["kunde_id"] = kid
    dup["email"] = (orig["email"] or "").upper() or None
    dup["registriert_am"] = random_date(date(2024, 1, 1), date(2026, 6, 30))
    kunden.append(dup)
    kunde_meta[kid] = dict(kunde_meta[orig["kunde_id"]])
    dq_log["kunde Dublette"] += 1

for k in kunden:
    meta = kunde_meta[k["kunde_id"]]
    if meta["geschaeft"]:
        aktivitaet = random.paretovariate(1.6)  # wenige sehr grosse Versender
        n_auftraege = int(min(90, 5 + aktivitaet * 7))
        bevorzugt = wchoice({"BUSINESS_PORTAL": 60, "WEBSTAMP": 15, "FILIALE": 12, "PICKATHOME": 13})
    else:
        aktivitaet = random.paretovariate(2.2)
        n_auftraege = int(min(45, random.randint(1, 6) * aktivitaet))
        bevorzugt = wchoice({"FILIALE": 40, "PARTNERFILIALE": 10, "POSTAPP": 20, "WEBSTAMP": 15, "MYPOST24": 12, "PICKATHOME": 3})
    meta.update(n_auftraege=n_auftraege, bevorzugt=bevorzugt)

empfaenger = []
EMPF_BY_ZONE = defaultdict(list)
EMPF_KANTON = {}  # empfaenger_id -> Kanton (nur für die Simulation)
LAND_GEWICHT = {"DE": 30, "AT": 12, "FR": 14, "IT": 12, "GB": 8, "NL": 5, "ES": 5, "BE": 3, "PT": 4, "SE": 3, "PL": 3,
                "US": 35, "CA": 10, "AU": 8, "JP": 7, "CN": 8, "BR": 5, "IN": 7, "SG": 5, "AE": 6, "ZA": 4, "TH": 5}


def neuer_empfaenger(zone):
    eid = len(empfaenger) + 1
    kanton = None
    if zone == "CH":
        adr, kanton, sprache = adresse_ch(dq=False)
        vn, nn = random.choice(VORNAMEN[sprache][random.choice("mf")]), random.choice(NACHNAMEN[sprache])
        firmen_prefix = FIRMEN_PREFIX
    else:
        laender = [c for c, _, z in LAENDER if z == zone]
        land = random.choices(laender, weights=[LAND_GEWICHT[c] for c in laender])[0]
        adr = adresse_ausland(land)
        firsts, lasts = AUSLAND_NAMEN.get(land, GENERISCH_NAMEN)
        vn, nn = random.choice(firsts), random.choice(lasts)
        firmen_prefix = ["Global", "Euro", "Nova", "Prime", "Blue"]
    if random.random() < 0.12:
        name = f"{random.choice(firmen_prefix)} {random.choice(['Trading', 'Handel', 'Services', 'Solutions', 'Shop'])} {random.choice(['AG', 'GmbH', 'Ltd', 'SA', 'Inc'])}"
    else:
        name = f"{vn} {nn}"
    empfaenger.append(dict(empfaenger_id=eid, name=name, **adr))
    EMPF_BY_ZONE[zone].append(eid)
    EMPF_KANTON[eid] = kanton
    return eid


for _ in range(N_EMPF_CH):
    neuer_empfaenger("CH")
for _ in range(N_EMPF_EUROPA):
    neuer_empfaenger("EUROPA")
for _ in range(N_EMPF_WELT):
    neuer_empfaenger("WELT")
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
    """UPU S10 Sendungsnummer inkl. Prüfziffer, z.B. CP12345678 + Prüfziffer + CH."""
    digits = f"{nummer:08d}"
    s = sum(int(c) * w for c, w in zip(digits, [8, 6, 4, 2, 3, 5, 9, 7]))
    check = 11 - (s % 11)
    check = 0 if check == 10 else 5 if check == 11 else check
    return f"{prefix}{digits}{check}CH"


def neue_tracking_nr(produkt, kunde_id):
    global _tracking_seq
    _tracking_seq += 1
    if produkt["zone"] == "CH":
        art = "99" if produkt["sendungsart"] == "PAKET" else "98"
        code = {"PRIORITY": "60", "ECONOMY": "34", "SAME-DAY": "00", "A-POST": "01", "B-POST": "02"}[produkt["service"]]
        return f"{art}.{code}.{(100000 + kunde_id * 13) % 1000000:06d}.{(10000000 + _tracking_seq * 37) % 100000000:08d}"
    if produkt["sendungsart"] == "PAKET":
        prefix = "CP" if produkt["service"] == "PRIORITY" else "CV"
    else:
        prefix = "LA" if produkt["service"] == "PRIORITY" else "LE"
    return s10(prefix, 10000000 + _tracking_seq * 7)


# ---------------------------------------------------------------------------
# Simulation der Aufträge und Sendungen
# ---------------------------------------------------------------------------
auftraege, sendungen = [], []

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


def gewicht(p):
    lo, hi = p["min_gewicht_g"], p["max_gewicht_g"]
    if p["sendungsart"] == "BRIEF":
        if p["format"] == "Standardbrief":
            return random.randint(5, 100) if random.random() < 0.2 else random.randint(5, 40)
        if p["format"] in ("Grossbrief", "bis 250 g"):
            return random.randint(max(lo, 30), hi)
        if p["format"] in ("Maxibrief", "bis 2 kg"):
            return random.randint(max(lo, 150), hi)
        return random.randint(3, 20)  # bis 20 g
    return max(lo, int(random.triangular(lo, hi, lo + (hi - lo) * 0.3)))


def stunde(d: date, kanal: str) -> int:
    """Uhrzeit (Stunde) der Auftragserfassung – bestimmt, ob die Sendung noch am selben Tag aufgegeben wird."""
    if kanal in ("FILIALE", "PARTNERFILIALE"):
        return random.randint(8, (12 if d.weekday() == 5 else 18) - 1)
    if kanal == "PICKATHOME":
        return random.randint(8, 16)
    return min(23, max(0, int(random.gauss(14, 4.5))))


LETZTER_AUFGABETAG = date(2026, 11, 9)  # letzter Werktag vor dem Stichtag
KANAL_TYP = {code: typ for _, code, _, typ, _ in KANAELE}


def simuliere_zustellung(s, p, tag, kanal, ziel_kanton):
    """Setzt Aufgabe-, Soll- und Ist-Zustelldatum sowie den Status einer Sendung."""
    if KANAL_TYP[kanal] == "DIGITAL":
        # Online frankiert: Aufgabe am selben Werktag (vor 16 Uhr) oder später
        aufgabe_tag = tag if stunde(tag, kanal) < 16 and is_werktag(tag) else next_werktag(tag + timedelta(days=1))
        if random.random() < 0.15:
            aufgabe_tag = next_werktag(aufgabe_tag + timedelta(days=1))
        aufgabe_tag = min(aufgabe_tag, LETZTER_AUFGABETAG)
    else:
        aufgabe_tag = tag
    s["aufgabe_datum"] = aufgabe_tag

    lz = p["laufzeit_soll_werktage"]
    start = next_werktag(aufgabe_tag)
    soll = add_werktage(start, lz) if lz > 0 else start
    s["soll_zustell_datum"] = soll

    # Pünktlichkeit
    if p["zone"] == "CH":
        p_punkt = {"A-POST": 0.965, "B-POST": 0.975, "PRIORITY": 0.955, "ECONOMY": 0.965, "SAME-DAY": 0.93}[p["service"]]
        if ziel_kanton in ("GR", "VS", "TI", "UR"):
            p_punkt -= 0.02  # Bergregionen
    else:
        p_punkt = 0.86 if p["service"] == "PRIORITY" else 0.80
    if soll.month == 12 and soll.day <= 24:
        p_punkt -= 0.07  # Weihnachtsspitze
    verspaetung = 0 if random.random() < p_punkt else (random.randint(1, 3) if p["zone"] == "CH" else random.randint(1, 10))
    ist = add_werktage(soll, verspaetung) if verspaetung else soll

    r = random.random()
    brief = p["sendungsart"] == "BRIEF"
    schicksal = "RETOURE" if r < (0.004 if brief else 0.015) else "VERLOREN" if r < (0.0045 if brief else 0.0165) else "OK"

    s["zustell_datum"] = None
    if schicksal == "VERLOREN" and ist + timedelta(days=21) < STICHTAG:
        s["status"] = "VERLOREN"            # nach 3 Wochen ohne Zustellung als verloren gemeldet
    elif ist >= STICHTAG:
        s["status"] = "UNTERWEGS"
    elif schicksal == "RETOURE":
        # 7 Werktage Abholfrist, danach 2 Werktage zurück an den Absender
        s["status"] = "RETOURNIERT" if add_werktage(ist, 9) < STICHTAG else "UNTERWEGS"
    else:
        s["status"] = "ZUGESTELLT"
        s["zustell_datum"] = ist


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
        a = dict(auftrag_id=auftrag_id, kunde_id=k["kunde_id"], kanal_id=KANAL_ID[kanal],
                 auftrag_datum=tag, betrag_total_chf=None)
        if meta["geschaeft"]:
            n_pos = min(20, max(1, int(random.expovariate(1 / (3.0 if meta["profil"] == "ECOMMERCE" else 1.8))) + 1))
        else:
            n_pos = wchoice({1: 70, 2: 18, 3: 7, 4: 3, 5: 2})
        summe = Decimal("0")
        for pos in range(1, n_pos + 1):
            p = produkt_waehlen(meta, kanal, jahr)
            if not meta["geschaeft"] and p["zone"] == "CH" and random.random() < 0.6:
                emp = random.choice(PRIVAT_FAVORITEN[k["kunde_id"]])
            else:
                emp = random.choice(EMPF_BY_ZONE[p["zone"]])
            preis = p["preis_chf"]
            if meta["geschaeft"] and p["sendungsart"] == "PAKET":
                preis = chf(preis * Decimal("0.9"))  # Mengenrabatt Geschäftskunden auf Pakete
            s = dict(auftrag_id=auftrag_id, position_nr=pos, tracking_nr=neue_tracking_nr(p, k["kunde_id"]),
                     produkt_id=p["produkt_id"], empfaenger_id=emp, gewicht_g=gewicht(p), preis_chf=preis,
                     aufgabe_datum=None, soll_zustell_datum=None, zustell_datum=None, status=None)
            simuliere_zustellung(s, p, tag, kanal, EMPF_KANTON[emp])

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


def write_file(basename, title, beschreibung, tables):
    cells = []
    for table, rows in tables:
        cols = list(rows[0].keys())
        cells.append(f"-- Tabelle {table}: {len(rows)} Zeilen\nTRUNCATE TABLE {SCHEMA}.{table};\n")
        cells.extend(insert_statements(table, rows, cols))
    path = OUT_DIR / f"{basename}.sql"
    content = HEADER + md_cell(title, beschreibung + ["", f"Generiert durch `scripts/generate_data.py` (Seed {SEED}). Nicht von Hand ändern."])
    content += CELL + CELL.join(cells)
    path.write_text(content, encoding="utf-8", newline="\n")
    return path


OUT_DIR.mkdir(parents=True, exist_ok=True)
for old in OUT_DIR.glob("0[2-5]_insert_*.sql"):
    old.unlink()

land_rows = [dict(land_code=c, land_name=n, zone=z) for c, n, z in LAENDER]
kanal_rows = [dict(kanal_id=a, kanal_name=c, kanal_typ=d) for a, b, c, d, e in KANAELE]
produkt_rows = [{c: p[c] for c in ("produkt_id", "produkt_name", "sendungsart", "zone", "service",
                                   "max_gewicht_g", "laufzeit_soll_werktage", "preis_chf")} for p in PRODUKTE]

T = {"land": land_rows, "verkaufskanal": kanal_rows, "produkt": produkt_rows, "kunde": kunden,
     "empfaenger": empfaenger, "auftrag": auftraege, "sendung": sendungen}

files = [
    write_file("02_insert_stammdaten", "02 – Stammdaten laden", ["Länder, Verkaufskanäle und Produkte."],
               [(t, T[t]) for t in ("land", "verkaufskanal", "produkt")]),
    write_file("03_insert_kunden_empfaenger", "03 – Kunden und Empfänger laden", ["Kunden (Privat/Geschäft) und Empfänger."],
               [(t, T[t]) for t in ("kunde", "empfaenger")]),
    write_file("04_insert_auftraege_sendungen", "04 – Aufträge und Sendungen laden",
               ["Aufträge und Sendungen (inkl. Trackingnummer, Empfänger und Zustelldaten)."],
               [(t, T[t]) for t in ("auftrag", "sendung")]),
]

# ---------------------------------------------------------------------------
# Zusammenfassung
# ---------------------------------------------------------------------------
print("Erzeugte Dateien:")
for f in files:
    print(f"  {f.relative_to(OUT_DIR.parent)}  ({f.stat().st_size / 1024:,.0f} KB)")
print("\nZeilen pro Tabelle:")
for name, rows in T.items():
    print(f"  {name:<24}{len(rows):>8,}")
print("\nEingebaute Datenqualitätsprobleme:")
for k, v in sorted(dq_log.items()):
    print(f"  {k:<55}{v:>6}")
print("\nStatus Sendungen:", dict(Counter(s["status"] for s in sendungen)))
