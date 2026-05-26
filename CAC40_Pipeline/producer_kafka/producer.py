# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────
import json
import time
import yfinance as yf
from kafka import KafkaProducer
from curl_cffi import requests as curl_requests

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

# symboles CAC40 sur Euronext Paris (suffixe .PA)
SYMBOLES = ["MC.PA", "BNP.PA", "TTE.PA", "SAN.PA", "AIR.PA"]

# port 29092 = port externe Docker, accessible depuis le host Windows
KAFKA_BOOTSTRAP = "localhost:29092"

# topic Kafka créé dans Kafdrop
TOPIC = "cac40_quotes"

# délai entre chaque symbole
DELAI_INTER_SYMBOLE = 5

# délai entre chaque cycle complet
DELAI_SECONDES = 30

# ─────────────────────────────────────────────
# SESSION CURL_CFFI : imite Chrome pour contourner l'anti-scraping Yahoo
# ─────────────────────────────────────────────
session = curl_requests.Session(impersonate="chrome")

# ─────────────────────────────────────────────
# INITIALISATION DU PRODUCER KAFKA
# ─────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ─────────────────────────────────────────────
# FONCTION : récupérer le cours d'un symbole
# ─────────────────────────────────────────────
def fetch_quote(symbole):
    """
    Récupère le cours via yfinance + session curl_cffi (anti-blocage).
    """
    try:
        # passage de la session Chrome au Ticker → évite le rate limiting
        ticker = yf.Ticker(symbole, session=session)
        hist = ticker.history(period="1d", interval="1m")

        if hist.empty:
            print(f"[SKIP] {symbole} — historique vide (marché fermé ?)")
            return None

        derniere = hist.iloc[-1]

        data = {
            "symbole":            symbole,
            "prix_actuel":        round(float(derniere["Close"]), 2),
            "prix_ouverture":     round(float(hist.iloc[0]["Open"]), 2),
            "prix_haut":          round(float(hist["High"].max()), 2),
            "prix_bas":           round(float(hist["Low"].min()), 2),
            "cloture_precedente": round(float(derniere["Close"]), 2),
            "volume":             int(derniere["Volume"]),
            "fetch_timestamp":    int(time.time()),
        }
        return data

    except Exception as e:
        print(f"[ERREUR] Symbole {symbole} : {e}")
        return None

# ─────────────────────────────────────────────
# BOUCLE PRINCIPALE
# ─────────────────────────────────────────────
print(f"[INFO] Producer démarré — topic : {TOPIC}")
print(f"[INFO] Symboles : {SYMBOLES}")
print(f"[INFO] Session : curl_cffi Chrome (anti-blocage)\n")

while True:
    for symbole in SYMBOLES:
        quote = fetch_quote(symbole)

        if quote:
            producer.send(TOPIC, value=quote)
            producer.flush()
            print(f"[OK] {quote['symbole']} | prix={quote['prix_actuel']}€ | ts={quote['fetch_timestamp']}")
        else:
            print(f"[SKIP] {symbole} — pas de données")

        time.sleep(DELAI_INTER_SYMBOLE)

    print(f"--- cycle terminé, pause {DELAI_SECONDES}s ---\n")
    time.sleep(DELAI_SECONDES)