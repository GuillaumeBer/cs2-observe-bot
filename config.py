import os
from pathlib import Path
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Chemins de fichiers
BASE_DIR = Path(__file__).resolve().parent
OBSERVATION_HOT_ITEMS_PATH = BASE_DIR / "data" / "observed_hot_items.json"
OBSERVATION_REPORT_PATH = BASE_DIR / "data" / "observation_report.json"
OBSERVATION_SNIPE_LOG_PATH = BASE_DIR / "data" / "snipe_log.jsonl"

# Seuils de prix (USD)
MIN_PRICE_USD = float(os.getenv("MIN_PRICE_USD", "10.0"))
MAX_PRICE_USD = float(os.getenv("MAX_PRICE_USD", "150.0"))

# Configuration DMarket
DMARKET_PUBLIC_KEY = os.getenv("DMARKET_PUBLIC_KEY", "")
DMARKET_SECRET_KEY = os.getenv("DMARKET_SECRET_KEY", "")

# Configuration Market.CSGO (DÉSACTIVÉ — clé API bloquée définitivement par Market.CSGO)
MARKET_CSGO_API_KEY = os.getenv("MARKET_CSGO_API_KEY", "")
MARKET_CSGO_ENABLED = False

# Configuration Skinport (WebSocket public, pas de clé API requise pour l'observation)
# Mettre True pour activer — aucune clé nécessaire
SKINPORT_ENABLED = True

# Configuration Waxpeer
WAXPEER_API_KEY = os.getenv("WAXPEER_API_KEY", "")

# Configuration CSFloat
CSFLOAT_API_KEY = os.getenv("CSFLOAT_API_KEY", "")

# Configuration du mode d'observation
OBS_BOT_SNIPE_TTD_MS = int(os.getenv("OBS_BOT_SNIPE_TTD_MS", "5000"))       # TTD < 5s -> snipe bot
OBS_FAST_HUMAN_TTD_MS = int(os.getenv("OBS_FAST_HUMAN_TTD_MS", "60000"))    # TTD < 60s -> achat humain rapide
OBS_EXPORT_INTERVAL_SEC = int(os.getenv("OBS_EXPORT_INTERVAL_SEC", "300"))   # Export JSON toutes les 5 min
OBS_MIN_SNIPE_COUNT = int(os.getenv("OBS_MIN_SNIPE_COUNT", "3"))             # Nb min de snipes pour qualifier un item
OBS_POLL_INTERVAL_MS = int(os.getenv("OBS_POLL_INTERVAL_MS", "1500"))        # Intervalle de polling (ms)
OBS_CONFIRMATION_CYCLES = int(os.getenv("OBS_CONFIRMATION_CYCLES", "2"))     # Cycles consécutifs d'absence avant de confirmer une disparition

# Configuration de la base de données SQLite de l'Observer
OBSERVER_DB_PATH = os.getenv("OBSERVER_DB_PATH", str(BASE_DIR / "data" / "observer_dataset.db"))
OBSERVER_MAX_TTD_SEC = int(os.getenv("OBSERVER_MAX_TTD_SEC", "86400"))       # Temps de rétention des listings (24h par défaut)
