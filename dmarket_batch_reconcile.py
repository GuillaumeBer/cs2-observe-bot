import asyncio
import sqlite3
import os
import json
import time
import urllib.parse
import yarl
import aiohttp
import logging
from datetime import datetime, timezone
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from dmarket_ingestion import generate_dmarket_headers
from transaction_db import TransactionDatabase

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("dmarket_reconcile.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("dmarket_reconcile")

# Initialisation de la BDD
db_path = os.getenv("OBSERVER_DB_PATH", os.path.join(config.BASE_DIR, "data", "observer_dataset.db"))
db = TransactionDatabase(db_path)


def ensure_index_listed_at():
    """Crée l'index listed_at si inexistant (idempotent, rapide)."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_obs_listed_at ON observed_listings (listed_at);"
        )
        conn.commit()
        logger.info("Index idx_obs_listed_at vérifié/créé.")
    finally:
        conn.close()

# Epsilon pour la comparaison des float (flottants SQLite et API DMarket)
FLOAT_EPSILON = 1e-6

async def fetch_last_sales(session: aiohttp.ClientSession, market_hash_name: str) -> list:
    """Interroge l'endpoint last-sales pour un skin donné."""
    encoded_title = urllib.parse.quote(market_hash_name)
    url_str = f"https://api.dmarket.com/trade-aggregator/v1/last-sales?title={encoded_title}&gameId=a8db&limit=50"
    url = yarl.URL(url_str)
    
    path = url.raw_path
    if url.raw_query_string:
        path += f"?{url.raw_query_string}"

    headers = generate_dmarket_headers(
        config.DMARKET_PUBLIC_KEY,
        config.DMARKET_SECRET_KEY,
        "GET",
        path
    )

    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("sales") or []
            elif response.status == 429:
                logger.warning(f"Rate limited (429) for {market_hash_name}. Pause 5s...")
                await asyncio.sleep(5.0)
                return []
            else:
                logger.warning(f"HTTP {response.status} for {market_hash_name}: {await response.text()}")
                return []
    except Exception as e:
        logger.error(f"Error fetching sales for {market_hash_name}: {e}")
        return []

def parse_sale_data(sale: dict) -> dict:
    """Extrait proprement le prix, float, seed et date d'un objet sale de DMarket."""
    # Extraction du prix (USD)
    sale_price = 0.0
    raw_price = sale.get("price")
    if isinstance(raw_price, str):
        try:
            sale_price = float(raw_price)
        except ValueError:
            return None
    elif isinstance(raw_price, dict):
        amt = raw_price.get("amount") or raw_price.get("USD") or 0.0
        try:
            sale_price = float(amt) if "." in str(amt) else float(amt) / 100.0
        except ValueError:
            return None
    else:
        return None

    # Extraction du timestamp de vente
    raw_date = sale.get("date")
    try:
        sale_date = float(raw_date)
    except (ValueError, TypeError):
        return None

    # Extraire float et seed
    offer_attrs = sale.get("offerAttributes") or {}
    float_val = offer_attrs.get("floatValue")
    paint_seed = offer_attrs.get("paintSeed")

    if float_val is None:
        return None

    try:
        float_val = float(float_val)
    except ValueError:
        return None

    return {
        "price_usd": sale_price,
        "sale_date": sale_date,  # Unix timestamp
        "float_value": float_val,
        "paint_seed": int(paint_seed) if paint_seed is not None else None
    }

async def reconcile_skin(session: aiohttp.ClientSession, market_hash_name: str, active_listings: list) -> int:
    """Récupère les ventes pour un skin, cherche les correspondances et met à jour la base."""
    sales = await fetch_last_sales(session, market_hash_name)
    if not sales:
        return 0

    # Filtrer les listings actifs correspondants à ce skin
    listings = [l for l in active_listings if l["market_hash_name"] == market_hash_name]
    if not listings:
        return 0

    matches_found = 0

    for sale_raw in sales:
        sale = parse_sale_data(sale_raw)
        if not sale:
            continue

        # Essayer de trouver un listing actif correspondant par float (avec epsilon)
        matched_listing = None
        for lst in listings:
            if lst["float_value"] is not None:
                if abs(lst["float_value"] - sale["float_value"]) < FLOAT_EPSILON:
                    matched_listing = lst
                    break

        if matched_listing:
            # Calcul du TTD en millisecondes
            # DMarket sale_date est un timestamp Unix (secondes), listed_at est en secondes
            listed_at = matched_listing["listed_at"]
            if not listed_at:
                # Fallback sur le timestamp string au format ISO si listed_at n'est pas dispo
                try:
                    dt = datetime.fromisoformat(matched_listing["timestamp"].replace("Z", "+00:00"))
                    listed_at = dt.timestamp()
                except Exception:
                    continue

            ttd_seconds = sale["sale_date"] - listed_at
            
            # Il est possible qu'il y ait des imprécisions d'horloges, mais le TTD doit rester positif.
            # On tolère un léger décalage (ex: -5s) et on le remet à 0.
            if ttd_seconds < 0:
                if ttd_seconds > -10:
                    ttd_seconds = 0
                else:
                    # Si le décalage est trop grand dans le négatif, ce n'est probablement pas le même listing (réutilisation de float)
                    continue

            ttd_ms = ttd_seconds * 1000.0

            # Convertir timestamp Unix en format ISO string pour la table transactions
            sale_dt = datetime.fromtimestamp(sale["sale_date"], timezone.utc)
            sale_timestamp_str = sale_dt.isoformat().replace("+00:00", "Z")

            # Catégorisation par vitesse de vente (cohérente avec observer._process_disappearance)
            if ttd_ms < config.OBS_BOT_SNIPE_TTD_MS:
                category = "BOT_SNIPE"
            elif ttd_ms < config.OBS_FAST_HUMAN_TTD_MS:
                category = "FAST_HUMAN"
            else:
                category = "NORMAL_SALE"

            # Enregistrer la transaction
            saved = db.save_transaction(
                market_hash_name=market_hash_name,
                price_usd=sale["price_usd"],
                ttd_ms=ttd_ms,
                platform="dmarket",
                category=category,
                float_value=sale["float_value"],
                paint_seed=sale["paint_seed"],
                timestamp=sale_timestamp_str,
                confidence="HIGH"  # Méthode exacte et réconciliée
            )

            if saved:
                logger.info(
                    f"Match réconcilié : {market_hash_name} (float: {sale['float_value']}) | "
                    f"Prix : ${sale['price_usd']:.2f} | TTD : {ttd_seconds:.1f}s"
                )
                matches_found += 1
                # Supprimer le listing de la base pour éviter les doublons
                db.delete_observed_listings([matched_listing["listing_id"]])
                # Retirer le listing de notre liste locale pour ne pas matcher une autre vente dessus
                listings.remove(matched_listing)

    return matches_found

async def main():
    logger.info("Démarrage de la réconciliation par lots (batch)...")
    
    # 0. Créer/vérifier l'index listed_at pour les performances
    ensure_index_listed_at()

    # 0b. Supprimer les listings de plus de 100 jours (fenêtre glissante)
    max_ttd_sec = float(os.getenv("OBSERVER_MAX_TTD_SEC", str(100 * 24 * 3600)))
    deleted = db.clean_old_observed_listings(max_ttd_sec)
    logger.info(f"Fenêtre glissante 100j : {deleted} listings expirés supprimés.")

    # 1. Charger la liste des skins cibles
    target_skins_path = os.path.join(config.BASE_DIR, "data", "target_skins.json")
    if not os.path.exists(target_skins_path):
        logger.error(f"Fichier cible {target_skins_path} manquant.")
        return
        
    with open(target_skins_path, "r", encoding="utf-8") as f:
        target_skins = json.load(f)
        
    logger.info(f"Chargement de {len(target_skins)} skins cibles à analyser.")

    # 2. Récupérer tous les listings observés actifs pour DMarket
    # get_pending_observed_listings retourne tous les listings pour une plateforme
    active_listings = db.get_pending_observed_listings("dmarket")
    logger.info(f"{len(active_listings)} listings observés actifs en base.")

    if not active_listings:
        logger.info("Aucun listing actif en base, réconciliation annulée.")
        return

    # 3. Lancer la boucle de requête avec rate limit strict (5 requêtes / seconde)
    async with aiohttp.ClientSession() as session:
        total_matches = 0
        start_time = time.time()
        
        for i, skin in enumerate(target_skins):
            loop_start = time.time()
            
            matches = await reconcile_skin(session, skin, active_listings)
            total_matches += matches
            
            # Rate limit : max 5 requêtes par seconde (intervalle de 0.2s minimum)
            elapsed = time.time() - loop_start
            sleep_time = max(0.2 - elapsed, 0.0)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

            if (i + 1) % 50 == 0:
                logger.info(f"Progression : {i + 1}/{len(target_skins)} skins traités...")

        total_elapsed = time.time() - start_time
        logger.info(
            f"Réconciliation terminée en {total_elapsed:.1f}s. "
            f"Total de correspondances trouvées et enregistrées : {total_matches}"
        )

if __name__ == "__main__":
    asyncio.run(main())
