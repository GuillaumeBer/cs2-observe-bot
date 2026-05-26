"""
aggregate_non_target_stats.py
-----------------------------
Script d'agrégation hebdomadaire des skins hors-cible.

Exécuté chaque dimanche à 02h00 UTC via cs2-aggregate.timer.

Logique :
  1. Charger les 800 skins cibles (target_skins.json)
  2. Agréger les observed_listings des skins NON cibles par skin :
       listing_count, avg/min/max price, avg/min/max float
  3. Upsert dans skin_market_stats avec le label ISO de la semaine courante
  4. Supprimer de observed_listings les lignes des skins hors-cible
     (on ne garde que les 800 cibles pour la réconciliation TTD)
  5. Agréger aussi les skins cibles pour comparaison (is_target=1)
"""

import os
import sys
import json
import logging
import sqlite3
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from transaction_db import TransactionDatabase

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("aggregate_stats.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("aggregate_non_target")

# ─── Config ───────────────────────────────────────────────────────────────────
DB_PATH = os.getenv(
    "OBSERVER_DB_PATH",
    os.path.join(config.BASE_DIR, "data", "observer_dataset.db"),
)
TARGET_SKINS_PATH = os.path.join(config.BASE_DIR, "data", "target_skins.json")
PLATFORM = "dmarket"


def iso_week_label(dt: datetime) -> str:
    """Retourne le label ISO semaine, ex: '2026-W22'."""
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def load_target_skins(path: str) -> set:
    if not os.path.exists(path):
        logger.error(f"Fichier target_skins.json introuvable : {path}")
        return set()
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return set(data)


def aggregate_and_save(db: TransactionDatabase, week_label: str, target_skins: set) -> dict:
    """
    Agrège tous les observed_listings par skin pour la semaine courante,
    upsert dans skin_market_stats, puis supprime les lignes hors-cible.
    Retourne un résumé {non_target_skins, target_skins, total_listings_deleted}.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        logger.info("Lecture des observed_listings pour agrégation...")
        rows = conn.execute(
            "SELECT market_hash_name, price_cents, float_value FROM observed_listings WHERE platform = ?",
            (PLATFORM,),
        ).fetchall()
        logger.info(f"{len(rows)} listings lus depuis observed_listings.")
    finally:
        conn.close()

    # ── Agrégation en mémoire ───────────────────────────────────────────────
    from collections import defaultdict
    skin_data: dict = defaultdict(lambda: {
        "prices": [], "floats": [], "listing_ids": []
    })

    for row in rows:
        name = row["market_hash_name"]
        skin_data[name]["prices"].append(row["price_cents"])
        if row["float_value"] and row["float_value"] > 0:
            skin_data[name]["floats"].append(row["float_value"])

    # ── Upsert dans skin_market_stats ───────────────────────────────────────
    non_target_count = 0
    target_count = 0

    for name, data in skin_data.items():
        prices = data["prices"]
        floats = data["floats"]
        is_target = name in target_skins

        avg_p = sum(prices) / len(prices) if prices else None
        min_p = min(prices) if prices else None
        max_p = max(prices) if prices else None
        avg_f = sum(floats) / len(floats) if floats else None
        min_f = min(floats) if floats else None
        max_f = max(floats) if floats else None

        db.upsert_skin_market_stats(
            week_label=week_label,
            market_hash_name=name,
            platform=PLATFORM,
            listing_count=len(prices),
            avg_price_cents=round(avg_p, 2) if avg_p else None,
            min_price_cents=min_p,
            max_price_cents=max_p,
            avg_float=round(avg_f, 6) if avg_f else None,
            min_float=round(min_f, 6) if min_f else None,
            max_float=round(max_f, 6) if max_f else None,
            is_target=is_target,
        )

        if is_target:
            target_count += 1
        else:
            non_target_count += 1

    logger.info(
        f"Upsert terminé : {target_count} skins cibles + {non_target_count} skins hors-cible → skin_market_stats."
    )

    # ── Suppression des listings hors-cible de observed_listings ────────────
    logger.info("Suppression des listings hors-cible de observed_listings...")
    conn = sqlite3.connect(DB_PATH)
    try:
        non_target_names = [n for n in skin_data if n not in target_skins]
        if non_target_names:
            placeholders = ",".join(["?"] * len(non_target_names))
            cursor = conn.execute(
                f"DELETE FROM observed_listings WHERE market_hash_name IN ({placeholders}) AND platform = ?",
                non_target_names + [PLATFORM],
            )
            conn.commit()
            deleted = cursor.rowcount
            logger.info(f"{deleted} listings hors-cible supprimés de observed_listings.")
        else:
            deleted = 0
            logger.info("Aucun listing hors-cible à supprimer.")
    finally:
        conn.close()

    return {
        "week_label": week_label,
        "non_target_skins": non_target_count,
        "target_skins_aggregated": target_count,
        "listings_deleted_from_observed": deleted,
    }


def main():
    now = datetime.now(timezone.utc)
    week_label = iso_week_label(now)
    logger.info(f"=== Agrégation hebdomadaire — {week_label} ===")

    # Charger les 800 skins cibles
    target_skins = load_target_skins(TARGET_SKINS_PATH)
    if not target_skins:
        logger.error("Liste des skins cibles vide ou introuvable. Abandon.")
        return

    logger.info(f"{len(target_skins)} skins cibles chargés depuis target_skins.json.")

    # Initialiser la DB (crée la table si absente)
    db = TransactionDatabase(DB_PATH)

    # Agréger et nettoyer
    summary = aggregate_and_save(db, week_label, target_skins)

    logger.info(
        f"=== Résumé ===\n"
        f"  Semaine          : {summary['week_label']}\n"
        f"  Skins hors-cible : {summary['non_target_skins']}\n"
        f"  Skins cibles     : {summary['target_skins_aggregated']}\n"
        f"  Listings purgés  : {summary['listings_deleted_from_observed']}\n"
    )


if __name__ == "__main__":
    main()
