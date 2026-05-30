import sqlite3
import logging
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import config

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cs2_observer_api")

app = FastAPI(
    title="CS2 Observer API",
    description="API REST pour interroger le dataset du bot d'observation CS2",
    version="1.0.0"
)

# Configuration CORS pour permettre au dashboard local d'interroger l'API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection() -> sqlite3.Connection:
    """Crée une connexion à la base de données SQLite."""
    try:
        conn = sqlite3.connect(config.OBSERVER_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à la base de données : {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

import subprocess
import os
import re
import json
from datetime import datetime, timezone

# Chargement de la liste des 800 skins cibles au démarrage
TARGET_SKINS: set = set()

def _load_target_skins() -> None:
    global TARGET_SKINS
    path = os.path.join(os.path.dirname(config.OBSERVER_DB_PATH), "target_skins.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            TARGET_SKINS = set(json.load(f))
        logger.info(f"target_skins.json chargé : {len(TARGET_SKINS)} skins cibles.")
    except Exception as e:
        logger.warning(f"Impossible de charger target_skins.json : {e}")

_load_target_skins()

@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "CS2 Observer API is running",
        "database_configured": config.OBSERVER_DB_PATH
    }

@app.get("/api/bot-status")
def get_bot_status():
    """Vérifie le statut de l'observer bot (service systemd et activité log)."""
    # 1. Vérifier si le service systemd est actif
    service_active = False
    try:
        res = subprocess.run(["systemctl", "is-active", "cs2-observer"], capture_output=True, text=True)
        service_active = (res.stdout.strip() == "active")
    except Exception as e:
        logger.error(f"Erreur systemctl is-active : {e}")
        # Fallback local
        service_active = os.path.exists("cs2_observer.log")

    # 2. Lire le timestamp de la dernière ligne du log
    last_activity = None
    last_log_line = ""
    log_path = "/home/ubuntu/cs2-observation-bot/cs2_observer.log"
    if not os.path.exists(log_path):
        log_path = "cs2_observer.log"
        
    if os.path.exists(log_path):
        try:
            with open(log_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                seek_pos = max(0, size - 2000)
                f.seek(seek_pos)
                content = f.read().decode("utf-8", errors="ignore")
                lines = content.splitlines()
                if lines:
                    for line in reversed(lines):
                        match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                        if match:
                            last_activity = match.group(1)
                            last_log_line = line
                            break
        except Exception as e:
            logger.error(f"Erreur lecture logs pour statut : {e}")

    # 3. Vérifier la date de dernière écriture en BDD
    last_db_update = None
    try:
        if os.path.exists(config.OBSERVER_DB_PATH):
            mtime = os.path.getmtime(config.OBSERVER_DB_PATH)
            last_db_update = datetime.fromtimestamp(mtime, timezone.utc).isoformat()
    except Exception as e:
        logger.error(f"Erreur mtime DB : {e}")

    # Déterminer le statut de santé
    status = "running" if service_active else "stopped"
    if service_active and last_activity:
        try:
            log_dt = datetime.strptime(last_activity, "%Y-%m-%d %H:%M:%S")
            now_ts = datetime.now()
            diff_sec = (now_ts - log_dt).total_seconds()
            # Si le bot tourne mais plus d'activité log depuis 300 secondes, suspicion de blocage
            if diff_sec > 300:
                status = "warning"
        except Exception:
            pass

    return {
        "status": status,
        "service_active": service_active,
        "last_log_time": last_activity,
        "last_log_line": last_log_line[:120] if last_log_line else "",
        "last_db_update": last_db_update
    }

@app.get("/api/stats")
def get_global_stats(
    platform: str = Query(default=None),
    speed: str = Query(default=None),
    confidence: str = Query(default=None),
):
    """Calcule et retourne les statistiques globales d'observation, avec filtres optionnels."""
    conn = get_db_connection()
    try:
        def build_where(extra_speed_clause: str = "") -> tuple[str, list]:
            clauses = ["1=1"]
            params = []
            if platform:
                clauses.append("platform = ?")
                params.append(platform)
            if confidence and confidence.upper() in {"HIGH", "MEDIUM", "LOW"}:
                clauses.append("confidence = ?")
                params.append(confidence.upper())
            if extra_speed_clause:
                clauses.append(extra_speed_clause)
            return "WHERE " + " AND ".join(clauses), params

        # Apply speed filter globally when set
        global_speed = ""
        if speed == "bot":
            global_speed = "ttd_ms < 5000"
        elif speed == "quick":
            global_speed = "ttd_ms >= 5000 AND ttd_ms < 60000"
        elif speed == "normal":
            global_speed = "ttd_ms >= 60000"

        where_base, params_base = build_where(global_speed)

        total_tx = conn.execute(f"SELECT COUNT(*) FROM transactions {where_base};", params_base).fetchone()[0]
        avg_ttd_row = conn.execute(f"SELECT AVG(ttd_ms) FROM transactions {where_base};", params_base).fetchone()[0]
        avg_ttd = round(avg_ttd_row / 1000.0, 2) if avg_ttd_row else 0.0

        # Speed breakdown — only relevant when no global speed filter
        if not speed:
            where_bot, p_bot = build_where("ttd_ms < 5000")
            where_quick, p_quick = build_where("ttd_ms >= 5000 AND ttd_ms < 60000")
            where_normal, p_normal = build_where("ttd_ms >= 60000")
        else:
            where_bot, p_bot = where_base, params_base
            where_quick, p_quick = where_base, params_base
            where_normal, p_normal = where_base, params_base

        bot_snipes = conn.execute(f"SELECT COUNT(*) FROM transactions {where_bot};", p_bot).fetchone()[0]
        quick_buys = conn.execute(f"SELECT COUNT(*) FROM transactions {where_quick};", p_quick).fetchone()[0]
        normal_sales = conn.execute(f"SELECT COUNT(*) FROM transactions {where_normal};", p_normal).fetchone()[0]

        active_listings = conn.execute("SELECT COUNT(*) FROM observed_listings;").fetchone()[0]

        # Stats par plateforme
        platform_stats = {}
        for row in conn.execute("""
            SELECT platform,
              COUNT(*) as total_listings,
              COUNT(DISTINCT market_hash_name) as unique_skins
            FROM observed_listings GROUP BY platform
        """).fetchall():
            platform_stats[row[0]] = {"listings_total": row[1], "listings_unique_skins": row[2], "sales_total": 0, "sales_unique_skins": 0, "transactions_high": 0}

        for row in conn.execute("""
            SELECT platform, COUNT(*) as total, COUNT(DISTINCT market_hash_name) as unique_count
            FROM marketplace_sales GROUP BY platform
        """).fetchall():
            if row[0] in platform_stats:
                platform_stats[row[0]]["sales_total"] = row[1]
                platform_stats[row[0]]["sales_unique_skins"] = row[2]
            else:
                platform_stats[row[0]] = {"listings_total": 0, "listings_unique_skins": 0, "sales_total": row[1], "sales_unique_skins": row[2], "transactions_high": 0}

        for row in conn.execute("""
            SELECT platform, COUNT(*) as n FROM transactions WHERE confidence='HIGH' GROUP BY platform
        """).fetchall():
            if row[0] in platform_stats:
                platform_stats[row[0]]["transactions_high"] = row[1]

        return {
            "total_transactions": total_tx,
            "average_ttd_seconds": avg_ttd,
            "bot_snipes_count": bot_snipes,
            "quick_buys_count": quick_buys,
            "normal_sales_count": normal_sales,
            "active_listings_count": active_listings,
            "platform_stats": platform_stats
        }
    except Exception as e:
        logger.error(f"Erreur lors du calcul des stats : {e}")
        raise HTTPException(status_code=500, detail="Error fetching statistics")
    finally:
        conn.close()

@app.get("/api/transactions")
def get_transactions(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    search: str = Query(default=None),
    skin_name: str = Query(default=None),
    platform: str = Query(default=None),
    speed: str = Query(default=None), # 'bot', 'quick', 'normal'
    confidence: str = Query(default=None), # 'HIGH', 'MEDIUM', 'LOW'
    target: str = Query(default=None), # 'true' = liste 800, 'false' = hors liste
    sort_by: str = Query(default="timestamp"),
    sort_dir: str = Query(default="desc")
):
    """Retourne la liste des transactions avec pagination, filtres et tri."""
    conn = get_db_connection()
    try:
        # Dictionnaire de correspondance sécurisé pour le tri
        allowed_sort_columns = {
            "timestamp": "timestamp",
            "price": "price_usd",
            "price_usd": "price_usd",
            "float": "float_value",
            "float_value": "float_value",
            "ttd": "ttd_ms",
            "ttd_ms": "ttd_ms",
            "ttd_seconds": "ttd_ms",
            "discount_pct": "discount_pct",
        }

        db_sort_col = allowed_sort_columns.get(sort_by, "timestamp")
        db_sort_dir = "ASC" if sort_dir.lower() == "asc" else "DESC"

        # NULLs en dernier quel que soit le sens du tri
        null_order = "NULLS LAST" if db_sort_dir == "ASC" else "NULLS LAST"

        query = """
        SELECT id, timestamp, market_hash_name, price_usd, float_value, paint_seed,
               sticker_count, sticker_names, (ttd_ms / 1000.0) AS ttd_seconds, platform, confidence,
               ref_price_usd, ref_price_confidence,
               CASE WHEN ref_price_usd IS NOT NULL AND ref_price_usd > 0
                    THEN (price_usd - ref_price_usd) / ref_price_usd * 100.0
                    ELSE NULL END AS discount_pct
        FROM transactions WHERE 1=1
        """
        params = []
        
        if search:
            query += " AND market_hash_name LIKE ?"
            params.append(f"%{search}%")
            
        if skin_name:
            query += " AND market_hash_name = ?"
            params.append(skin_name)
            
        if platform:
            query += " AND platform = ?"
            params.append(platform)
            
        if speed:
            if speed == "bot":
                query += " AND ttd_ms < 5000"
            elif speed == "quick":
                query += " AND ttd_ms >= 5000 AND ttd_ms < 60000"
            elif speed == "normal":
                query += " AND ttd_ms >= 60000"

        if confidence:
            allowed_confidences = {"HIGH", "MEDIUM", "LOW"}
            if confidence.upper() in allowed_confidences:
                query += " AND confidence = ?"
                params.append(confidence.upper())

        if target == "true" and TARGET_SKINS:
            placeholders = ",".join("?" * len(TARGET_SKINS))
            query += f" AND market_hash_name IN ({placeholders})"
            params.extend(list(TARGET_SKINS))
        elif target == "false" and TARGET_SKINS:
            placeholders = ",".join("?" * len(TARGET_SKINS))
            query += f" AND market_hash_name NOT IN ({placeholders})"
            params.extend(list(TARGET_SKINS))

        query += f" ORDER BY {db_sort_col} {db_sort_dir} NULLS LAST LIMIT ? OFFSET ?;"
        params.extend([limit, offset])

        cursor = conn.execute(query, params)
        transactions = []
        for row in cursor.fetchall():
            tx = dict(row)
            tx["is_target"] = tx["market_hash_name"] in TARGET_SKINS
            transactions.append(tx)

        return transactions
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des transactions : {e}")
        raise HTTPException(status_code=500, detail="Error fetching transactions")
    finally:
        conn.close()

@app.get("/api/top-items")
def get_top_items(
    limit: int = Query(default=15, ge=1, le=100),
    platform: str = Query(default=None),
    speed: str = Query(default=None),
    confidence: str = Query(default=None),
    target: str = Query(default=None), # 'true' = liste 800, 'false' = hors liste
):
    """Retourne la liste des items les plus fréquemment achetés avec filtrage optionnel par plateforme, vitesse et fiabilité."""
    conn = get_db_connection()
    try:
        query = """
        SELECT market_hash_name, COUNT(*) as snipes_count, AVG(ttd_ms) / 1000.0 as avg_ttd_seconds, AVG(price_usd) as avg_price_usd
        FROM transactions
        WHERE 1=1
        """
        params = []

        # Filtrage par vitesse (si non spécifié, défaut à < 60s pour la notion de snipe)
        if speed == "bot":
            query += " AND ttd_ms < 5000"
        elif speed == "quick":
            query += " AND ttd_ms >= 5000 AND ttd_ms < 60000"
        elif speed == "normal":
            query += " AND ttd_ms >= 60000"
        else:
            query += " AND ttd_ms < 60000"

        if platform:
            query += " AND platform = ?"
            params.append(platform)

        if confidence and confidence.upper() in {"HIGH", "MEDIUM", "LOW"}:
            query += " AND confidence = ?"
            params.append(confidence.upper())

        if target == "true" and TARGET_SKINS:
            placeholders = ",".join("?" * len(TARGET_SKINS))
            query += f" AND market_hash_name IN ({placeholders})"
            params.extend(list(TARGET_SKINS))
        elif target == "false" and TARGET_SKINS:
            placeholders = ",".join("?" * len(TARGET_SKINS))
            query += f" AND market_hash_name NOT IN ({placeholders})"
            params.extend(list(TARGET_SKINS))

        query += """
        GROUP BY market_hash_name
        ORDER BY snipes_count DESC
        LIMIT ?;
        """
        params.append(limit)

        cursor = conn.execute(query, params)
        top_items = []
        for row in cursor.fetchall():
            top_items.append({
                "market_hash_name": row["market_hash_name"],
                "snipes_count": row["snipes_count"],
                "avg_ttd_seconds": round(row["avg_ttd_seconds"], 2),
                "avg_price_usd": round(row["avg_price_usd"], 2),
                "is_target": row["market_hash_name"] in TARGET_SKINS,
            })
        return top_items
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des top items : {e}")
        raise HTTPException(status_code=500, detail="Error fetching top items")
    finally:
        conn.close()

@app.get("/api/ml-stats")
def get_ml_stats():
    """Statistiques du dataset pour l'entraînement ML et usage disque."""
    import shutil

    conn = get_db_connection()
    try:
        # Comptages par catégorie
        rows = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN category != 'EXPIRED' THEN 1 ELSE 0 END) as sold,
                SUM(CASE WHEN category  = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
                SUM(CASE WHEN ref_price_usd IS NOT NULL THEN 1 ELSE 0 END) as with_ref_price,
                SUM(CASE WHEN sticker_count > 0 THEN 1 ELSE 0 END) as with_stickers,
                COUNT(DISTINCT market_hash_name) as unique_skins
            FROM transactions;
        """).fetchone()

        total = rows["total"] or 0
        sold = rows["sold"] or 0
        expired = rows["expired"] or 0
        with_ref = rows["with_ref_price"] or 0
        unique_skins = rows["unique_skins"] or 0

        # Distribution TTD (ventes seulement, pas les expirés)
        dist = conn.execute("""
            SELECT
                SUM(CASE WHEN ttd_ms < 5000 THEN 1 ELSE 0 END) as bot,
                SUM(CASE WHEN ttd_ms >= 5000 AND ttd_ms < 60000 THEN 1 ELSE 0 END) as fast,
                SUM(CASE WHEN ttd_ms >= 60000 THEN 1 ELSE 0 END) as normal
            FROM transactions WHERE category != 'EXPIRED';
        """).fetchone()

        # Top skins par nombre de points de données
        top_skins = conn.execute("""
            SELECT market_hash_name,
                   COUNT(*) as total_points,
                   SUM(CASE WHEN category != 'EXPIRED' THEN 1 ELSE 0 END) as sold_points,
                   SUM(CASE WHEN category  = 'EXPIRED' THEN 1 ELSE 0 END) as expired_points,
                   SUM(CASE WHEN ref_price_usd IS NOT NULL THEN 1 ELSE 0 END) as with_ref
            FROM transactions
            GROUP BY market_hash_name
            ORDER BY total_points DESC
            LIMIT 20;
        """).fetchall()

    except Exception as e:
        logger.error(f"Erreur ml-stats : {e}")
        raise HTTPException(status_code=500, detail="Error fetching ML stats")
    finally:
        conn.close()

    # Disk usage
    try:
        disk = shutil.disk_usage("/home")
    except Exception:
        try:
            disk = shutil.disk_usage("/")
        except Exception:
            disk = None

    db_size_bytes = 0
    try:
        db_size_bytes = os.path.getsize(config.OBSERVER_DB_PATH)
    except Exception:
        pass

    return {
        "dataset": {
            "total": total,
            "sold": sold,
            "expired": expired,
            "unique_skins": unique_skins,
            "with_ref_price": with_ref,
            "ref_price_coverage_pct": round(with_ref / total * 100, 1) if total else 0,
        },
        "ttd_distribution": {
            "bot_snipe": dist["bot"] if dist else 0,
            "fast_human": dist["fast"] if dist else 0,
            "normal_sale": dist["normal"] if dist else 0,
        },
        "disk": {
            "total_bytes": disk.total if disk else 0,
            "used_bytes": disk.used if disk else 0,
            "free_bytes": disk.free if disk else 0,
            "db_size_bytes": db_size_bytes,
        },
        "top_skins": [
            {
                "name": r["market_hash_name"],
                "total": r["total_points"],
                "sold": r["sold_points"],
                "expired": r["expired_points"],
                "with_ref": r["with_ref"],
            }
            for r in top_skins
        ],
    }


TRADING_DB_PATH = os.getenv("TRADING_DB_PATH", "/home/ubuntu/cs2-trading-bot/data/trading.db")

def get_trading_db_connection():
    """Connexion read-only à la DB du bot de trading."""
    db_path = os.path.abspath(TRADING_DB_PATH)
    if not os.path.exists(db_path):
        return None
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/api/signals")
def get_signals(limit: int = Query(100, ge=1, le=500), decision: str = Query(None)):
    """Signaux GO/WATCH détectés par le bot de trading.
    Pour chaque signal GO, vérifie si une vente a eu lieu dans l'heure suivante.
    `decision` (GO/WATCH) filtre côté serveur — sinon les GO rares sont noyés sous les WATCH."""
    trading_conn = get_trading_db_connection()
    if trading_conn is None:
        return {"signals": [], "error": "Trading bot DB not found"}
    obs_conn = get_db_connection()
    decision_filter = "WHERE decision = ?" if decision in ("GO", "WATCH") else ""
    params = ([decision, limit] if decision in ("GO", "WATCH") else [limit])
    try:
        rows = trading_conn.execute(f"""
            SELECT detected_at, listed_at, platform, listing_id, market_hash_name,
                   float_value, price_usd, ref_price_usd, discount_pct,
                   predicted_ttd_h, predicted_ttd_resell_h,
                   optimal_sell_price, expected_roi_per_hour, expected_profit_usd,
                   expected_p_sell, expected_ev_usd, breakeven_p_sell,
                   decision, sticker_count
            FROM signals
            {decision_filter}
            ORDER BY detected_at DESC
            LIMIT ?
        """, params).fetchall()

        signals = []
        for r in rows:
            confirmed_sale = None
            confirmed_ttd_h = None

            # Pour tous les signaux, cherche une vente dans l'heure suivante
            if r["float_value"]:
                from datetime import datetime as dt, timezone as tz
                try:
                    detected_ts = dt.fromisoformat(r["detected_at"].replace("Z", "+00:00")).timestamp()
                    window_end = detected_ts + 3600  # 1h après détection
                    sale = obs_conn.execute("""
                        SELECT sale_ts, price_usd FROM marketplace_sales
                        WHERE market_hash_name = ?
                          AND platform = ?
                          AND ABS(float_value - ?) < 0.000001
                          AND sale_ts BETWEEN ? AND ?
                        ORDER BY sale_ts ASC
                        LIMIT 1
                    """, (r["market_hash_name"], r["platform"], r["float_value"],
                          detected_ts, window_end)).fetchone()
                    if sale:
                        confirmed_sale = True
                        confirmed_ttd_h = (sale["sale_ts"] - detected_ts) / 3600
                except Exception:
                    pass

            signals.append({
                "detected_at": r["detected_at"],
                "platform": r["platform"],
                "listing_id": r["listing_id"],
                "market_hash_name": r["market_hash_name"],
                "float_value": r["float_value"],
                "price_usd": r["price_usd"],
                "ref_price_usd": r["ref_price_usd"],
                "discount_pct": round(r["discount_pct"] or 0, 1),
                "predicted_ttd_h":        round(r["predicted_ttd_h"], 2) if r["predicted_ttd_h"] else None,
                "predicted_ttd_resell_h": round(r["predicted_ttd_resell_h"], 2) if r["predicted_ttd_resell_h"] else None,
                "optimal_sell_price":     round(r["optimal_sell_price"], 2) if r["optimal_sell_price"] else None,
                "expected_roi_per_hour":  round(r["expected_roi_per_hour"], 2) if r["expected_roi_per_hour"] else None,
                "expected_profit_usd":    round(r["expected_profit_usd"], 2) if r["expected_profit_usd"] else None,
                "expected_p_sell":        round(r["expected_p_sell"], 4) if r["expected_p_sell"] else None,
                "expected_ev_usd":        round(r["expected_ev_usd"], 2) if r["expected_ev_usd"] else None,
                "breakeven_p_sell":       round(r["breakeven_p_sell"], 4) if r["breakeven_p_sell"] else None,
                "decision": r["decision"],
                "sticker_count": r["sticker_count"],
                "confirmed_sale": confirmed_sale,
                "confirmed_ttd_h": round(confirmed_ttd_h, 2) if confirmed_ttd_h else None,
                "detection_delay_s": max(0, round(
                    dt.fromisoformat(r["detected_at"].replace("Z", "+00:00")).timestamp() - r["listed_at"]
                )) if r["listed_at"] else None,
            })

        # Ventes récentes (1h glissante) avec flag "détecté par le bot"
        from datetime import datetime as dt, timezone as tz
        now_ts = dt.now(tz.utc).timestamp()
        recent_sales = obs_conn.execute("""
            SELECT market_hash_name, platform, float_value, price_usd, sale_ts
            FROM marketplace_sales
            WHERE sale_ts > ? AND float_value IS NOT NULL
            ORDER BY sale_ts DESC
            LIMIT 200
        """, (now_ts - 3600,)).fetchall()

        sales_with_detection = []
        for sale in recent_sales:
            # Cherche si le bot a émis un signal pour ce skin dans la dernière heure
            # Matching par market_hash_name + platform (fiable car c'est l'ID du skin)
            # Comparaison sur epoch (strftime) car detected_at est en ISO avec 'T'
            # alors que datetime(unixepoch) utilise un espace → comparaison string KO
            signal = trading_conn.execute("""
                SELECT decision, predicted_ttd_h, discount_pct
                FROM signals
                WHERE market_hash_name = ?
                  AND platform = ?
                  AND CAST(strftime('%s', detected_at) AS REAL) BETWEEN ? AND ?
                ORDER BY detected_at DESC
                LIMIT 1
            """, (sale["market_hash_name"], sale["platform"],
                  sale["sale_ts"] - 3600, sale["sale_ts"])).fetchone()

            # TTD réel depuis transactions (vente réconciliée avec listing observé)
            tx = obs_conn.execute("""
                SELECT ttd_ms FROM transactions
                WHERE market_hash_name = ?
                  AND platform = ?
                  AND ABS(float_value - ?) < 0.000001
                  AND ABS(price_usd - ?) < 0.01
                ORDER BY ABS(price_usd - ?) ASC
                LIMIT 1
            """, (sale["market_hash_name"], sale["platform"], sale["float_value"],
                  sale["price_usd"], sale["price_usd"])).fetchone()

            real_ttd_h = round(tx["ttd_ms"] / 3600000, 4) if tx and tx["ttd_ms"] is not None else None
            real_ttd_s = round(tx["ttd_ms"] / 1000) if tx and tx["ttd_ms"] is not None else None

            sales_with_detection.append({
                "market_hash_name": sale["market_hash_name"],
                "platform": sale["platform"],
                "float_value": sale["float_value"],
                "price_usd": sale["price_usd"],
                "sale_ts": sale["sale_ts"],
                "detected_by_bot": signal is not None,
                "bot_decision": signal["decision"] if signal else None,
                "bot_predicted_ttd_h": round(signal["predicted_ttd_h"], 2) if signal and signal["predicted_ttd_h"] else None,
                "bot_discount_pct": round(signal["discount_pct"], 1) if signal and signal["discount_pct"] else None,
                "real_ttd_h": real_ttd_h,
                "real_ttd_s": real_ttd_s,
            })

        return {
            "total": len(signals),
            "signals": signals,
            "recent_sales": sales_with_detection,
        }
    finally:
        trading_conn.close()
        obs_conn.close()


@app.get("/api/backtest")
def run_backtest(
    min_discount: float = Query(20.0, description="Décote minimum %"),
    max_ttd_resell: float = Query(3.0, description="TTD revente max (h)"),
    min_sales_vol: int = Query(5, description="Volume ventes min"),
):
    """Backtest du modèle sur les transactions historiques."""
    import numpy as np
    import onnxruntime as ort
    from datetime import datetime as dt_class

    obs_conn = get_db_connection()
    try:
        model_path = config.TRADING_MODEL_PATH if hasattr(config, 'TRADING_MODEL_PATH') else "/home/ubuntu/cs2-trading-bot/model/ttd_predictor.onnx"
        try:
            session = ort.InferenceSession(model_path)
        except Exception as e:
            return {"error": f"Modèle non disponible: {e}"}

        def predict_ttd(feats):
            X = np.array([[
                feats.get('price_usd',0), feats.get('float_value',0),
                feats.get('sticker_count',0), feats.get('discount_pct',0),
                feats.get('n_listings',0), feats.get('is_knife',0),
                feats.get('is_gloves',0), feats.get('is_stattrak',0),
                feats.get('is_souvenir',0), feats.get('wear_fn',0),
                feats.get('wear_mw',0), feats.get('wear_ft',0),
                feats.get('wear_ww',0), feats.get('wear_bs',0),
                feats.get('price_percentile',0), feats.get('price_cv',0),
                feats.get('sales_volume',0), feats.get('hour_of_day',0),
                feats.get('day_of_week',0),
            ]], dtype=np.float32)
            out = session.run(None, {'input': X})
            return float(np.expm1(out[0].flat[0]))

        # Pré-calculs
        vol_map = {(r[0], r[1]): r[2] for r in obs_conn.execute(
            "SELECT market_hash_name, platform, COUNT(*) FROM marketplace_sales GROUP BY market_hash_name, platform"
        ).fetchall()}

        listings_cache = {}
        for r in obs_conn.execute("SELECT market_hash_name, platform, price_cents FROM observed_listings ORDER BY market_hash_name, platform, price_cents ASC").fetchall():
            listings_cache.setdefault((r[0], r[1]), []).append(r[2] / 100.0)

        transactions = obs_conn.execute("""
            SELECT market_hash_name, price_usd, float_value, sticker_count, ttd_ms, platform, timestamp
            FROM transactions WHERE ttd_ms IS NOT NULL AND float_value IS NOT NULL
        """).fetchall()

        go_correct, go_incorrect, watch_missed, watch_ok = [], [], [], []

        for tx in transactions:
            name, price, platform = tx[0], tx[1], tx[5]
            real_ttd_h = tx[4] / 3600000.0
            prices = listings_cache.get((name, platform), [])
            n = len(prices)
            if n < 5: continue
            p10 = prices[max(0, n//10)]
            discount = (p10 - price) / p10 * 100 if p10 > 0 else 0
            if discount < min_discount: continue
            vol = vol_map.get((name, platform), 0)
            if vol < min_sales_vol: continue

            mean_p = sum(prices)/n
            var = sum((p-mean_p)**2 for p in prices)/n
            cv = (var**0.5)/mean_p if mean_p > 0 else 0.0
            pct = sum(1 for p in prices if p < price)/n*100
            wear = tx[2]
            try:
                d = dt_class.fromisoformat(tx[6]); hour, dow = d.hour, d.weekday()
            except: hour, dow = 12, 0

            feats_resell = {
                'price_usd': price*1.10, 'float_value': wear,
                'sticker_count': tx[3], 'n_listings': n,
                'discount_pct': (p10-price*1.10)/p10*100 if p10>0 else 0,
                'is_knife': 1 if '★' in name and not any(k in name for k in ['Gloves','Wraps','Moto','Driver','Sport','Hand','Specialist']) else 0,
                'is_gloves': 1 if any(k in name for k in ['Gloves','Wraps','Moto','Driver','Sport','Hand','Specialist']) else 0,
                'is_stattrak': 1 if 'StatTrak' in name else 0, 'is_souvenir': 1 if 'Souvenir' in name else 0,
                'wear_fn': 1 if wear<0.07 else 0, 'wear_mw': 1 if 0.07<=wear<0.15 else 0,
                'wear_ft': 1 if 0.15<=wear<0.38 else 0, 'wear_ww': 1 if 0.38<=wear<0.45 else 0, 'wear_bs': 1 if wear>=0.45 else 0,
                'price_percentile': sum(1 for p in prices if p < price*1.10)/n*100,
                'price_cv': cv, 'sales_volume': vol, 'hour_of_day': hour, 'day_of_week': dow,
            }
            ttd_resell = predict_ttd(feats_resell)
            decision = 'GO' if ttd_resell < max_ttd_resell else 'WATCH'
            entry = {'name': name, 'price': round(price,2), 'discount': round(discount,1),
                     'real_ttd_h': round(real_ttd_h,2), 'ttd_resell_pred': round(ttd_resell,2),
                     'vol': vol, 'platform': platform}
            if decision == 'GO':
                (go_correct if real_ttd_h < 24.0 else go_incorrect).append(entry)
            else:
                (watch_missed if real_ttd_h < max_ttd_resell else watch_ok).append(entry)

        total = len(go_correct)+len(go_incorrect)+len(watch_missed)+len(watch_ok)
        go_total = len(go_correct)+len(go_incorrect)
        precision = len(go_correct)/go_total*100 if go_total else 0
        recall = len(go_correct)/(len(go_correct)+len(watch_missed))*100 if (len(go_correct)+len(watch_missed)) else 0
        avg_real = sum(e['real_ttd_h'] for e in go_correct)/len(go_correct) if go_correct else 0
        avg_pred = sum(e['ttd_resell_pred'] for e in go_correct)/len(go_correct) if go_correct else 0

        return {
            "params": {"min_discount": min_discount, "max_ttd_resell": max_ttd_resell, "min_sales_vol": min_sales_vol},
            "summary": {
                "total_filtered": total, "go_total": go_total,
                "go_correct": len(go_correct), "go_incorrect": len(go_incorrect),
                "watch_missed": len(watch_missed), "watch_ok": len(watch_ok),
                "precision_pct": round(precision, 1), "recall_pct": round(recall, 1),
                "avg_real_ttd_h": round(avg_real, 2), "avg_pred_ttd_h": round(avg_pred, 2),
            },
            "go_correct":   sorted(go_correct,   key=lambda x: x['real_ttd_h'])[:50],
            "go_incorrect": sorted(go_incorrect,  key=lambda x: -x['real_ttd_h'])[:20],
            "watch_missed": sorted(watch_missed,  key=lambda x: x['real_ttd_h'])[:20],
        }
    finally:
        obs_conn.close()


if __name__ == "__main__":
    # Démarrage sur le port 8000 sur toutes les interfaces réseau (0.0.0.0)
    uvicorn.run("api_main:app", host="0.0.0.0", port=8000, reload=False)
