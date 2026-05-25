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
from datetime import datetime, timezone

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
            # Si le bot tourne mais plus d'activité log depuis 120 secondes, suspicion de blocage
            if diff_sec > 120:
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
def get_global_stats():
    """Calcule et retourne les statistiques globales d'observation."""
    conn = get_db_connection()
    try:
        # Total des transactions
        total_tx = conn.execute("SELECT COUNT(*) FROM transactions;").fetchone()[0]
        
        # TTD Moyen
        avg_ttd_row = conn.execute("SELECT AVG(ttd_ms) FROM transactions;").fetchone()[0]
        avg_ttd = round(avg_ttd_row / 1000.0, 2) if avg_ttd_row else 0.0
        
        # Snipes bot (< 5s)
        bot_snipes = conn.execute("SELECT COUNT(*) FROM transactions WHERE ttd_ms < 5000;").fetchone()[0]
        
        # Achats rapides (>= 5s et < 60s)
        quick_buys = conn.execute("SELECT COUNT(*) FROM transactions WHERE ttd_ms >= 5000 AND ttd_ms < 60000;").fetchone()[0]
        
        # Ventes normales (>= 60s)
        normal_sales = conn.execute("SELECT COUNT(*) FROM transactions WHERE ttd_ms >= 60000;").fetchone()[0]
        
        # Listings actifs en cours d'observation
        active_listings = conn.execute("SELECT COUNT(*) FROM observed_listings;").fetchone()[0]
        
        return {
            "total_transactions": total_tx,
            "average_ttd_seconds": avg_ttd,
            "bot_snipes_count": bot_snipes,
            "quick_buys_count": quick_buys,
            "normal_sales_count": normal_sales,
            "active_listings_count": active_listings
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
            "ttd_seconds": "ttd_ms"
        }
        
        # Validation du champ de tri
        db_sort_col = allowed_sort_columns.get(sort_by, "timestamp")
        
        # Validation de la direction du tri
        db_sort_dir = "ASC" if sort_dir.lower() == "asc" else "DESC"

        query = """
        SELECT id, timestamp, market_hash_name, price_usd, float_value, paint_seed, 
               sticker_count, sticker_names, (ttd_ms / 1000.0) AS ttd_seconds, platform, confidence 
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
                
        query += f" ORDER BY {db_sort_col} {db_sort_dir} LIMIT ? OFFSET ?;"
        params.extend([limit, offset])
        
        cursor = conn.execute(query, params)
        transactions = []
        for row in cursor.fetchall():
            tx = dict(row)
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
    speed: str = Query(default=None)
):
    """Retourne la liste des items les plus fréquemment achetés avec filtrage optionnel par plateforme et vitesse."""
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
                "avg_price_usd": round(row["avg_price_usd"], 2)
            })
        return top_items
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des top items : {e}")
        raise HTTPException(status_code=500, detail="Error fetching top items")
    finally:
        conn.close()

if __name__ == "__main__":
    # Démarrage sur le port 8000 sur toutes les interfaces réseau (0.0.0.0)
    uvicorn.run("api_main:app", host="0.0.0.0", port=8000, reload=False)
