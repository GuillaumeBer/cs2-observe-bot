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

@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "CS2 Observer API is running",
        "database_configured": config.OBSERVER_DB_PATH
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
    platform: str = Query(default=None),
    speed: str = Query(default=None) # 'bot', 'quick', 'normal'
):
    """Retourne la liste des transactions avec pagination et filtres."""
    conn = get_db_connection()
    try:
        query = """
        SELECT id, timestamp, market_hash_name, price_usd, float_value, paint_seed, 
               sticker_count, sticker_names, (ttd_ms / 1000.0) AS ttd_seconds, platform, confidence 
        FROM transactions WHERE 1=1
        """
        params = []
        
        if search:
            query += " AND market_hash_name LIKE ?"
            params.append(f"%{search}%")
            
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
                
        query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?;"
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
def get_top_items(limit: int = Query(default=15, ge=1, le=100)):
    """Retourne la liste des items les plus fréquemment achetés rapidement (< 60s)."""
    conn = get_db_connection()
    try:
        query = """
        SELECT market_hash_name, COUNT(*) as snipes_count, AVG(ttd_ms) / 1000.0 as avg_ttd_seconds, AVG(price_usd) as avg_price_usd
        FROM transactions
        WHERE ttd_ms < 60000
        GROUP BY market_hash_name
        ORDER BY snipes_count DESC
        LIMIT ?;
        """
        cursor = conn.execute(query, [limit])
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
