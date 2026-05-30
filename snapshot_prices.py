"""
Snapshot périodique des distributions de prix par skin.

Stocke des RESUMES statistiques (percentiles p5..p95 + moyenne/écart-type) par
(skin, plateforme) à l'instant courant, dans une base SEPAREE pour zéro contention
avec l'observer. Permet ensuite de calculer le percentile d'un event À SON HEURE
de vente (et non vs les listings actuels), pour un modèle de survie plus précis.

Léger : ~12 Mo/jour, ~250 Mo sur 21 jours. Lancé par un timer systemd (horaire).
"""
import sqlite3
import time
import math

OBS_DB  = "/home/ubuntu/cs2-observation-bot/data/observer_dataset.db"
SNAP_DB = "/home/ubuntu/cs2-observation-bot/data/price_snapshots.db"
MIN_LISTINGS   = 5
RETENTION_DAYS = 21


def percentile(sorted_vals, q):
    """q en 0..100 ; convention index = floor(q/100 * n), capé."""
    n = len(sorted_vals)
    return sorted_vals[min(n - 1, max(0, int(q / 100.0 * n)))]


def main():
    now = time.time()

    # Lecture (read-only) de l'observer, groupé par skin
    obs = sqlite3.connect(f"file:{OBS_DB}?mode=ro", uri=True, timeout=30)
    obs.execute("PRAGMA busy_timeout=30000")
    rows = obs.execute("""
        SELECT market_hash_name, platform, price_cents
        FROM observed_listings
        ORDER BY market_hash_name, platform, price_cents
    """).fetchall()
    obs.close()

    # Base snapshots séparée
    snap = sqlite3.connect(SNAP_DB, timeout=30)
    snap.execute("PRAGMA journal_mode=WAL")
    snap.execute("PRAGMA busy_timeout=30000")
    snap.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            snapshot_ts      REAL NOT NULL,
            market_hash_name TEXT NOT NULL,
            platform         TEXT NOT NULL,
            n_listings       INTEGER NOT NULL,
            p5  REAL, p10 REAL, p25 REAL, p50 REAL, p75 REAL, p90 REAL, p95 REAL,
            min_price REAL, max_price REAL, mean_price REAL, std_price REAL
        )
    """)
    snap.execute("CREATE INDEX IF NOT EXISTS idx_snap_skin ON price_snapshots(market_hash_name, platform, snapshot_ts)")

    # Agrégation par skin en un seul passage (rows triées)
    batch = []
    cur_key = None
    prices = []

    def flush(key, prices):
        if key is None or len(prices) < MIN_LISTINGS:
            return
        name, platform = key
        n = len(prices)
        mean = sum(prices) / n
        std = math.sqrt(sum((p - mean) ** 2 for p in prices) / n)
        batch.append((
            now, name, platform, n,
            percentile(prices, 5), percentile(prices, 10), percentile(prices, 25),
            percentile(prices, 50), percentile(prices, 75), percentile(prices, 90),
            percentile(prices, 95), prices[0], prices[-1], round(mean, 2), round(std, 2),
        ))

    for name, platform, cents in rows:
        key = (name, platform)
        if key != cur_key:
            flush(cur_key, prices)
            cur_key, prices = key, []
        prices.append(cents / 100.0)
    flush(cur_key, prices)

    with snap:
        snap.executemany("""
            INSERT INTO price_snapshots
            (snapshot_ts, market_hash_name, platform, n_listings,
             p5, p10, p25, p50, p75, p90, p95, min_price, max_price, mean_price, std_price)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)
        snap.execute("DELETE FROM price_snapshots WHERE snapshot_ts < ?", (now - RETENTION_DAYS * 86400,))

    total = snap.execute("SELECT COUNT(*) FROM price_snapshots").fetchone()[0]
    snap.close()
    print(f"Snapshot OK : {len(batch)} skins capturés à {time.strftime('%Y-%m-%d %H:%M', time.gmtime(now))} UTC | total table: {total:,}")


if __name__ == "__main__":
    main()
