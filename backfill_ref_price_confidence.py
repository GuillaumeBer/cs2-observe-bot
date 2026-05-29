#!/usr/bin/env python3
"""
Backfill ref_price_confidence pour les transactions existantes.
Utilise la même logique que _compute_ref_price() avec les seuils stricts.
"""
import sqlite3
from datetime import datetime
import sys

def compute_ref_price_confidence(conn, market_hash_name, before_ts, suggested_price_cents=None):
    """Calcule la confiance du prix de référence."""
    DAY = 86400.0

    # Source 1 : suggestedPrice DMarket (HIGH confiance)
    if suggested_price_cents and suggested_price_cents > 0:
        return "HIGH"

    cutoff_2d = before_ts - 2 * DAY
    cutoff_7d = before_ts - 7 * DAY

    # Source 2 : marketplace_sales (MEDIUM confiance)
    rows = conn.execute("""
        SELECT price_usd, sale_ts FROM marketplace_sales
        WHERE market_hash_name = ?
          AND sale_ts < ?
          AND sale_ts > ?
        ORDER BY sale_ts DESC
    """, (market_hash_name, before_ts, cutoff_7d)).fetchall()

    # 10+ ventes dans 2j
    prices_2d = [r[0] for r in rows if before_ts - r[1] < 2 * DAY]
    if len(prices_2d) >= 10:
        return "MEDIUM"

    # 5+ ventes dans 7j
    if len(rows) >= 5:
        return "MEDIUM"

    return None

# Connect
conn = sqlite3.connect('/home/ubuntu/cs2-observation-bot/data/observer_dataset.db')
cursor = conn.cursor()

# Transactions sans confiance
cursor.execute("""
    SELECT t.id, t.market_hash_name, t.timestamp, t.ref_price_usd,
           MAX(ol.suggested_price_cents) as suggested_price_cents
    FROM transactions t
    LEFT JOIN observed_listings ol ON ol.market_hash_name = t.market_hash_name
    WHERE t.ref_price_usd IS NOT NULL AND t.ref_price_confidence IS NULL
    GROUP BY t.id
    ORDER BY t.timestamp
""")

rows = cursor.fetchall()
print(f"Backfill: {len(rows)} transactions sans confidence\n")

updated = 0
for i, (id_, name, ts_str, ref_price, suggested_price) in enumerate(rows):
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        conf = compute_ref_price_confidence(conn, name, ts, suggested_price)

        if conf:
            conn.execute("""
                UPDATE transactions
                SET ref_price_confidence = ?
                WHERE id = ?
            """, (conf, id_))
            updated += 1

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(rows)} traitées...")
    except Exception as e:
        print(f"  Error row {id_}: {e}")

conn.commit()
conn.close()

print(f"\n✅ Backfill terminé: {updated}/{len(rows)} transactions mises à jour")
