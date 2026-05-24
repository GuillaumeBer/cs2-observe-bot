import sqlite3
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple
import config

logger = logging.getLogger("cs2_sniper.transaction_db")


class TransactionDatabase:
    """
    Gère la base de données SQLite des transactions réelles (disparitions d'items).
    Sert à calculer les prix de référence dynamiques et les multiplicateurs d'usure (float).
    """

    def __init__(self, db_path: str = "data/transactions.db"):
        self.db_path = db_path
        # Assurer que le dossier parent existe
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialise la table transactions et la table opportunities avec leurs index."""
        query_tx = """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            market_hash_name TEXT NOT NULL,
            price_usd REAL NOT NULL,
            float_value REAL,
            paint_seed INTEGER,
            sticker_count INTEGER DEFAULT 0,
            sticker_names TEXT, -- Stocké au format JSON (liste de chaines)
            ttd_ms REAL NOT NULL,
            category TEXT NOT NULL,
            platform TEXT NOT NULL,
            confidence TEXT NOT NULL DEFAULT 'LOW'
        );
        """
        query_opp = """
        CREATE TABLE IF NOT EXISTS opportunities (
            id TEXT PRIMARY KEY, -- ID unique du listing (ex: platform_offerid)
            timestamp TEXT NOT NULL,
            market_hash_name TEXT NOT NULL,
            price_usd REAL NOT NULL,
            base_ref_price_usd REAL NOT NULL,
            adjusted_ref_price_usd REAL NOT NULL,
            discount_percent REAL NOT NULL,
            net_profit_usd REAL NOT NULL,
            gross_profit_usd REAL NOT NULL,
            volume_24h REAL NOT NULL,
            float_value REAL,
            float_desc TEXT,
            paint_seed INTEGER,
            sticker_count INTEGER DEFAULT 0,
            sticker_desc TEXT,
            is_premium INTEGER DEFAULT 0,
            item_url TEXT NOT NULL
        );
        """
        query_observed = """
        CREATE TABLE IF NOT EXISTS observed_listings (
            listing_id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            listed_at REAL,
            market_hash_name TEXT NOT NULL,
            price_cents INTEGER NOT NULL,
            float_value REAL,
            paint_seed INTEGER,
            sticker_count INTEGER DEFAULT 0,
            sticker_names TEXT,
            platform TEXT NOT NULL
        );
        """
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(query_tx)
                conn.execute(query_opp)
                conn.execute(query_observed)
                # Créer des index pour accélérer les requêtes fréquentes
                conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON transactions (market_hash_name);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_name_float ON transactions (market_hash_name, float_value);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_opp_name ON opportunities (market_hash_name);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_name ON observed_listings (market_hash_name);")
                # Migrations pour BD existante
                try:
                    conn.execute("ALTER TABLE transactions ADD COLUMN confidence TEXT NOT NULL DEFAULT 'LOW';")
                except Exception:
                    pass
                try:
                    conn.execute("ALTER TABLE observed_listings ADD COLUMN listed_at REAL;")
                except Exception:
                    pass
            logger.info("Base de données des transactions et opportunités initialisée avec succès.")
        except Exception as e:
            logger.error(f"Erreur d'initialisation de la base de données : {e}")
        finally:
            conn.close()

    def save_transaction(
        self,
        market_hash_name: str,
        price_usd: float,
        ttd_ms: float,
        platform: str,
        category: str,
        float_value: Optional[float] = None,
        paint_seed: Optional[int] = None,
        sticker_count: int = 0,
        sticker_names: Optional[List[str]] = None,
        timestamp: Optional[str] = None,
        confidence: str = "LOW",
    ) -> bool:
        """
        Enregistre une transaction détectée dans la base de données.
        Vérifie au préalable qu'un item avec le même float n'a pas été déjà enregistré
        comme vendu dans les dernières 24 heures pour éviter l'empoisonnement par annulations.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()

        # Seuls les skins avec un float réel sont enregistrés (stickers/kits/charms exclus)
        if not float_value or float_value <= 0:
            return False

        # 1. Vérification anti-empoisonnement par float identique récent
        if float_value is not None and float_value > 0:
            query_check = """
            SELECT timestamp FROM transactions 
            WHERE market_hash_name = ? AND float_value = ? 
            ORDER BY timestamp DESC LIMIT 1;
            """
            conn = self._get_connection()
            try:
                row = conn.execute(query_check, (market_hash_name, float_value)).fetchone()
                if row:
                    try:
                        last_ts = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
                        now_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        delta = now_dt - last_ts
                        if delta.total_seconds() < 86400:  # 24 heures
                            logger.warning(
                                f"Transaction ignorée (doublon de float récent / suspicion d'annulation) : "
                                f"{market_hash_name} (float: {float_value}) - Dernier TS: {row['timestamp']}"
                            )
                            return False
                    except Exception as parse_err:
                        logger.error(f"Erreur lors du parsing du timestamp de vérification : {parse_err}")
            except Exception as check_err:
                logger.error(f"Erreur lors de la vérification anti-doublon de float : {check_err}")
            finally:
                conn.close()

        stickers_json = json.dumps(sticker_names or [])

        # Debug temporaire : logger un traceback si float est None/0 juste avant INSERT
        if not float_value or float_value <= 0:
            import traceback
            logger.error(f"INSERT_NO_FLOAT_BUG: {market_hash_name} fv={float_value!r}\n{''.join(traceback.format_stack())}")

        query = """
        INSERT INTO transactions (
            timestamp, market_hash_name, price_usd, float_value,
            paint_seed, sticker_count, sticker_names, ttd_ms, category, platform, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(
                    query,
                    (
                        timestamp,
                        market_hash_name,
                        price_usd,
                        float_value,
                        paint_seed,
                        sticker_count,
                        stickers_json,
                        ttd_ms,
                        category,
                        platform,
                        confidence,
                    )
                )
            logger.debug(f"Transaction enregistrée pour {market_hash_name} à ${price_usd:.2f}")
            return True
        except Exception as e:
            logger.error(f"Erreur d'enregistrement de la transaction pour {market_hash_name} : {e}")
            return False
        finally:
            conn.close()

    def get_recent_transactions(self, market_hash_name: str, limit: int = 50) -> List[dict]:
        """
        Récupère les transactions les plus récentes pour un skin donné.
        """
        query = """
        SELECT * FROM transactions 
        WHERE market_hash_name = ? 
        ORDER BY timestamp DESC 
        LIMIT ?;
        """
        conn = self._get_connection()
        results = []
        try:
            cursor = conn.execute(query, (market_hash_name, limit))
            for row in cursor.fetchall():
                row_dict = dict(row)
                try:
                    row_dict["sticker_names"] = json.loads(row_dict["sticker_names"])
                except Exception:
                    row_dict["sticker_names"] = []
                results.append(row_dict)
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des transactions pour {market_hash_name} : {e}")
        finally:
            conn.close()
        return results

    def get_historical_median_price(
        self, 
        market_hash_name: str, 
        exclude_fast_ttd: bool = True, 
        limit: int = 50
    ) -> Optional[float]:
        """
        Calcule le prix médian observé à partir des transactions récentes.
        exclude_fast_ttd : Si True, ignore les snipes ultra-rapides (< seuil bot) pour éviter le biais vers le bas.
        """
        seuil_bot_ms = getattr(config, "OBS_BOT_SNIPE_TTD_MS", 5000)
        
        query = """
        SELECT price_usd FROM transactions 
        WHERE market_hash_name = ?
        """
        params = [market_hash_name]
        
        if exclude_fast_ttd:
            query += " AND ttd_ms >= ?"
            params.append(seuil_bot_ms)
            
        query += " ORDER BY timestamp DESC LIMIT ?;"
        params.append(limit)

        conn = self._get_connection()
        prices = []
        try:
            cursor = conn.execute(query, params)
            prices = [row["price_usd"] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Erreur de calcul de la médiane historique pour {market_hash_name} : {e}")
        finally:
            conn.close()

        if len(prices) < 3:
            # Historique insuffisant localement
            return None

        prices.sort()
        n = len(prices)
        if n % 2 == 1:
            return prices[n // 2]
        else:
            return (prices[n // 2 - 1] + prices[n // 2]) / 2.0

    def get_float_tier_range(self, float_value: float) -> Tuple[float, float, str]:
        """
        Détermine l'intervalle d'usure (palier de float) correspondant au float fourni.
        Retourne (min_float, max_float, nom_du_palier)
        """
        if float_value < 0.01:
            return 0.0, 0.01, "Ultra Low FN"
        elif float_value < 0.03:
            return 0.01, 0.03, "Low FN"
        elif float_value < 0.07:
            return 0.03, 0.07, "Average FN"
        elif float_value < 0.08:
            return 0.07, 0.080, "Ultra Low MW"
        elif float_value < 0.15:
            return 0.08, 0.15, "Standard MW"
        elif float_value < 0.18:
            return 0.15, 0.18, "Ultra Low FT"
        elif float_value < 0.38:
            return 0.18, 0.38, "Standard FT"
        elif float_value < 0.45:
            return 0.38, 0.45, "Standard WW"
        else:
            return 0.45, 1.0, "Standard BS"

    def get_float_tier_coefficient(
        self, 
        market_hash_name: str, 
        float_value: float, 
        base_median: float,
        limit: int = 30
    ) -> float:
        """
        Calcule le coefficient d'ajustement de prix historique lié à la catégorie d'usure (float tier).
        Retourne 1.0 s'il n'y a pas assez de ventes dans ce tier pour juger.
        """
        if float_value is None or base_median <= 0.0:
            return 1.0

        min_f, max_f, name = self.get_float_tier_range(float_value)

        # Chercher le prix médian des ventes dans la même tranche d'usure
        query = """
        SELECT price_usd FROM transactions 
        WHERE market_hash_name = ? AND float_value >= ? AND float_value < ?
        ORDER BY timestamp DESC LIMIT ?;
        """
        conn = self._get_connection()
        tier_prices = []
        try:
            cursor = conn.execute(query, (market_hash_name, min_f, max_f, limit))
            tier_prices = [row["price_usd"] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Erreur d'extraction du float coefficient pour {market_hash_name} : {e}")
        finally:
            conn.close()

        # Il nous faut au moins 3 points dans ce palier de float pour estimer le coefficient
        if len(tier_prices) < 3:
            return 1.0

        tier_prices.sort()
        n = len(tier_prices)
        if n % 2 == 1:
            tier_median = tier_prices[n // 2]
        else:
            tier_median = (tier_prices[n // 2 - 1] + tier_prices[n // 2]) / 2.0

        coeff = tier_median / base_median
        # Limiter à des coefficients réalistes pour éviter les aberrations (max +50% et min -50%)
        return max(0.5, min(coeff, 1.5))

    def get_historical_volume_24h(self, market_hash_name: str) -> float:
        """
        Calcule le volume moyen de transactions sur 24h pour un item donné
        en se basant sur l'historique local.
        """
        count_query = "SELECT COUNT(*) as cnt FROM transactions WHERE market_hash_name = ?;"
        range_query = "SELECT MIN(timestamp), MAX(timestamp) FROM transactions;"
        
        conn = self._get_connection()
        try:
            cnt = conn.execute(count_query, (market_hash_name,)).fetchone()["cnt"]
            min_ts, max_ts = conn.execute(range_query).fetchone()
        except Exception as e:
            logger.error(f"Erreur lors du calcul du volume historique pour {market_hash_name} : {e}")
            return 0.0
        finally:
            conn.close()

        if cnt == 0:
            return 0.0

        if not min_ts or not max_ts:
            return float(cnt)

        try:
            dt_min = datetime.fromisoformat(min_ts.replace("Z", "+00:00"))
            dt_max = datetime.fromisoformat(max_ts.replace("Z", "+00:00"))
            delta = dt_max - dt_min
            duration_days = delta.total_seconds() / 86400.0
        except Exception:
            duration_days = 0.0

        duration_days = max(duration_days, 1.0)
        sales_per_day = cnt / duration_days
        return round(sales_per_day, 2)

    def save_opportunity(self, opp: dict) -> bool:
        """
        Enregistre une opportunité de sniping détectée dans la table opportunities.
        """
        opp_id = opp.get("id")
        if not opp_id:
            logger.error("Impossible d'enregistrer une opportunité sans ID.")
            return False

        timestamp = opp.get("timestamp") or datetime.now(timezone.utc).isoformat()
        
        # Déterminer la plateforme et construire l'URL d'achat si absente
        market_hash_name = opp.get("market_hash_name")
        item_url = opp.get("item_url")
        if not item_url:
            if opp_id.startswith("dmarket_"):
                item_url = f"https://dmarket.com/ingame-items/item-list/csgo-skins?title={market_hash_name.replace(' ', '%20')}"
            elif opp_id.startswith("market_csgo_"):
                item_url = f"https://market.csgo.com/?search={market_hash_name.replace(' ', '%20')}"
            else:
                item_url = f"https://csfloat.com/item/{market_hash_name.replace(' ', '%20')}"

        query = """
        INSERT OR REPLACE INTO opportunities (
            id, timestamp, market_hash_name, price_usd, base_ref_price_usd,
            adjusted_ref_price_usd, discount_percent, net_profit_usd, gross_profit_usd,
            volume_24h, float_value, float_desc, paint_seed, sticker_count,
            sticker_desc, is_premium, item_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(
                    query,
                    (
                        opp_id,
                        timestamp,
                        market_hash_name,
                        opp.get("price_usd", 0.0),
                        opp.get("base_ref_price_usd", 0.0),
                        opp.get("adjusted_ref_price_usd", 0.0),
                        opp.get("discount_percent", 0.0),
                        opp.get("net_profit_usd", 0.0),
                        opp.get("gross_profit_usd", 0.0),
                        opp.get("volume_24h", 0.0),
                        opp.get("float_value"),
                        opp.get("float_desc"),
                        opp.get("paint_seed"),
                        opp.get("sticker_count", 0),
                        opp.get("sticker_desc"),
                        1 if opp.get("is_premium") else 0,
                        item_url
                    )
                )
            logger.info(f"Opportunité enregistrée en DB pour {market_hash_name} : +${opp.get('net_profit_usd', 0.0):.2f}")
            return True
        except Exception as e:
            logger.error(f"Erreur d'enregistrement de l'opportunité {opp_id} : {e}")
            return False
        finally:
            conn.close()

    def save_observed_listing(
        self,
        listing_id: str,
        market_hash_name: str,
        price_cents: int,
        platform: str,
        float_value: Optional[float] = None,
        paint_seed: Optional[int] = None,
        sticker_count: int = 0,
        sticker_names: Optional[List[str]] = None,
        timestamp: Optional[str] = None,
        listed_at: Optional[float] = None,
    ) -> bool:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()
        stickers_json = json.dumps(sticker_names or [])
        query = """
        INSERT OR IGNORE INTO observed_listings (
            listing_id, timestamp, listed_at, market_hash_name, price_cents,
            float_value, paint_seed, sticker_count, sticker_names, platform
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(
                    query,
                    (
                        listing_id,
                        timestamp,
                        listed_at,
                        market_hash_name,
                        price_cents,
                        float_value,
                        paint_seed,
                        sticker_count,
                        stickers_json,
                        platform,
                    ),
                )
            return True
        except Exception as e:
            logger.error(f"Erreur d'enregistrement du listing observé {listing_id} : {e}")
            return False
        finally:
            conn.close()

    def get_pending_observed_listings(self, platform: str) -> List[dict]:
        query = "SELECT * FROM observed_listings WHERE platform = ?;"
        conn = self._get_connection()
        results = []
        try:
            cursor = conn.execute(query, (platform,))
            for row in cursor.fetchall():
                row_dict = dict(row)
                try:
                    row_dict["sticker_names"] = json.loads(row_dict["sticker_names"])
                except Exception:
                    row_dict["sticker_names"] = []
                results.append(row_dict)
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des listings observés de {platform} : {e}")
        finally:
            conn.close()
        return results

    def delete_observed_listings(self, listing_ids: List[str]) -> bool:
        if not listing_ids:
            return True
        placeholders = ",".join(["?"] * len(listing_ids))
        query = f"DELETE FROM observed_listings WHERE listing_id IN ({placeholders});"
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(query, listing_ids)
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la suppression des listings observés : {e}")
            return False
        finally:
            conn.close()

    def clean_old_observed_listings(self, age_seconds: float) -> int:
        conn = self._get_connection()
        count = 0
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
            cutoff_str = cutoff.isoformat()
            query = "DELETE FROM observed_listings WHERE timestamp < ?;"
            with conn:
                cursor = conn.execute(query, (cutoff_str,))
                count = cursor.rowcount
            if count > 0:
                logger.info(f"Nettoyage de {count} listings observés expirés (> {age_seconds/3600:.1f}h).")
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des vieux listings observés : {e}")
        finally:
            conn.close()
        return count
