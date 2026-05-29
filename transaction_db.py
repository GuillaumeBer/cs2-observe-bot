import sqlite3
import os
import json
import logging
import time
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
            confidence TEXT NOT NULL DEFAULT 'LOW',
            ref_price_usd REAL,
            ref_price_confidence TEXT DEFAULT NULL
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
            original_listed_at REAL,
            market_hash_name TEXT NOT NULL,
            price_cents INTEGER NOT NULL,
            float_value REAL,
            paint_seed INTEGER,
            sticker_count INTEGER DEFAULT 0,
            sticker_names TEXT,
            platform TEXT NOT NULL,
            is_target INTEGER NOT NULL DEFAULT 1,
            listed_at_source TEXT
        );
        """
        query_sales = """
        CREATE TABLE IF NOT EXISTS marketplace_sales (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            platform         TEXT    NOT NULL,
            market_hash_name TEXT    NOT NULL,
            float_value      REAL    NOT NULL,
            price_usd        REAL    NOT NULL,
            sale_ts          REAL    NOT NULL,
            fetched_at       REAL    NOT NULL
        );
        """
        query_stats = """
        CREATE TABLE IF NOT EXISTS skin_market_stats (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            week_label       TEXT    NOT NULL,   -- ex: '2026-W22'
            market_hash_name TEXT    NOT NULL,
            platform         TEXT    NOT NULL DEFAULT 'dmarket',
            listing_count    INTEGER DEFAULT 0,  -- nb de listings distincts observés dans la semaine
            avg_price_cents  REAL,
            min_price_cents  INTEGER,
            max_price_cents  INTEGER,
            avg_float        REAL,
            min_float        REAL,
            max_float        REAL,
            is_target        INTEGER DEFAULT 0,  -- 1 si dans les 800 cibles lors de l'agrégation
            created_at       TEXT,
            updated_at       TEXT,
            UNIQUE (week_label, market_hash_name, platform)
        );
        """
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(query_tx)
                conn.execute(query_opp)
                conn.execute(query_observed)
                conn.execute(query_stats)
                conn.execute(query_sales)
                # Index transactions
                conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON transactions (market_hash_name);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_name_float ON transactions (market_hash_name, float_value);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_opp_name ON opportunities (market_hash_name);")
                # Index observed_listings
                conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_name ON observed_listings (market_hash_name);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_platform ON observed_listings (platform);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_listed_at ON observed_listings (listed_at);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_platform_skin ON observed_listings (platform, market_hash_name);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_reconcile ON observed_listings (platform, market_hash_name, float_value, original_listed_at);")
                # Index marketplace_sales
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_dedup ON marketplace_sales (platform, market_hash_name, float_value, sale_ts);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_reconcile ON marketplace_sales (platform, market_hash_name, float_value);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_cleanup ON marketplace_sales (fetched_at);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_week ON skin_market_stats (week_label);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_skin ON skin_market_stats (market_hash_name);")
                # Migrations pour BD existante
                for migration in [
                    "ALTER TABLE transactions ADD COLUMN confidence TEXT NOT NULL DEFAULT 'LOW';",
                    "ALTER TABLE observed_listings ADD COLUMN listed_at REAL;",
                    "ALTER TABLE transactions ADD COLUMN ref_price_usd REAL;",
                    "ALTER TABLE observed_listings ADD COLUMN is_target INTEGER NOT NULL DEFAULT 1;",
                    "ALTER TABLE observed_listings ADD COLUMN listed_at_source TEXT;",
                    "ALTER TABLE observed_listings ADD COLUMN original_listed_at REAL;",
                    "ALTER TABLE marketplace_sales ADD COLUMN reconciled_at REAL;",
                    "ALTER TABLE observed_listings ADD COLUMN suggested_price_cents INTEGER;",
                    "ALTER TABLE transactions ADD COLUMN ref_price_confidence TEXT DEFAULT NULL;",
                ]:
                    try:
                        conn.execute(migration)
                    except Exception:
                        pass
                # Index partiel sur reconciled_at — doit être APRÈS la migration qui ajoute la colonne
                try:
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_unreconciled ON marketplace_sales (reconciled_at) WHERE reconciled_at IS NULL;")
                except Exception:
                    pass
                # Backfill original_listed_at = listed_at pour les rows existantes
                conn.execute("""
                    UPDATE observed_listings
                    SET original_listed_at = listed_at
                    WHERE original_listed_at IS NULL AND listed_at IS NOT NULL
                """)
            logger.info("Base de données initialisée avec succès (tables: transactions, opportunities, observed_listings, skin_market_stats, marketplace_sales).")
        except Exception as e:
            logger.error(f"Erreur d'initialisation de la base de données : {e}")
        finally:
            conn.close()

    def _compute_ref_price(
        self,
        market_hash_name: str,
        before_timestamp: str,
        limit: int = 50,
        suggested_price_cents: Optional[int] = None,
    ) -> tuple[Optional[float], Optional[str]]:
        """
        Prix de référence strict avec confiance (rejette LOW confidence).
        Retourne: (price_usd, confidence_level)

        Hiérarchie:
          HIGH: suggestedPrice DMarket (Steam Market officiel)
          MEDIUM: 10+ ventes marketplace_sales dans 2j, OU 5+ ventes dans 7j
          REJECTED: < 5 ventes, ou fallback à nos transactions
        """
        DAY = 86400.0

        # Source 1 : suggestedPrice DMarket (Steam Market) = HIGH confiance
        if suggested_price_cents and suggested_price_cents > 0:
            return suggested_price_cents / 100.0, "HIGH"

        conn = self._get_connection()
        try:
            before_ts = datetime.fromisoformat(
                before_timestamp.replace("Z", "+00:00")
            ).timestamp()
            cutoff_2d = before_ts - 2 * DAY
            cutoff_7d = before_ts - 7 * DAY

            # Source 2 : marketplace_sales MEDIUM confiance (seuils stricts)
            rows = conn.execute("""
                SELECT price_usd, sale_ts FROM marketplace_sales
                WHERE market_hash_name = ?
                  AND sale_ts < ?
                  AND sale_ts > ?
                ORDER BY sale_ts DESC
            """, (market_hash_name, before_ts, cutoff_7d)).fetchall()

            # Stratégie 2a : 10+ ventes dans 2j = MEDIUM (très bon)
            prices_2d = [r["price_usd"] for r in rows if before_ts - r["sale_ts"] < 2 * DAY]
            if len(prices_2d) >= 10:
                prices_2d.sort()
                n = len(prices_2d)
                median_price = prices_2d[n // 2] if n % 2 else (prices_2d[n // 2 - 1] + prices_2d[n // 2]) / 2.0
                return median_price, "MEDIUM"

            # Stratégie 2b : 5+ ventes dans 7j = MEDIUM (acceptable)
            if len(rows) >= 5:
                prices = [r["price_usd"] for r in rows]
                prices.sort()
                n = len(prices)
                median_price = prices[n // 2] if n % 2 else (prices[n // 2 - 1] + prices[n // 2]) / 2.0
                return median_price, "MEDIUM"

            # Source 3 rejetée : fallback à nos transactions trop peu fiable
            # (return None pour rejeter complètement)
            return None, None

        except Exception as e:
            logger.error(f"Erreur calcul ref_price pour {market_hash_name} : {e}")
            return None, None
        finally:
            conn.close()

        return None

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
        ref_price_usd: Optional[float] = None,
        ref_price_confidence: Optional[str] = None,
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

        if ref_price_usd is None:
            ref_price_usd, ref_price_confidence = self._compute_ref_price(market_hash_name, before_timestamp=timestamp)

        # 1. Vérification anti-doublon : même (float, sale_ts) déjà enregistré ?
        # On compare le sale_ts de la nouvelle transaction avec ceux déjà en base.
        # Un même item peut être vendu plusieurs fois mais jamais au même timestamp exact.
        # Tolérance de 60s pour absorber d'éventuelles variations de représentation.
        if float_value is not None and float_value > 0:
            conn = self._get_connection()
            try:
                sale_ts_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                if sale_ts_dt.tzinfo is None:
                    sale_ts_dt = sale_ts_dt.replace(tzinfo=timezone.utc)
                # Chercher les transactions existantes pour ce (skin, float)
                rows = conn.execute("""
                    SELECT timestamp FROM transactions
                    WHERE market_hash_name = ? AND float_value = ?
                    ORDER BY timestamp DESC LIMIT 10;
                """, (market_hash_name, float_value)).fetchall()
                for row in rows:
                    try:
                        existing_ts = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
                        if existing_ts.tzinfo is None:
                            existing_ts = existing_ts.replace(tzinfo=timezone.utc)
                        delta_s = abs((sale_ts_dt - existing_ts).total_seconds())
                        if delta_s < 60:  # même vente (± 60s de tolérance)
                            logger.warning(
                                f"Transaction ignorée (doublon exact sale_ts) : "
                                f"{market_hash_name} (float: {float_value}) — "
                                f"sale_ts={timestamp} déjà enregistré à {row['timestamp']} (delta={delta_s:.0f}s)"
                            )
                            return False
                    except Exception as parse_err:
                        logger.error(f"Erreur parsing timestamp doublon : {parse_err}")
            except Exception as check_err:
                logger.error(f"Erreur vérification anti-doublon : {check_err}")
            finally:
                conn.close()

        stickers_json = json.dumps(sticker_names or [])

        query = """
        INSERT INTO transactions (
            timestamp, market_hash_name, price_usd, float_value,
            paint_seed, sticker_count, sticker_names, ttd_ms, category, platform, confidence,
            ref_price_usd, ref_price_confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                        ref_price_usd,
                        ref_price_confidence,
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
        listed_at_source: Optional[str] = None,
        is_target: bool = True,
        suggested_price_cents: Optional[int] = None,
    ) -> bool:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()
        stickers_json = json.dumps(sticker_names or [])
        # original_listed_at = listed_at au moment de l'INSERT, jamais mis à jour ensuite
        query = """
        INSERT OR IGNORE INTO observed_listings (
            listing_id, timestamp, listed_at, original_listed_at, market_hash_name, price_cents,
            float_value, paint_seed, sticker_count, sticker_names, platform, is_target,
            listed_at_source, suggested_price_cents
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                        listed_at,  # original_listed_at = listed_at à l'insertion
                        market_hash_name,
                        price_cents,
                        float_value,
                        paint_seed,
                        sticker_count,
                        stickers_json,
                        platform,
                        1 if is_target else 0,
                        listed_at_source,
                        suggested_price_cents,
                    ),
                )
            return True
        except Exception as e:
            logger.error(f"Erreur d'enregistrement du listing observé {listing_id} : {e}")
            return False
        finally:
            conn.close()

    def update_observed_listing_price(
        self,
        listing_id: str,
        price_cents: int,
        listed_at: Optional[float] = None,
        listed_at_source: Optional[str] = None,
    ) -> bool:
        """Met à jour le prix et listed_at d'un listing existant suite à un reprixage détecté."""
        conn = self._get_connection()
        try:
            with conn:
                cursor = conn.execute(
                    "UPDATE observed_listings SET price_cents = ?, listed_at = ?, listed_at_source = ? WHERE listing_id = ?;",
                    (price_cents, listed_at, listed_at_source, listing_id),
                )
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Erreur update_observed_listing_price({listing_id}): {e}")
            return False
        finally:
            conn.close()

    def get_pending_observed_listings(self, platform: str, only_targets: bool = True) -> List[dict]:
        """
        Retourne les listings observés pour une plateforme.
        only_targets=True (défaut) : ne retourne que les skins cibles (is_target=1)
        pour la réconciliation. Passer False pour l'agrégation hors-cible.
        """
        if only_targets:
            query = "SELECT * FROM observed_listings WHERE platform = ? AND is_target = 1;"
        else:
            query = "SELECT * FROM observed_listings WHERE platform = ? AND is_target = 0;"
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

    def get_pending_observed_skin_names(self, platform: str, max_age_seconds: float = 7200.0) -> List[str]:
        """Retourne les market_hash_name distincts hors-cible (is_target=0) ayant au moins un listing
        récent (listed_at ou timestamp dans les max_age_seconds dernières secondes).
        Le filtre de fraîcheur borne le pool à ~max_age_seconds/3600 × taux_accumulation skins,
        évitant une croissance illimitée au fil du temps."""
        import time as _time
        cutoff_ts = _time.time() - max_age_seconds
        cutoff_iso = datetime.fromtimestamp(cutoff_ts, timezone.utc).isoformat()
        conn = self._get_connection()
        try:
            rows = conn.execute(
                "SELECT DISTINCT market_hash_name FROM observed_listings "
                "WHERE platform = ? AND is_target = 0 "
                "AND ((listed_at IS NOT NULL AND listed_at > ?) OR (listed_at IS NULL AND timestamp > ?));",
                (platform, cutoff_ts, cutoff_iso)
            ).fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Erreur get_pending_observed_skin_names({platform}): {e}")
            return []
        finally:
            conn.close()

    def get_pending_observed_listings_for_skin(self, platform: str, market_hash_name: str, is_target: bool = True) -> List[dict]:
        """Retourne les listings observés actifs pour un skin précis (requête indexée).
        is_target=True (défaut) pour les skins cibles (batch), False pour les skins hors-cible (boucle déférée)."""
        query = "SELECT * FROM observed_listings WHERE platform = ? AND market_hash_name = ? AND is_target = ?;"
        conn = self._get_connection()
        results = []
        try:
            cursor = conn.execute(query, (platform, market_hash_name, 1 if is_target else 0))
            for row in cursor.fetchall():
                row_dict = dict(row)
                try:
                    row_dict["sticker_names"] = json.loads(row_dict["sticker_names"])
                except Exception:
                    row_dict["sticker_names"] = []
                results.append(row_dict)
        except Exception as e:
            logger.error(f"Erreur get_pending_observed_listings_for_skin({platform}, {market_hash_name}): {e}")
        finally:
            conn.close()
        return results

    def count_pending_observed_listings(self, platform: str) -> int:
        """Retourne le nombre de listings observés actifs pour une plateforme (sans les charger)."""
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM observed_listings WHERE platform = ? AND is_target = 1;",
                (platform,)
            ).fetchone()
            return row[0] if row else 0
        except Exception as e:
            logger.error(f"Erreur count_pending_observed_listings({platform}): {e}")
            return 0
        finally:
            conn.close()

    def get_observed_listing_by_id(self, listing_id: str) -> Optional[dict]:
        """Retourne un listing observé par son listing_id, ou None s'il n'existe pas."""
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM observed_listings WHERE listing_id = ?;",
                (listing_id,)
            ).fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Erreur get_observed_listing_by_id({listing_id}): {e}")
            return None
        finally:
            conn.close()

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

    def delete_observed_listings_before_timestamp(self, float_value: float, platform: str, max_timestamp_iso: str, market_hash_name: str = None) -> int:
        """
        Supprime tous les listings observés d'un item (même float, plateforme et skin) dont la date de mise en ligne
        est inférieure ou égale à max_timestamp_iso (avec un delta d'erreur de 1.5s inclus).
        """
        if float_value is None or float_value <= 0:
            return 0

        try:
            dt = datetime.fromisoformat(max_timestamp_iso.replace("Z", "+00:00"))
            max_unix = dt.timestamp()
        except Exception:
            max_unix = None

        if max_unix is not None:
            max_iso_adjusted = datetime.fromtimestamp(max_unix + 1.5, timezone.utc).isoformat()
            if market_hash_name:
                query = """
                DELETE FROM observed_listings
                WHERE float_value = ?
                  AND platform = ?
                  AND market_hash_name = ?
                  AND (
                      (listed_at IS NOT NULL AND listed_at <= ?)
                      OR
                      (listed_at IS NULL AND timestamp <= ?)
                  );
                """
                params = (float_value, platform, market_hash_name, max_unix + 1.5, max_iso_adjusted)
            else:
                query = """
                DELETE FROM observed_listings
                WHERE float_value = ?
                  AND platform = ?
                  AND (
                      (listed_at IS NOT NULL AND listed_at <= ?)
                      OR
                      (listed_at IS NULL AND timestamp <= ?)
                  );
                """
                params = (float_value, platform, max_unix + 1.5, max_iso_adjusted)
            conn = self._get_connection()
            try:
                with conn:
                    cursor = conn.execute(query, params)
                    return cursor.rowcount
            except Exception as e:
                logger.error(f"Erreur delete_observed_listings_before_timestamp : {e}")
                return 0
            finally:
                conn.close()
        return 0


    def clean_old_observed_listings(self, age_seconds: float) -> int:
        conn = self._get_connection()
        count = 0
        saved_count = 0
        try:
            now = datetime.now(timezone.utc)
            now_ts = now.timestamp()
            now_str = now.isoformat()
            cutoff_str = (now - timedelta(seconds=age_seconds)).isoformat()
            cutoff_24h_str = (now - timedelta(seconds=86400)).isoformat()

            # Récupérer les listings expirés pour des items ayant un historique de vente
            expired = conn.execute("""
                SELECT ol.* FROM observed_listings ol
                WHERE ol.timestamp < ?
                  AND ol.float_value > 0
                  AND EXISTS (
                      SELECT 1 FROM transactions t
                      WHERE t.market_hash_name = ol.market_hash_name
                      LIMIT 1
                  );
            """, (cutoff_str,)).fetchall()

            with conn:
                for row in expired:
                    r = dict(row)
                    market_hash_name = r["market_hash_name"]
                    float_value = r.get("float_value")
                    if not float_value or float_value <= 0:
                        continue

                    # Calculer le TTD depuis la mise en vente réelle
                    listed_at = r.get("listed_at")
                    try:
                        first_ts = float(listed_at) if listed_at else datetime.fromisoformat(
                            r["timestamp"].replace("Z", "+00:00")
                        ).timestamp()
                    except Exception:
                        continue

                    ttd_ms = (now_ts - first_ts) * 1000

                    # Ignorer si le même float a été enregistré comme vendu dans les 24h
                    already_sold = conn.execute("""
                        SELECT 1 FROM transactions
                        WHERE market_hash_name = ? AND float_value = ? AND timestamp > ?
                        LIMIT 1;
                    """, (market_hash_name, float_value, cutoff_24h_str)).fetchone()
                    if already_sold:
                        continue

                    ref_price_usd, ref_price_confidence = self._compute_ref_price(market_hash_name, before_timestamp=now_str)

                    try:
                        sticker_names = json.dumps(json.loads(r.get("sticker_names") or "[]"))
                    except Exception:
                        sticker_names = "[]"

                    conn.execute("""
                        INSERT INTO transactions (
                            timestamp, market_hash_name, price_usd, float_value,
                            paint_seed, sticker_count, sticker_names, ttd_ms,
                            category, platform, confidence, ref_price_usd, ref_price_confidence
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        now_str,
                        market_hash_name,
                        r["price_cents"] / 100.0,
                        float_value,
                        r.get("paint_seed"),
                        r.get("sticker_count", 0),
                        sticker_names,
                        ttd_ms,
                        "EXPIRED",
                        r["platform"],
                        "LOW",
                        ref_price_usd,
                        ref_price_confidence,
                    ))
                    saved_count += 1

                cursor = conn.execute("DELETE FROM observed_listings WHERE timestamp < ?;", (cutoff_str,))
                count = cursor.rowcount

            if count > 0:
                logger.info(
                    f"Nettoyage de {count} listings expirés (>{age_seconds/3600:.1f}h) "
                    f"— {saved_count} enregistrés comme EXPIRED."
                )
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des vieux listings observés : {e}")
        finally:
            conn.close()
        return count

    def deduplicate_target_listings(self, platform: str) -> int:
        """Pour chaque (market_hash_name, float_value) avec is_target=1 ayant plusieurs
        entrées, supprime toutes sauf la plus récente (par timestamp).
        Retourne le nombre de lignes supprimées."""
        conn = self._get_connection()
        try:
            with conn:
                cursor = conn.execute("""
                    DELETE FROM observed_listings
                    WHERE is_target = 1
                      AND platform = ?
                      AND rowid NOT IN (
                          SELECT MAX(rowid)
                          FROM observed_listings
                          WHERE is_target = 1
                            AND platform = ?
                          GROUP BY market_hash_name, float_value
                      );
                """, (platform, platform))
                count = cursor.rowcount
            if count > 0:
                logger.info(
                    f"Déduplication is_target=1 ({platform}) : {count} doublons supprimés."
                )
            return count
        except Exception as e:
            logger.error(f"Erreur deduplicate_target_listings({platform}): {e}")
            return 0
        finally:
            conn.close()

    def upgrade_waxpeer_low_transactions(self, sales: list, float_eps: float = 1e-4) -> int:
        """Charge toutes les transactions Waxpeer LOW en une seule requête, les matche
        en Python contre les ventes sales-history, puis UPDATE en batch.
        Retourne le nombre de transactions upgradées."""
        if not sales:
            return 0
        conn = self._get_connection()
        upgraded = 0
        try:
            # Charger toutes les tx LOW waxpeer en une fois
            rows = conn.execute("""
                SELECT id, market_hash_name, float_value, timestamp
                FROM transactions
                WHERE platform = 'waxpeer' AND confidence = 'LOW'
            """).fetchall()

            if not rows:
                return 0

            # Index des tx LOW : (name, float_rounded_4) → [(id, ts_unix), ...]
            tx_index: dict = {}
            for r in rows:
                try:
                    ts_unix = datetime.fromisoformat(
                        r["timestamp"].replace("Z", "+00:00")
                    ).timestamp()
                except Exception:
                    ts_unix = 0.0
                key = (r["market_hash_name"], round(r["float_value"], 4))
                tx_index.setdefault(key, []).append({
                    "rowid": r["id"],
                    "ts_unix": ts_unix,
                })

            # Matcher chaque vente en Python
            upgrades: list = []  # (rowid, sale_ts_iso)
            used_rowids: set = set()

            for sale in sales:
                name = sale.get("name", "")
                fv = sale.get("float_value")
                sale_ts = sale.get("sale_ts")
                if not name or not fv or not sale_ts:
                    continue

                # Chercher dans l'index avec float arrondi
                key = (name, round(fv, 4))
                candidates = tx_index.get(key, [])

                best = None
                best_delta = float("inf")
                for c in candidates:
                    if c["rowid"] in used_rowids:
                        continue
                    delta = abs(c["ts_unix"] - sale_ts)
                    if delta < 300 and delta < best_delta:
                        best = c
                        best_delta = delta

                if best:
                    sale_ts_iso = datetime.fromtimestamp(sale_ts, timezone.utc).isoformat()
                    upgrades.append((sale_ts_iso, best["rowid"]))
                    used_rowids.add(best["rowid"])

            # Batch UPDATE
            if upgrades:
                with conn:
                    conn.executemany(
                        "UPDATE transactions SET confidence = 'HIGH', timestamp = ? WHERE rowid = ?",
                        upgrades,
                    )
                upgraded = len(upgrades)

        except Exception as e:
            logger.error(f"Erreur upgrade_waxpeer_low_transactions: {e}")
        finally:
            conn.close()
        return upgraded

    # -------------------------------------------------------------------------
    # Nouvelle architecture : marketplace_sales + réconciliation
    # -------------------------------------------------------------------------

    def insert_marketplace_sales(self, records: list) -> int:
        """Insère en batch les ventes récupérées des APIs. records = liste de tuples
        (platform, market_hash_name, float_value, price_usd, sale_ts, fetched_at).
        Retourne le nombre de nouvelles lignes insérées (doublons ignorés)."""
        if not records:
            return 0
        conn = self._get_connection()
        try:
            # total_changes est cumulatif et fiable pour INSERT OR IGNORE
            # (cursor.rowcount sur executemany compte les tentatives, pas les insertions réelles)
            before = conn.total_changes
            with conn:
                conn.executemany("""
                    INSERT OR IGNORE INTO marketplace_sales
                        (platform, market_hash_name, float_value, price_usd, sale_ts, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, records)
            return conn.total_changes - before
        except Exception as e:
            logger.error(f"Erreur insert_marketplace_sales: {e}")
            return 0
        finally:
            conn.close()

    def reconcile_and_save(self, retention_days: int = 7) -> int:
        """Matche chaque vente dans marketplace_sales avec le listing observé le plus
        récent antérieur à la vente (même platform, skin, float ±1e-4, dans la fenêtre
        de retention_days). Sauvegarde une transaction HIGH confidence pour chaque match
        puis supprime les deux lignes correspondantes. Retourne le nombre de matches."""
        conn = self._get_connection()
        matched = 0
        try:
            max_window = retention_days * 86400
            now_ts = time.time()

            unmatched_count = conn.execute(
                "SELECT COUNT(*) FROM marketplace_sales WHERE reconciled_at IS NULL"
            ).fetchone()[0]
            listings_count = conn.execute(
                "SELECT COUNT(*) FROM observed_listings WHERE float_value IS NOT NULL"
            ).fetchone()[0]
            logger.info(
                f"[RECONCILIATION] Démarrage — ventes non-matchées: {unmatched_count}, "
                f"listings avec float: {listings_count}, fenêtre: {retention_days}j"
            )

            rows = conn.execute("""
                WITH match_candidates AS (
                  SELECT
                    s.id            AS sale_id,
                    l.listing_id,
                    l.market_hash_name,
                    l.float_value,
                    l.price_cents,
                    s.price_usd,
                    s.sale_ts,
                    l.original_listed_at,
                    l.listed_at,
                    l.suggested_price_cents,
                    COALESCE(l.listed_at, l.original_listed_at) AS effective_listed_at,
                    (s.sale_ts - COALESCE(l.listed_at, l.original_listed_at)) * 1000 AS ttd_ms,
                    l.platform,
                    l.paint_seed,
                    l.sticker_count,
                    l.sticker_names,
                    ABS(l.float_value - s.float_value) as float_diff,
                    ABS(l.price_cents - s.price_usd * 100) as price_diff_cents,
                    ROW_NUMBER() OVER (
                      PARTITION BY s.id
                      ORDER BY ABS(l.float_value - s.float_value) ASC, ABS(l.price_cents - s.price_usd * 100) ASC, l.listing_id ASC
                    ) as match_rank
                  FROM marketplace_sales s
                  JOIN observed_listings l ON
                    l.platform = s.platform
                    AND l.market_hash_name = s.market_hash_name
                    AND l.float_value = s.float_value
                    AND l.price_cents / 100.0 = s.price_usd
                    AND l.original_listed_at < s.sale_ts
                    AND l.original_listed_at > s.sale_ts - ?
                    AND COALESCE(l.listed_at, l.original_listed_at) <= s.sale_ts
                  WHERE l.float_value IS NOT NULL
                    AND s.reconciled_at IS NULL
                )
                SELECT
                  sale_id, listing_id, market_hash_name, float_value, price_cents,
                  price_usd, sale_ts, original_listed_at, listed_at, suggested_price_cents,
                  effective_listed_at, ttd_ms, platform, paint_seed, sticker_count, sticker_names
                FROM match_candidates
                WHERE match_rank = 1
                ORDER BY sale_id
            """, (max_window,)).fetchall()

            logger.info(f"[RECONCILIATION] Requête SQL — {len(rows)} matches exacts trouvés (float = float, prix = prix)")

            sale_ids_to_mark = []

            for row in rows:
                ttd_ms = max(0.0, row["ttd_ms"])
                try:
                    sticker_names = json.loads(row["sticker_names"] or "[]")
                except Exception:
                    sticker_names = []

                sale_ts_iso = datetime.fromtimestamp(row["sale_ts"], timezone.utc).isoformat()
                effective_listed_at_iso = datetime.fromtimestamp(row["effective_listed_at"], timezone.utc).isoformat() if row["effective_listed_at"] else "N/A"
                listed_at_source = "listed_at" if row["listed_at"] else "original_listed_at(fallback)"

                if not row["listed_at"]:
                    logger.warning(
                        f"MATCH {row['platform'].upper()}: listed_at NULL pour {row['market_hash_name']} "
                        f"(float={row['float_value']:.6f}) — fallback sur original_listed_at pour TTD"
                    )

                logger.info(
                    f"MATCH {row['platform'].upper()}: {row['market_hash_name']} | "
                    f"float={row['float_value']:.6f} | prix=${row['price_usd']:.2f} | "
                    f"effective_listed_at={effective_listed_at_iso} [{listed_at_source}] | "
                    f"sale_ts={sale_ts_iso} | "
                    f"TTD={ttd_ms/1000:.1f}s [{self._ttd_category(ttd_ms)}]"
                )

                ref_price, ref_price_confidence = self._compute_ref_price(
                    row["market_hash_name"],
                    before_timestamp=sale_ts_iso,
                    suggested_price_cents=row["suggested_price_cents"],
                )

                saved = self.save_transaction(
                    market_hash_name=row["market_hash_name"],
                    price_usd=row["price_usd"],
                    ttd_ms=ttd_ms,
                    platform=row["platform"],
                    category=self._ttd_category(ttd_ms),
                    float_value=row["float_value"],
                    paint_seed=row["paint_seed"],
                    sticker_count=row["sticker_count"] or 0,
                    sticker_names=sticker_names,
                    timestamp=sale_ts_iso,
                    confidence="HIGH",
                    ref_price_usd=ref_price,
                    ref_price_confidence=ref_price_confidence,
                )
                # Marquer la vente comme réconciliée (on ne supprime pas, cleanup 7j s'en charge)
                # Les observed_listings ne sont pas supprimés non plus
                sale_ids_to_mark.append((now_ts, row["sale_id"]))
                if saved:
                    matched += 1

            if sale_ids_to_mark:
                with conn:
                    conn.executemany(
                        "UPDATE marketplace_sales SET reconciled_at = ? WHERE id = ?",
                        sale_ids_to_mark
                    )

            logger.info(
                f"[RECONCILIATION] Terminé — {matched} transactions créées sur {len(rows)} matches"
            )

        except Exception as e:
            logger.error(f"Erreur reconcile_and_save: {e}", exc_info=True)
        finally:
            conn.close()
        return matched

    def _ttd_category(self, ttd_ms: float) -> str:
        bot_ms = getattr(config, "OBS_BOT_SNIPE_TTD_MS", 5000)
        fast_ms = getattr(config, "OBS_FAST_HUMAN_TTD_MS", 60000)
        if ttd_ms < bot_ms:
            return "BOT_SNIPE"
        if ttd_ms < fast_ms:
            return "FAST_HUMAN"
        return "NORMAL_SALE"

    def cleanup_old_marketplace_data(self, days: int = 7) -> tuple:
        """Supprime les ventes et listings plus anciens que days jours.
        Retourne (sales_deleted, listings_deleted)."""
        cutoff = time.time() - days * 86400
        cutoff_iso = datetime.fromtimestamp(cutoff, timezone.utc).isoformat()
        conn = self._get_connection()
        try:
            with conn:
                c1 = conn.execute(
                    "DELETE FROM marketplace_sales WHERE fetched_at < ?", (cutoff,)
                ).rowcount
                c2 = conn.execute(
                    "DELETE FROM observed_listings WHERE timestamp < ?", (cutoff_iso,)
                ).rowcount
            if c1 or c2:
                logger.info(f"Cleanup 7j : {c1} sales supprimées, {c2} listings supprimés.")
            return c1, c2
        except Exception as e:
            logger.error(f"Erreur cleanup_old_marketplace_data: {e}")
            return 0, 0
        finally:
            conn.close()

    def get_active_skin_names_for_sales(self, platform: str, max_age_hours: float = 7 * 24) -> List[str]:
        """Retourne les market_hash_name distincts ayant des listings actifs récents,
        pour piloter la collecte per-skin des sales APIs."""
        cutoff_ts = time.time() - max_age_hours * 3600
        cutoff_iso = datetime.fromtimestamp(cutoff_ts, timezone.utc).isoformat()
        conn = self._get_connection()
        try:
            rows = conn.execute("""
                SELECT DISTINCT market_hash_name FROM observed_listings
                WHERE platform = ?
                  AND ((listed_at IS NOT NULL AND listed_at > ?)
                       OR (listed_at IS NULL AND timestamp > ?))
            """, (platform, cutoff_ts, cutoff_iso)).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            logger.error(f"Erreur get_active_skin_names_for_sales: {e}")
            return []
        finally:
            conn.close()

    # -------------------------------------------------------------------------
    # skin_market_stats — Agrégation hebdomadaire des skins hors-cible
    # -------------------------------------------------------------------------

    def upsert_skin_market_stats(
        self,
        week_label: str,
        market_hash_name: str,
        platform: str,
        listing_count: int,
        avg_price_cents: float,
        min_price_cents: int,
        max_price_cents: int,
        avg_float: Optional[float],
        min_float: Optional[float],
        max_float: Optional[float],
        is_target: bool = False,
    ) -> bool:
        """
        Insère ou met à jour les statistiques agrégées d'un skin pour une semaine donnée.
        Idempotent : si la ligne (week_label, market_hash_name, platform) existe déjà,
        elle est remplacée.
        """
        now_str = datetime.now(timezone.utc).isoformat()
        query = """
        INSERT INTO skin_market_stats (
            week_label, market_hash_name, platform,
            listing_count, avg_price_cents, min_price_cents, max_price_cents,
            avg_float, min_float, max_float,
            is_target, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (week_label, market_hash_name, platform)
        DO UPDATE SET
            listing_count   = excluded.listing_count,
            avg_price_cents = excluded.avg_price_cents,
            min_price_cents = excluded.min_price_cents,
            max_price_cents = excluded.max_price_cents,
            avg_float       = excluded.avg_float,
            min_float       = excluded.min_float,
            max_float       = excluded.max_float,
            is_target       = excluded.is_target,
            updated_at      = excluded.updated_at;
        """
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(query, (
                    week_label, market_hash_name, platform,
                    listing_count, avg_price_cents, min_price_cents, max_price_cents,
                    avg_float, min_float, max_float,
                    1 if is_target else 0,
                    now_str, now_str,
                ))
            return True
        except Exception as e:
            logger.error(f"Erreur upsert skin_market_stats pour {market_hash_name} ({week_label}) : {e}")
            return False
        finally:
            conn.close()

    def get_skin_market_stats(
        self,
        week_label: Optional[str] = None,
        is_target: Optional[bool] = None,
        min_listings: int = 1,
        order_by: str = "listing_count DESC",
        limit: int = 200,
    ) -> List[dict]:
        """
        Retourne les statistiques agrégées, optionnellement filtrées par semaine
        et/ou par statut cible. Triées par défaut par volume décroissant.
        """
        conditions = ["listing_count >= ?"]
        params: list = [min_listings]

        if week_label:
            conditions.append("week_label = ?")
            params.append(week_label)
        if is_target is not None:
            conditions.append("is_target = ?")
            params.append(1 if is_target else 0)

        where = " AND ".join(conditions)
        query = f"""
        SELECT * FROM skin_market_stats
        WHERE {where}
        ORDER BY {order_by}
        LIMIT ?;
        """
        params.append(limit)

        conn = self._get_connection()
        results = []
        try:
            cursor = conn.execute(query, params)
            results = [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Erreur get_skin_market_stats : {e}")
        finally:
            conn.close()
        return results
