"""
observation_ingestion.py — Ingesteur dédié au mode Observation

Interroge les APIs des marketplaces et retourne à chaque cycle un snapshot
COMPLET des listings actifs (sans filtre de prix), pour que MarketObserver
puisse détecter les disparitions.
"""

import asyncio
import logging
import time
import json
from typing import Optional
import aiohttp

from datetime import datetime, timezone
import config
from observer import MarketObserver
from dmarket_ingestion import generate_dmarket_headers
from utils import FIFOUniqueCache

logger = logging.getLogger("cs2_sniper.observation_ingestion")

LIVE_DISPLAY_INTERVAL_SEC = 30


class ObservationIngestor:
    """
    Ingesteur de snapshots complets pour le mode Observation.
    Supporte DMarket, CSFloat, Waxpeer et Market.CSGO.
    """

    def __init__(self, observer: MarketObserver, platform: str = "dmarket"):
        self.observer = observer
        self.platform = platform
        self.is_running = False
        self._tasks: list = []
        self._verification_queue = None
        self._dmarket_cycle = 0
        self._matched_sales_cache = FIFOUniqueCache(maxsize=200)
        
        # Charger les skins cibles
        self.target_skins = set()
        try:
            import os
            # target_skins.json est généralement dans le même dossier ou dans data
            possible_paths = [
                "target_skins.json",
                "data/target_skins.json",
                os.path.join(os.path.dirname(__file__), "target_skins.json"),
                os.path.join(os.path.dirname(__file__), "data", "target_skins.json"),
            ]
            for p in possible_paths:
                if os.path.exists(p):
                    with open(p, "r", encoding="utf-8") as f:
                        self.target_skins = set(json.load(f))
                    logger.info(f"ObservationIngestor : {len(self.target_skins)} skins cibles chargés depuis {p}")
                    break
            if not self.target_skins:
                logger.warning("ObservationIngestor : Aucun skin cible chargé dans target_skins (toutes les insertions seront traitées comme hors-cible ou is_target=True par défaut si le fichier manque ? Non, par défaut is_target=False si non trouvé dans la liste pour économiser l'espace, ou True par sécurité ? Choisissons False si la liste est chargée mais vide, ou True si aucun fichier n'existe du tout pour ne pas tout ignorer. Mettons is_target = (market_hash_name in self.target_skins) si la liste est non vide, sinon True)")
        except Exception as e:
            logger.error(f"Erreur lors du chargement de target_skins.json : {e}")

    async def start(self, session: aiohttp.ClientSession) -> None:
        self.is_running = True
        logger.info(f"Mode Observation démarré sur : {self.platform.upper()}")

        self._verification_queue = asyncio.Queue()
        self._tasks.append(
            asyncio.create_task(self._verification_worker(session))
        )

        if self.platform in ("dmarket", "all"):
            self._tasks.append(
                asyncio.create_task(self._observe_dmarket(session))
            )
            self._tasks.append(
                asyncio.create_task(self._deferred_verification_loop(session))
            )
        if self.platform in ("csfloat", "all"):
            self._tasks.append(
                asyncio.create_task(self._observe_csfloat(session))
            )
            self._tasks.append(
                asyncio.create_task(self._deferred_csfloat_verification_loop(session))
            )
        if self.platform in ("waxpeer", "all"):
            self._tasks.append(
                asyncio.create_task(self._observe_waxpeer(session))
            )
        if self.platform in ("market_csgo", "all") and config.MARKET_CSGO_ENABLED:
            self._tasks.append(
                asyncio.create_task(self._observe_market_csgo(session))
            )
            self._tasks.append(
                asyncio.create_task(self._deferred_market_csgo_reconciliation_loop(session))
            )
        if self.platform in ("skinport", "all") and config.SKINPORT_ENABLED:
            self._tasks.append(
                asyncio.create_task(self._observe_skinport(session))
            )

        self._tasks.append(
            asyncio.create_task(self._display_loop())
        )

    async def stop(self) -> None:
        self.is_running = False
        for task in self._tasks:
            task.cancel()
        logger.info("Mode Observation arrêté. Export des résultats finaux...")
        self.observer.export_results()
        self.observer.print_live_stats(top_n=20)

    # ──────────────────────────────────────────────────────────────────────────
    # BOUCLE D'OBSERVATION DMARKET
    # ──────────────────────────────────────────────────────────────────────────

    async def _observe_dmarket(self, session: aiohttp.ClientSession) -> None:
        poll_interval = config.OBS_POLL_INTERVAL_MS / 1000.0
        base_url = "https://api.dmarket.com"

        logger.info(f"DMarket Observation : polling par date de création toutes les {poll_interval}s")

        processed_ids = FIFOUniqueCache(maxsize=10000)
        warmup = True

        while self.is_running:
            cycle_start = time.perf_counter()
            try:
                min_price_cents = int(config.MIN_PRICE_USD * 100)
                max_price_cents = int(config.MAX_PRICE_USD * 100)
                path = f"/exchange/v1/market/items?gameId=a8db&limit=100&orderBy=updated&orderDir=desc&currency=USD&priceFrom={min_price_cents}&priceTo={max_price_cents}"
                headers = generate_dmarket_headers(
                    config.DMARKET_PUBLIC_KEY,
                    config.DMARKET_SECRET_KEY,
                    "GET",
                    path,
                )
                url = base_url + path

                async with session.get(url, headers=headers, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                        page_items = data.get("objects", [])

                        new_count = 0
                        for item in page_items:
                            norm = self._normalize_dmarket(item)
                            if not norm:
                                continue

                            listing_id = norm["id"]
                            if processed_ids.add(listing_id):
                                if warmup:
                                    continue
                                if not norm.get("float_value") or norm["float_value"] <= 0:
                                    continue

                                is_tgt = (norm["market_hash_name"] in self.target_skins) if self.target_skins else True
                                self.observer._db.save_observed_listing(
                                    listing_id=listing_id,
                                    market_hash_name=norm["market_hash_name"],
                                    price_cents=norm["price"],
                                    platform="dmarket",
                                    float_value=norm.get("float_value"),
                                    paint_seed=norm.get("paint_seed"),
                                    sticker_count=norm.get("sticker_count", 0),
                                    sticker_names=norm.get("sticker_names", []),
                                    timestamp=datetime.now(timezone.utc).isoformat(),
                                    listed_at=norm.get("listed_at"),
                                    listed_at_source=norm.get("listed_at_source"),
                                    is_target=is_tgt,
                                )
                                new_count += 1

                        if warmup:
                            logger.info(f"DMarket : {len(page_items)} IDs initialisés (warmup terminé).")
                            warmup = False
                        elif new_count > 0:
                            logger.debug(f"DMarket : {new_count} nouveaux listings enregistrés.")

                    elif response.status == 429:
                        retry_after = 30
                        try:
                            retry_after = int(response.headers.get("Retry-After", 30))
                        except (ValueError, TypeError):
                            pass
                        logger.warning(f"DMarket : Rate limited — pause de {retry_after}s")
                        await asyncio.sleep(retry_after + 2)
                    else:
                        logger.warning(f"DMarket : HTTP {response.status}")

            except asyncio.TimeoutError:
                logger.warning("DMarket : Timeout de connexion")
            except Exception as e:
                logger.error(f"DMarket observation error : {e}")

            elapsed = time.perf_counter() - cycle_start
            sleep_time = max(0.0, poll_interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def _deferred_verification_loop(self, session: aiohttp.ClientSession) -> None:
        await asyncio.sleep(30)

        import urllib.parse
        import yarl

        while self.is_running:
            try:
                start_time = time.perf_counter()
                logger.info("Début de la boucle de réconciliation différée DMarket...")

                self.observer._db.clean_old_observed_listings(config.OBSERVER_MAX_TTD_SEC)

                # Option A : récupérer uniquement la liste des skins distincts (pas tous les listings)
                skin_names = self.observer._db.get_pending_observed_skin_names("dmarket")
                if not skin_names:
                    logger.debug("Aucun listing DMarket en attente de réconciliation.")
                    await asyncio.sleep(120)
                    continue

                logger.info(f"Réconciliation DMarket : {len(skin_names)} skins avec listings actifs...")

                matched_count = 0

                for skin_name in skin_names:
                    if not self.is_running:
                        break

                    await asyncio.sleep(0.5)

                    # Option A : charger uniquement les listings de ce skin (requête indexée)
                    listings = self.observer._db.get_pending_observed_listings_for_skin("dmarket", skin_name)
                    if not listings:
                        continue

                    encoded_title = urllib.parse.quote(skin_name)
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
                        async with session.get(url, headers=headers, timeout=5) as response:
                            if response.status == 200:
                                data = await response.json()
                                sales = data.get("sales") or []

                                server_date_str = response.headers.get("Date")
                                if server_date_str:
                                    try:
                                        from email.utils import parsedate_to_datetime
                                        server_ts = parsedate_to_datetime(server_date_str).timestamp()
                                        self._dmarket_clock_offset = time.time() - server_ts
                                    except Exception:
                                        pass

                                if not sales:
                                    continue

                                for sale in sales:
                                    sale_price = 0.0
                                    raw_price = sale.get("price")
                                    if isinstance(raw_price, str):
                                        try:
                                            sale_price = float(raw_price)
                                        except ValueError:
                                            continue
                                    elif isinstance(raw_price, dict):
                                        amt = raw_price.get("amount") or raw_price.get("USD") or 0.0
                                        try:
                                            sale_price = float(amt) if "." in str(amt) else float(amt) / 100.0
                                        except ValueError:
                                            continue
                                    else:
                                        continue

                                    raw_date = sale.get("date")
                                    try:
                                        sale_ts = float(raw_date)
                                    except (ValueError, TypeError):
                                        continue

                                    opp_attrs = sale.get("offerAttributes") or {}
                                    sale_float = opp_attrs.get("floatValue")
                                    if sale_float is not None:
                                        try:
                                            sale_float = float(sale_float)
                                        except ValueError:
                                            sale_float = None

                                    best_match = None
                                    best_time_diff = float('inf')

                                    for item in list(listings):
                                        item_price_usd = item["price_cents"] / 100.0
                                        if abs(item_price_usd - sale_price) >= 0.01:
                                            continue

                                        if sale_float is not None and item["float_value"] is not None:
                                            if abs(item["float_value"] - sale_float) >= 1e-5:
                                                continue

                                        if item.get("listed_at") is not None:
                                            l_ts = float(item["listed_at"])
                                            t_src = item.get("listed_at_source") or "unknown"
                                        else:
                                            try:
                                                listed_dt = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                                                clock_offset = getattr(self, "_dmarket_clock_offset", 0.0)
                                                l_ts = listed_dt.timestamp() - clock_offset
                                                t_src = "observation"
                                            except Exception:
                                                continue

                                        if l_ts - 1.5 > sale_ts:
                                            continue

                                        if sale_ts - l_ts > config.OBSERVER_MAX_TTD_SEC:
                                            continue

                                        time_diff = sale_ts - l_ts
                                        if time_diff < best_time_diff:
                                            best_time_diff = time_diff
                                            best_match = (item, l_ts, t_src)

                                    if best_match is not None:
                                        item, listed_ts, ttd_source = best_match
                                        sale_sig = f"dmarket_deferred_{skin_name}_{sale_price:.2f}_{sale_ts:.3f}"
                                        if self._matched_sales_cache.add(sale_sig):
                                            ttd_ms = max(0.0, (sale_ts - listed_ts) * 1000)

                                            if ttd_ms < config.OBS_BOT_SNIPE_TTD_MS:
                                                category = "BOT_SNIPE"
                                            elif ttd_ms < config.OBS_FAST_HUMAN_TTD_MS:
                                                category = "FAST_HUMAN"
                                            else:
                                                category = "NORMAL_SALE"

                                            try:
                                                sticker_names = json.loads(item["sticker_names"])
                                            except Exception:
                                                sticker_names = []

                                            # P3 : confidence basée sur listed_at_source
                                            listed_at_source = item.get("listed_at_source")
                                            confidence = "HIGH" if listed_at_source == "createdAt" else "MEDIUM"

                                            item_price_usd = item["price_cents"] / 100.0
                                            sale_ts_iso = datetime.fromtimestamp(sale_ts, timezone.utc).isoformat().replace("+00:00", "Z")

                                            saved = self.observer._db.save_transaction(
                                                market_hash_name=skin_name,
                                                price_usd=item_price_usd,
                                                ttd_ms=ttd_ms,
                                                platform="dmarket",
                                                category=category,
                                                float_value=item["float_value"],
                                                paint_seed=item["paint_seed"],
                                                sticker_count=item["sticker_count"],
                                                sticker_names=sticker_names,
                                                timestamp=sale_ts_iso,
                                                confidence=confidence,
                                            )

                                            # P2 : log et nettoyage seulement si le save a réussi
                                            if saved:
                                                logger.info(
                                                    f"MATCH RÉCONCILIÉ DMarket: {skin_name} | "
                                                    f"Prix: ${item_price_usd:.2f} | "
                                                    f"TTD: {ttd_ms/1000:.1f}s [{ttd_source}] | "
                                                    f"Catégorie: {category} | Confidence: {confidence}"
                                                )
                                                self.observer._db.delete_observed_listings_before_timestamp(
                                                    float_value=item["float_value"],
                                                    platform="dmarket",
                                                    max_timestamp_iso=sale_ts_iso
                                                )
                                                listings[:] = [l for l in listings if abs(l["float_value"] - item["float_value"]) >= 1e-5]
                                                matched_count += 1
                                            break

                            elif response.status == 429:
                                logger.warning("DMarket last-sales rate limited. Pause 10s.")
                                await asyncio.sleep(10)
                            else:
                                logger.warning(f"DMarket last-sales HTTP {response.status} pour {skin_name}")
                    except Exception as skin_err:
                        logger.error(f"Erreur de réconciliation pour {skin_name} : {skin_err}")

                elapsed = time.perf_counter() - start_time
                logger.info(f"Fin de la boucle de réconciliation. {matched_count} ventes réconciliées en {elapsed:.1f}s.")

            except Exception as loop_err:
                logger.error(f"Erreur dans la boucle de réconciliation : {loop_err}")

            await asyncio.sleep(120)

    async def _deferred_csfloat_verification_loop(self, session: aiohttp.ClientSession) -> None:
        await asyncio.sleep(45)

        while self.is_running:
            try:
                start_time = time.perf_counter()
                logger.info("Début de la boucle de réconciliation différée CSFloat...")

                self.observer._db.clean_old_observed_listings(config.OBSERVER_MAX_TTD_SEC)

                pending = self.observer._db.get_pending_observed_listings("csfloat")
                if not pending:
                    logger.debug("Aucun listing CSFloat en attente de réconciliation.")
                    await asyncio.sleep(120)
                    continue

                by_skin = {}
                for p in pending:
                    by_skin.setdefault(p["market_hash_name"], []).append(p)

                for skin_name in by_skin:
                    by_skin[skin_name].sort(
                        key=lambda x: x["listed_at"] if x.get("listed_at") is not None else datetime.fromisoformat(x["timestamp"].replace("Z", "+00:00")).timestamp(),
                        reverse=True
                    )

                logger.info(f"Réconciliation CSFloat : {len(pending)} listings sur {len(by_skin)} skins...")

                matched_count = 0
                import urllib.parse

                for skin_name, listings in by_skin.items():
                    if not self.is_running:
                        break

                    await asyncio.sleep(0.5)

                    encoded_name = urllib.parse.quote(skin_name, safe="")
                    url = f"https://csfloat.com/api/v1/history/{encoded_name}/sales"
                    headers = {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                        )
                    }
                    if config.CSFLOAT_API_KEY:
                        headers["Authorization"] = config.CSFLOAT_API_KEY

                    try:
                        async with session.get(url, headers=headers, timeout=5) as response:
                            if response.status == 200:
                                sales = await response.json()
                                if not sales:
                                    continue

                                server_date_str = response.headers.get("Date")
                                if server_date_str:
                                    try:
                                        from email.utils import parsedate_to_datetime
                                        server_ts = parsedate_to_datetime(server_date_str).timestamp()
                                        self._csfloat_clock_offset = time.time() - server_ts
                                    except Exception:
                                        pass

                                for sale in sales:
                                    sale_price = float(sale.get("price", 0)) / 100.0
                                    raw_date = sale.get("created_at") or sale.get("sold_at")
                                    if not raw_date:
                                        continue
                                    try:
                                        dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                                        sale_ts = dt.timestamp()
                                    except Exception:
                                        continue

                                    sale_float = sale.get("item", {}).get("float_value")
                                    if not sale_float or sale_float <= 0:
                                        continue

                                    for item in list(listings):
                                        item_price_usd = item["price_cents"] / 100.0
                                        if abs(item_price_usd - sale_price) >= 0.01:
                                            continue

                                        if item.get("listed_at") is not None:
                                            listed_ts = float(item["listed_at"])
                                            ttd_source = "createdAt"
                                        else:
                                            try:
                                                listed_dt = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                                                clock_offset = getattr(self, "_csfloat_clock_offset", 0.0)
                                                listed_ts = listed_dt.timestamp() - clock_offset
                                                ttd_source = "observation"
                                            except Exception:
                                                continue

                                        if not (listed_ts - 10.0 <= sale_ts <= listed_ts + config.OBSERVER_MAX_TTD_SEC):
                                            continue

                                        if abs(item["float_value"] - sale_float) >= 1e-5:
                                            continue

                                        sale_sig = f"csfloat_deferred_{skin_name}_{sale_price:.2f}_{sale_ts:.3f}"
                                        if self._matched_sales_cache.add(sale_sig):
                                            ttd_ms = max(0.0, (sale_ts - listed_ts) * 1000)

                                            if ttd_ms < config.OBS_BOT_SNIPE_TTD_MS:
                                                category = "BOT_SNIPE"
                                            elif ttd_ms < config.OBS_FAST_HUMAN_TTD_MS:
                                                category = "FAST_HUMAN"
                                            else:
                                                category = "NORMAL_SALE"

                                            try:
                                                sticker_names = json.loads(item["sticker_names"])
                                            except Exception:
                                                sticker_names = []

                                            logger.info(
                                                f"MATCH RÉCONCILIÉ CSFloat: {skin_name} | "
                                                f"Prix: ${item_price_usd:.2f} | "
                                                f"TTD: {ttd_ms/1000:.1f}s [{ttd_source}] | "
                                                f"Catégorie: {category} | Confidence: HIGH"
                                            )

                                            self.observer._db.save_transaction(
                                                market_hash_name=skin_name,
                                                price_usd=item_price_usd,
                                                ttd_ms=ttd_ms,
                                                platform="csfloat",
                                                category=category,
                                                float_value=item["float_value"],
                                                paint_seed=item["paint_seed"],
                                                sticker_count=item["sticker_count"],
                                                sticker_names=sticker_names,
                                                timestamp=datetime.fromtimestamp(sale_ts, timezone.utc).isoformat(),
                                                confidence="HIGH",
                                            )

                                            # Au lieu de supprimer juste un listing_id, on supprime TOUTES les entrées de cet item
                                            # antérieures à la date de vente (avec le delta d'erreur de 1.5s géré par la DB).
                                            sale_ts_iso = datetime.fromtimestamp(sale_ts, timezone.utc).isoformat()
                                            self.observer._db.delete_observed_listings_before_timestamp(
                                                float_value=item["float_value"],
                                                platform="csfloat",
                                                max_timestamp_iso=sale_ts_iso
                                            )
                                            # On nettoie listings localement
                                            listings[:] = [l for l in listings if abs(l["float_value"] - item["float_value"]) >= 1e-5]
                                            matched_count += 1
                                            break
                            elif response.status == 429:
                                logger.warning("CSFloat last-sales rate limited. Pause 10s.")
                                await asyncio.sleep(10)
                            else:
                                logger.warning(f"CSFloat last-sales HTTP {response.status} pour {skin_name}")
                    except Exception as skin_err:
                        logger.error(f"Erreur de réconciliation CSFloat pour {skin_name} : {skin_err}")

                elapsed = time.perf_counter() - start_time
                logger.info(f"Fin de la boucle de réconciliation CSFloat. {matched_count} ventes réconciliées en {elapsed:.1f}s.")

            except Exception as loop_err:
                logger.error(f"Erreur dans la boucle de réconciliation CSFloat : {loop_err}")

            await asyncio.sleep(120)

    def _normalize_dmarket(self, raw: dict) -> dict:
        try:
            price_usd_str = raw.get("price", {}).get("USD", "0")
            if "." in price_usd_str:
                price_cents = int(round(float(price_usd_str) * 100))
            else:
                price_cents = int(price_usd_str)
        except (ValueError, TypeError):
            price_cents = 0

        extra = raw.get("extra") or {}
        offer_id = raw.get("offerId") or extra.get("offerId", "")
        item_id = raw.get("itemId", "")

        raw_ts = raw.get("createdAt") or extra.get("createdAt")
        listed_at_source = "createdAt" if raw_ts else None
        if not raw_ts:
            raw_ts = raw.get("updatedAt") or extra.get("updatedAt")
            listed_at_source = "updatedAt" if raw_ts else None
        try:
            listed_at = float(raw_ts) if raw_ts is not None else None
        except (ValueError, TypeError):
            listed_at = None

        raw_float = extra.get("floatValue") or extra.get("floatPartValue")
        if raw_float is not None:
            try:
                float_value = float(raw_float)
            except ValueError:
                float_value = None
        else:
            float_value = None

        paint_seed = extra.get("paintSeed")
        dmarket_stickers = extra.get("stickers") or []
        stickers = []
        if isinstance(dmarket_stickers, list):
            for i, s in enumerate(dmarket_stickers):
                sticker_name = s.get("name")
                if not sticker_name:
                    continue
                sticker_price_usd = s.get("price") or s.get("value") or 0.0
                try:
                    sticker_value_cents = int(round(float(sticker_price_usd) * 100))
                except ValueError:
                    sticker_value_cents = 0

                stickers.append({
                    "name": sticker_name,
                    "wear": float(s.get("wear") or 0.0),
                    "value": sticker_value_cents,
                    "slot": s.get("slot") or i
                })

        cheapest_by_sa = extra.get("cheapestBySteamAnalyst") or False

        return {
            "id": offer_id or item_id,
            "offer_id": offer_id or item_id,
            "market_hash_name": raw.get("title", ""),
            "price": price_cents,
            "listed_at": listed_at,
            "listed_at_source": listed_at_source,
            "float_value": float_value,
            "paint_seed": paint_seed,
            "sticker_count": len(stickers),
            "sticker_names": [s["name"] for s in stickers],
            "stickers": stickers,
            "cheapest_by_sa": cheapest_by_sa,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # BOUCLE D'OBSERVATION CSFLOAT
    # ──────────────────────────────────────────────────────────────────────────

    async def _observe_csfloat(self, session: aiohttp.ClientSession) -> None:
        if not config.CSFLOAT_API_KEY:
            logger.warning("CSFloat : Clé API non configurée — plateforme désactivée.")
            return

        poll_interval = config.OBS_POLL_INTERVAL_MS / 1000.0
        min_price_cents = int(config.MIN_PRICE_USD * 100)
        max_price_cents = int(config.MAX_PRICE_USD * 100)
        url = f"https://csfloat.com/api/v1/listings?limit=50&sort_by=created_at_desc&min_price={min_price_cents}&max_price={max_price_cents}"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Authorization": config.CSFLOAT_API_KEY,
        }

        logger.info(f"CSFloat Observation : polling toutes les {poll_interval}s")

        processed_ids = FIFOUniqueCache(maxsize=10000)
        warmup = True

        while self.is_running:
            cycle_start = time.perf_counter()
            try:
                async with session.get(url, headers=headers, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                        raw_listings = data.get("data", []) or data
                        if not isinstance(raw_listings, list):
                            raw_listings = []

                        new_count = 0
                        for raw_item in raw_listings:
                            norm = self._normalize_csfloat(raw_item)
                            if not norm:
                                continue

                            listing_id = norm["id"]
                            if processed_ids.add(listing_id):
                                if warmup:
                                    continue

                                is_tgt = (norm["market_hash_name"] in self.target_skins) if self.target_skins else True
                                self.observer._db.save_observed_listing(
                                    listing_id=listing_id,
                                    market_hash_name=norm["market_hash_name"],
                                    price_cents=norm["price"],
                                    platform="csfloat",
                                    float_value=norm.get("float_value"),
                                    paint_seed=norm.get("paint_seed"),
                                    sticker_count=norm.get("sticker_count", 0),
                                    sticker_names=norm.get("sticker_names", []),
                                    timestamp=datetime.now(timezone.utc).isoformat(),
                                    listed_at=norm.get("listed_at"),
                                    is_target=is_tgt,
                                )
                                new_count += 1

                        if warmup:
                            logger.info(f"CSFloat : {len(raw_listings)} IDs initialisés (warmup terminé).")
                            warmup = False
                        elif new_count > 0:
                            logger.debug(f"CSFloat : {new_count} nouveaux listings enregistrés.")

                    elif response.status == 403:
                        logger.error("CSFloat : HTTP 403 — Clé API invalide.")
                        await asyncio.sleep(60)
                    elif response.status == 429:
                        logger.warning("CSFloat : Rate limited. Pause 10s.")
                        await asyncio.sleep(10)
                    else:
                        logger.warning(f"CSFloat : HTTP {response.status}")

            except asyncio.TimeoutError:
                logger.warning("CSFloat : Timeout de connexion")
            except Exception as e:
                logger.error(f"CSFloat observation error : {e}")

            elapsed = time.perf_counter() - cycle_start
            sleep_time = max(0.0, poll_interval - elapsed)
            await asyncio.sleep(sleep_time)

    def _normalize_csfloat(self, raw: dict) -> Optional[dict]:
        item = raw.get("item") or {}
        name = item.get("market_hash_name", "")
        price = raw.get("price", 0)

        float_value = item.get("float_value")
        if float_value is None:
            return None
        try:
            float_value = float(float_value)
        except ValueError:
            return None

        if float_value <= 0:
            return None

        raw_stickers = item.get("stickers") or []
        stickers = []
        for i, s in enumerate(raw_stickers):
            if isinstance(s, dict):
                stickers.append({
                    "name": s.get("name", ""),
                    "wear": float(s.get("wear") or 0.0),
                    "value": 0,
                    "slot": s.get("slot") or i
                })

        created_at_str = raw.get("created_at")
        listed_at = None
        if created_at_str:
            try:
                dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                listed_at = dt.timestamp()
            except Exception:
                pass

        return {
            "id": str(raw.get("id", "")),
            "market_hash_name": name,
            "price": price,
            "listed_at": listed_at,
            "float_value": float_value,
            "paint_seed": item.get("paint_seed"),
            "sticker_count": len(stickers),
            "sticker_names": [s["name"] for s in stickers],
        }

    # ──────────────────────────────────────────────────────────────────────────
    # AFFICHAGE LIVE PÉRIODIQUE
    # ──────────────────────────────────────────────────────────────────────────

    async def _display_loop(self) -> None:
        while self.is_running:
            await asyncio.sleep(LIVE_DISPLAY_INTERVAL_SEC)
            if self.is_running:
                self.observer.print_live_stats()

    # ──────────────────────────────────────────────────────────────────────────
    # LOGIQUE DE DOUBLE-VÉRIFICATION
    # ──────────────────────────────────────────────────────────────────────────

    def _enqueue_candidates(self, candidates: list, platform: str) -> None:
        for c in candidates:
            ttd = c.get("ttd_max_ms", 999999)
            max_ttd_ms = config.OBSERVER_MAX_TTD_SEC * 1000

            if ttd < max_ttd_ms:
                c["_attempts"] = 0
                self._verification_queue.put_nowait((c, platform))
            else:
                self.observer.confirm_disappearance(c["listing_id"], is_still_active=True)

    async def _verification_worker(self, session: aiohttp.ClientSession) -> None:
        last_request_time = 0.0

        while self.is_running:
            try:
                candidate_data = await self._verification_queue.get()
                c, platform = candidate_data

                attempts = c.get("_attempts", 0)
                if attempts >= 3:
                    logger.warning(f"Verification abandonnée après {attempts} essais pour {c['name']} ({c['listing_id']})")
                    self.observer.confirm_disappearance(c["listing_id"], is_still_active=True)
                    self._verification_queue.task_done()
                    continue

                delay = 0.5
                now = time.perf_counter()
                elapsed = now - last_request_time
                if elapsed < delay:
                    await asyncio.sleep(delay - elapsed)

                listing_id = c["listing_id"]
                status_code = 200
                is_still_active = True

                try:
                    last_request_time = time.perf_counter()
                    if platform == "dmarket":
                        status_code, is_still_active = await self._verify_dmarket_listing(
                            c, session
                        )
                    elif platform == "csfloat":
                        status_code, is_still_active = await self._verify_csfloat_listing(
                            c, session
                        )
                    elif platform == "waxpeer":
                        status_code, is_still_active = await self._verify_waxpeer_listing(
                            c, session
                        )
                    elif platform == "market_csgo":
                        status_code, is_still_active = await self._verify_market_csgo_listing(
                            c, session
                        )
                        if status_code == 200 and is_still_active:
                            logger.info(
                                f"Market.CSGO: Vente non trouvée immédiatement pour {c.get('name')} "
                                f"({c.get('listing_id')}). Planification d'une vérification différée dans 75 minutes."
                            )
                            asyncio.create_task(self._run_delayed_market_csgo_verification(c, session))
                            self._verification_queue.task_done()
                            continue
                except Exception as e:
                    logger.error(f"Worker : exception pour {listing_id} ({platform}): {e}")
                    status_code = 500
                    is_still_active = True

                if status_code == 429:
                    c["_attempts"] = attempts + 1
                    logger.warning(f"Rate limited pour {c['name']} sur {platform}. Pause 10s.")
                    await asyncio.sleep(10)
                    await self._verification_queue.put((c, platform))
                else:
                    self.observer.confirm_disappearance(listing_id, is_still_active)

                self._verification_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erreur dans la boucle du worker de vérification : {e}")
                await asyncio.sleep(1)

    async def _verify_dmarket_listing(self, candidate: dict, session: aiohttp.ClientSession) -> tuple[int, bool]:
        name = candidate.get("name") or candidate.get("market_hash_name", "")
        price_cents = candidate.get("price_cents") or candidate.get("price", 0)
        listing_id = candidate.get("listing_id") or candidate.get("id", "")
        first_seen = candidate.get("first_seen_ts") or time.time()
        absent_seen = candidate.get("absent_first_seen_ts") or time.time()

        if not name or not price_cents:
            return 400, True

        import urllib.parse
        import yarl
        encoded_title = urllib.parse.quote(name)
        url_str = f"https://api.dmarket.com/trade-aggregator/v1/last-sales?title={encoded_title}&gameId=a8db&limit=20"
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
            async with session.get(url, headers=headers, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    sales = data.get("sales") or []
                    
                    server_date_str = response.headers.get("Date")
                    clock_offset = 0.0
                    if server_date_str:
                        try:
                            from email.utils import parsedate_to_datetime
                            server_ts = parsedate_to_datetime(server_date_str).timestamp()
                            clock_offset = time.time() - server_ts
                        except Exception:
                            pass
                    
                    dmarket_first_seen = first_seen - clock_offset
                    dmarket_absent_seen = absent_seen - clock_offset
                    candidate_price_usd = price_cents / 100.0
                    
                    for sale in sales:
                        sale_price = 0.0
                        raw_price = sale.get("price")
                        if isinstance(raw_price, str):
                            try:
                                sale_price = float(raw_price)
                            except ValueError:
                                continue
                        elif isinstance(raw_price, dict):
                            amt = raw_price.get("amount") or raw_price.get("USD") or 0.0
                            try:
                                sale_price = float(amt) if "." in str(amt) else float(amt) / 100.0
                            except ValueError:
                                continue
                        else:
                            continue
                            
                        raw_date = sale.get("date")
                        try:
                            sale_date = float(raw_date)
                        except (ValueError, TypeError):
                            continue
                            
                        price_match = abs(sale_price - candidate_price_usd) < 0.01
                        time_match = (dmarket_first_seen - 10.0) <= sale_date <= (dmarket_absent_seen + 10.0)
                        
                        if price_match and time_match:
                            sale_sig = f"{name}_{sale_price:.2f}_{sale_date:.3f}"
                            if self._matched_sales_cache.add(sale_sig):
                                logger.info(
                                    f"Match DMarket last-sales trouvé pour {name} : "
                                    f"prix_sale={sale_price:.2f}$, date={sale_date}"
                                )
                                return 200, False
                            else:
                                logger.debug(f"Transaction déjà consommée : {sale_sig}")
                            
                    return 200, True
                elif response.status == 429:
                    return 429, True
                else:
                    logger.warning(f"DMarket last-sales error for {listing_id}: HTTP {response.status}")
                    return response.status, True
        except Exception as e:
            logger.exception(f"DMarket last-sales exception for {listing_id}")
            return 500, True

    async def _verify_csfloat_listing(self, candidate: dict, session: aiohttp.ClientSession) -> tuple[int, bool]:
        name = candidate.get("name") or candidate.get("market_hash_name", "")
        price_cents = candidate.get("price_cents") or candidate.get("price", 0)
        listing_id = candidate.get("listing_id") or candidate.get("id", "")
        first_seen = candidate.get("first_seen_ts") or time.time()
        absent_seen = candidate.get("absent_first_seen_ts") or time.time()

        if not name or not price_cents:
            return 400, True

        import urllib.parse
        encoded_name = urllib.parse.quote(name, safe="")
        url = f"https://csfloat.com/api/v1/history/{encoded_name}/sales"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
        if config.CSFLOAT_API_KEY:
            headers["Authorization"] = config.CSFLOAT_API_KEY

        try:
            async with session.get(url, headers=headers, timeout=5) as response:
                if response.status == 200:
                    sales = await response.json()
                    
                    server_date_str = response.headers.get("Date")
                    clock_offset = 0.0
                    if server_date_str:
                        try:
                            from email.utils import parsedate_to_datetime
                            server_ts = parsedate_to_datetime(server_date_str).timestamp()
                            clock_offset = time.time() - server_ts
                        except Exception:
                            pass
                    
                    csfloat_first_seen = first_seen - clock_offset
                    csfloat_absent_seen = absent_seen - clock_offset
                    candidate_price_usd = price_cents / 100.0
                    
                    for sale in sales:
                        sale_price = float(sale.get("price", 0)) / 100.0
                        
                        raw_date = sale.get("created_at") or sale.get("sold_at")
                        if not raw_date:
                            continue
                        try:
                            from datetime import datetime
                            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                            sale_date = dt.timestamp()
                        except Exception:
                            continue
                            
                        price_match = abs(sale_price - candidate_price_usd) < 0.01
                        time_match = (csfloat_first_seen - 10.0) <= sale_date <= (csfloat_absent_seen + 10.0)
                        
                        if price_match and time_match:
                            sale_sig = f"csfloat_{name}_{sale_price:.2f}_{sale_date:.3f}"
                            if self._matched_sales_cache.add(sale_sig):
                                logger.info(
                                    f"Match CSFloat history trouvé pour {name} : "
                                    f"prix_sale={sale_price:.2f}$, date={sale_date}"
                                )
                                return 200, False
                            else:
                                logger.debug(f"Transaction CSFloat déjà consommée : {sale_sig}")
                                
                    return 200, True
                elif response.status == 429:
                    return 429, True
                else:
                    logger.warning(f"CSFloat history error for {listing_id}: HTTP {response.status}")
                    return response.status, True
        except Exception as e:
            logger.error(f"CSFloat history exception for {listing_id}: {e}")
            return 500, True

    async def _verify_waxpeer_listing(self, candidate: dict, session: aiohttp.ClientSession) -> tuple[int, bool]:
        name = candidate.get("name") or candidate.get("market_hash_name", "")
        price_cents = candidate.get("price_cents") or candidate.get("price", 0)
        listing_id = candidate.get("listing_id") or candidate.get("id", "")
        first_seen = candidate.get("first_seen_ts") or time.time()
        absent_seen = candidate.get("absent_first_seen_ts") or time.time()

        if not name or not price_cents:
            return 400, True

        url = "https://api.waxpeer.com/v1/history"
        params = {"api": config.WAXPEER_API_KEY}
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            async with session.get(url, params=params, headers=headers, timeout=5) as response:
                if response.status == 200:
                    body = await response.json()
                    sales = body.get("history", [])

                    server_date_str = response.headers.get("Date")
                    clock_offset = 0.0
                    if server_date_str:
                        try:
                            from email.utils import parsedate_to_datetime
                            server_ts = parsedate_to_datetime(server_date_str).timestamp()
                            clock_offset = time.time() - server_ts
                        except Exception:
                            pass

                    waxpeer_first_seen = first_seen - clock_offset
                    waxpeer_absent_seen = absent_seen - clock_offset
                    candidate_price_usd = price_cents / 100.0

                    for sale in sales:
                        sale_name = sale.get("name")
                        if sale_name != name:
                            continue
                        
                        sale_price = float(sale.get("price", 0)) / 1000.0
                        
                        raw_date = sale.get("created")
                        if not raw_date:
                            continue
                        try:
                            from datetime import datetime
                            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                            sale_date = dt.timestamp()
                        except Exception:
                            continue

                        price_match = abs(sale_price - candidate_price_usd) < 0.01
                        time_match = (waxpeer_first_seen - 10.0) <= sale_date <= (waxpeer_absent_seen + 10.0)

                        if price_match and time_match:
                            sale_sig = f"waxpeer_{name}_{sale_price:.2f}_{sale_date:.3f}"
                            if self._matched_sales_cache.add(sale_sig):
                                logger.info(
                                    f"Match Waxpeer history trouvé pour {name} : "
                                    f"prix={sale_price:.2f}$, date={sale_date}"
                                )
                                return 200, False
                            else:
                                logger.debug(f"Transaction Waxpeer déjà consommée : {sale_sig}")

                    return 200, True
                elif response.status == 429:
                    return 429, True
                else:
                    logger.warning(f"Waxpeer history error for {listing_id}: HTTP {response.status}")
                    return response.status, True
        except Exception as e:
            logger.error(f"Waxpeer history exception for {listing_id}: {e}")
            return 500, True

    async def _run_delayed_market_csgo_verification(self, candidate: dict, session: aiohttp.ClientSession) -> None:
        """
        Gère la double-vérification différée pour Market.CSGO afin de contourner le délai de cache
        d'environ 1 heure sur leur API d'historique de ventes.
        """
        absent_seen = candidate.get("absent_first_seen_ts") or time.time()
        # On attend 75 minutes (4500 secondes) depuis la disparition pour interroger l'historique
        delay = (absent_seen + 4500) - time.time()
        if delay > 0:
            logger.info(
                f"Market.CSGO: Vérification différée planifiée pour {candidate.get('name')} "
                f"({candidate.get('listing_id')}) dans {delay/60:.1f} minutes."
            )
            await asyncio.sleep(delay)

        attempts = candidate.get("_attempts", 0)
        try:
            status_code, is_still_active = await self._verify_market_csgo_listing(candidate, session)
            if status_code == 429:
                candidate["_attempts"] = attempts + 1
                if candidate["_attempts"] < 3:
                    logger.warning(f"Market.CSGO: Rate limit lors de la vérification différée de {candidate.get('name')}. Réessai dans 5 min.")
                    await asyncio.sleep(300)
                    asyncio.create_task(self._run_delayed_market_csgo_verification(candidate, session))
                    return
                else:
                    is_still_active = True
            
            self.observer.confirm_disappearance(candidate["listing_id"], is_still_active)
        except Exception as e:
            logger.error(f"Market.CSGO: Erreur lors de la vérification différée de {candidate.get('listing_id')} : {e}")
            self.observer.confirm_disappearance(candidate["listing_id"], is_still_active=True)

    async def _get_market_csgo_history_id(self, name: str, session: aiohttp.ClientSession) -> Optional[int]:
        if not hasattr(self, "_market_csgo_history_ids"):
            url = "https://market.csgo.com/api/v2/full-history/all.json"
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/120.0.0.0"
                )
            }
            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        self._market_csgo_history_ids = data.get("history", {})
                        logger.info(f"Loaded {len(self._market_csgo_history_ids)} item history IDs from Market.CSGO")
                    else:
                        logger.error(f"Failed to load Market.CSGO history map: {response.status}")
                        return None
            except Exception as e:
                logger.error(f"Error loading Market.CSGO history map: {e}")
                return None
        return self._market_csgo_history_ids.get(name)

    async def _verify_market_csgo_listing(self, candidate: dict, session: aiohttp.ClientSession) -> tuple[int, bool]:
        name = candidate.get("name") or candidate.get("market_hash_name", "")
        price_cents = candidate.get("price_cents") or candidate.get("price", 0)
        listing_id = candidate.get("listing_id") or candidate.get("id", "")
        first_seen = candidate.get("first_seen_ts") or time.time()
        absent_seen = candidate.get("absent_first_seen_ts") or time.time()

        if not name or not price_cents:
            return 400, True

        item_id = await self._get_market_csgo_history_id(name, session)
        if not item_id:
            logger.warning(f"Market.CSGO : ID d'historique non trouvé pour {name}")
            return 404, True

        url = f"https://market.csgo.com/api/v2/full-history/{item_id}.json"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/120.0.0.0"
            )
        }

        try:
            async with session.get(url, headers=headers, timeout=5) as response:
                if response.status == 200:
                    body = await response.json()
                    sales = body.get("data", {}).get("history", [])

                    server_date_str = response.headers.get("Date")
                    clock_offset = 0.0
                    if server_date_str:
                        try:
                            from email.utils import parsedate_to_datetime
                            server_ts = parsedate_to_datetime(server_date_str).timestamp()
                            clock_offset = time.time() - server_ts
                        except Exception:
                            pass

                    market_first_seen = first_seen - clock_offset
                    market_absent_seen = absent_seen - clock_offset
                    candidate_price_usd = price_cents / 100.0

                    for sale in sales:
                        if len(sale) < 3:
                            continue
                        sale_date = float(sale[0])
                        sale_price = float(sale[2])

                        price_match = abs(sale_price - candidate_price_usd) < 0.01
                        time_match = (market_first_seen - 10.0) <= sale_date <= (market_absent_seen + 10.0)

                        if price_match and time_match:
                            sale_sig = f"market_csgo_{name}_{sale_price:.2f}_{sale_date:.3f}"
                            if self._matched_sales_cache.add(sale_sig):
                                logger.info(
                                    f"Match Market.CSGO history trouvé pour {name} : "
                                    f"prix={sale_price:.2f}$, date={sale_date}"
                                )
                                return 200, False
                            else:
                                logger.debug(f"Transaction Market.CSGO déjà consommée : {sale_sig}")

                    return 200, True
                elif response.status == 429:
                    return 429, True
                else:
                    logger.warning(f"Market.CSGO history error for {listing_id}: HTTP {response.status}")
                    return response.status, True
        except Exception as e:
            logger.error(f"Market.CSGO history exception for {listing_id}: {e}")
            return 500, True

    # ──────────────────────────────────────────────────────────────────────────
    # BOUCLE D'OBSERVATION WAXPEER (WEBSOCKET)
    # ──────────────────────────────────────────────────────────────────────────

    async def _observe_waxpeer(self, session: aiohttp.ClientSession) -> None:
        from waxpeer_ingestion import WaxpeerIngestor

        logger.info("Waxpeer Observation : abonné au flux WebSocket...")

        def on_new(listing: dict):
            fv = listing.get("item", {}).get("float_value")
            if not fv or fv <= 0:
                return
            self.observer.record_addition({
                "id": listing.get("id"),
                "offer_id": listing.get("id").replace("waxpeer_", ""),
                "market_hash_name": listing.get("market_hash_name"),
                "price": listing.get("price"),
                "listed_at": listing.get("listed_at"),
                "float_value": fv,
                "paint_seed": listing.get("item", {}).get("paint_seed"),
                "sticker_count": len(listing.get("item", {}).get("stickers", [])),
                "sticker_names": [s["name"] for s in listing.get("item", {}).get("stickers", [])],
            }, platform="waxpeer")

        def on_removed(data: dict):
            self.observer.record_removal(data["id"], platform="waxpeer", auto_confirm=True)

        ingestor = WaxpeerIngestor(callback=on_new, on_removed=on_removed)
        await ingestor.start(session=session)
        
        try:
            while self.is_running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await ingestor.stop()

    # ──────────────────────────────────────────────────────────────────────────
    # BOUCLE D'OBSERVATION MARKET.CSGO (WS + SNAPSHOT PARTIEL)
    # ──────────────────────────────────────────────────────────────────────────

    async def _observe_market_csgo(self, session: aiohttp.ClientSession) -> None:
        from market_csgo_ingestion import MarketCSGOIngestor

        logger.info("Market.CSGO Observation : abonné au flux WebSocket...")

        def on_new(listing: dict):
            pass

        def on_snapshot(listings: list, market_hash_name: str):
            now_iso = datetime.now(timezone.utc).isoformat()
            new_count = 0
            for listing in listings:
                listing_id = listing.get("id")
                item_data = listing.get("item") or {}
                float_value = item_data.get("float_value")
                if not listing_id or not float_value or float_value <= 0:
                    continue
                stickers = item_data.get("stickers") or []
                is_tgt = (market_hash_name in self.target_skins) if self.target_skins else True
                saved = self.observer._db.save_observed_listing(
                    listing_id=listing_id,
                    market_hash_name=market_hash_name,
                    price_cents=listing.get("price", 0),
                    platform="market_csgo",
                    float_value=float_value,
                    paint_seed=item_data.get("paint_seed"),
                    sticker_count=len(stickers),
                    sticker_names=[s["name"] for s in stickers if isinstance(s, dict) and "name" in s],
                    timestamp=now_iso,
                    listed_at=None,
                    is_target=is_tgt,
                )
                if saved:
                    new_count += 1
            if new_count > 0:
                logger.debug(f"Market.CSGO: {new_count} nouveaux listings enregistrés pour {market_hash_name}")

        ingestor = MarketCSGOIngestor(callback=on_new, on_snapshot_callback=on_snapshot)
        await ingestor.start(session=session)

        try:
            while self.is_running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await ingestor.stop()

    async def _deferred_market_csgo_reconciliation_loop(self, session: aiohttp.ClientSession) -> None:
        """
        Réconcilie les listings Market.CSGO observés avec l'API de recherche.
        Pour chaque skin ayant eu une mise à jour de prix récente (dirty), on re-interroge
        l'API pour détecter quels listings ont disparu (vendus).
        """
        await asyncio.sleep(60)

        while self.is_running:
            try:
                start_time = time.perf_counter()
                logger.info("Début de la boucle de réconciliation différée Market.CSGO...")

                self.observer._db.clean_old_observed_listings(config.OBSERVER_MAX_TTD_SEC)

                pending = self.observer._db.get_pending_observed_listings("market_csgo")
                if not pending:
                    logger.debug("Aucun listing Market.CSGO en attente de réconciliation.")
                    await asyncio.sleep(120)
                    continue

                by_skin: dict = {}
                for p in pending:
                    by_skin.setdefault(p["market_hash_name"], []).append(p)

                logger.info(f"Réconciliation Market.CSGO : {len(pending)} listings sur {len(by_skin)} skins...")

                matched_count = 0
                sold_ids: list = []

                url_search = "https://market.csgo.com/api/v2/search-item-by-hash-name-specific"
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                }

                for skin_name, listings in by_skin.items():
                    if not self.is_running:
                        break

                    await asyncio.sleep(1.0)

                    params = {"key": config.MARKET_CSGO_API_KEY, "hash_name": skin_name}
                    try:
                        async with session.get(url_search, params=params, headers=headers, timeout=15) as response:
                            if response.status == 200:
                                data = await response.json()
                                current_raw = data.get("data", []) or []
                                current_ids = {str(l.get("id")) for l in current_raw if l.get("id")}

                                now_ts = time.time()

                                # Phase 1 — collecter les items disparus avec leur TTD
                                disappeared: list = []
                                for item in listings:
                                    raw_id = item["listing_id"].replace("market_csgo_", "")
                                    if raw_id in current_ids:
                                        continue

                                    # Déduplication inter-cycles
                                    if not self._matched_sales_cache.add(item["listing_id"]):
                                        sold_ids.append(item["listing_id"])
                                        continue

                                    if item.get("listed_at") is not None:
                                        first_ts = float(item["listed_at"])
                                    else:
                                        try:
                                            first_ts = datetime.fromisoformat(
                                                item["timestamp"].replace("Z", "+00:00")
                                            ).timestamp()
                                        except Exception:
                                            continue

                                    ttd_ms = max(0.0, (now_ts - first_ts) * 1000)
                                    if ttd_ms > config.OBSERVER_MAX_TTD_SEC * 1000:
                                        sold_ids.append(item["listing_id"])
                                        continue

                                    disappeared.append((item, first_ts, ttd_ms))

                                if not disappeared:
                                    pass
                                else:
                                    # Phase 2 — un seul appel à l'historique pour ce skin
                                    skin_history: list = []
                                    hist_clock_offset = 0.0
                                    item_id = await self._get_market_csgo_history_id(skin_name, session)
                                    if item_id:
                                        hist_url = f"https://market.csgo.com/api/v2/full-history/{item_id}.json"
                                        try:
                                            async with session.get(hist_url, headers=headers, timeout=10) as hr:
                                                if hr.status == 200:
                                                    hbody = await hr.json()
                                                    skin_history = hbody.get("data", {}).get("history", []) or []
                                                    srv_date = hr.headers.get("Date")
                                                    if srv_date:
                                                        try:
                                                            from email.utils import parsedate_to_datetime as _p2
                                                            hist_clock_offset = time.time() - _p2(srv_date).timestamp()
                                                        except Exception:
                                                            pass
                                                elif hr.status == 429:
                                                    logger.warning("Market.CSGO history rate limited durant réconciliation.")
                                                    await asyncio.sleep(30)
                                        except Exception as he:
                                            logger.warning(f"Market.CSGO history fetch failed pour {skin_name}: {he}")

                                    # Phase 3 — assigner HIGH/MEDIUM et enregistrer
                                    for item, first_ts, ttd_ms in disappeared:
                                        if ttd_ms < config.OBS_BOT_SNIPE_TTD_MS:
                                            category = "BOT_SNIPE"
                                        elif ttd_ms < config.OBS_FAST_HUMAN_TTD_MS:
                                            category = "FAST_HUMAN"
                                        else:
                                            category = "NORMAL_SALE"

                                        candidate_price_usd = item["price_cents"] / 100.0
                                        market_first = first_ts - hist_clock_offset
                                        market_absent = now_ts - hist_clock_offset
                                        confidence = "MEDIUM"

                                        for sale in skin_history:
                                            if len(sale) < 3:
                                                continue
                                            sale_date = float(sale[0])
                                            sale_price = float(sale[2])
                                            if (
                                                abs(sale_price - candidate_price_usd) < 0.01
                                                and (market_first - 10.0) <= sale_date <= (market_absent + 10.0)
                                            ):
                                                hist_sig = f"market_csgo_{skin_name}_{sale_price:.2f}_{sale_date:.3f}"
                                                if self._matched_sales_cache.add(hist_sig):
                                                    confidence = "HIGH"
                                                    break

                                        try:
                                            sticker_names = json.loads(item["sticker_names"])
                                        except Exception:
                                            sticker_names = []

                                        logger.info(
                                            f"MATCH Market.CSGO: {skin_name} | "
                                            f"Prix: ${candidate_price_usd:.2f} | "
                                            f"TTD: {ttd_ms / 1000:.1f}s | "
                                            f"Catégorie: {category} | Confidence: {confidence}"
                                        )

                                        self.observer._db.save_transaction(
                                            market_hash_name=skin_name,
                                            price_usd=candidate_price_usd,
                                            ttd_ms=ttd_ms,
                                            platform="market_csgo",
                                            category=category,
                                            float_value=item["float_value"],
                                            paint_seed=item["paint_seed"],
                                            sticker_count=item["sticker_count"],
                                            sticker_names=sticker_names,
                                            confidence=confidence,
                                        )
                                        sold_ids.append(item["listing_id"])
                                        matched_count += 1

                            elif response.status == 429:
                                logger.warning("Market.CSGO reconciliation rate limited. Pause 30s.")
                                await asyncio.sleep(30)
                            else:
                                logger.warning(f"Market.CSGO reconciliation HTTP {response.status} pour {skin_name}")
                    except Exception as skin_err:
                        logger.error(f"Erreur réconciliation Market.CSGO pour {skin_name}: {skin_err}")

                if sold_ids:
                    self.observer._db.delete_observed_listings(sold_ids)

                elapsed = time.perf_counter() - start_time
                logger.info(
                    f"Réconciliation Market.CSGO terminée : {matched_count} ventes détectées en {elapsed:.1f}s."
                )

            except Exception as loop_err:
                logger.error(f"Erreur boucle réconciliation Market.CSGO : {loop_err}")

            await asyncio.sleep(120)

    # ──────────────────────────────────────────────────────────────────────────
    # BOUCLE D'OBSERVATION SKINPORT (WEBSOCKET SOCKET.IO + MSGPACK)
    # Activation : config.SKINPORT_ENABLED = True
    # Pas de clé API requise pour le flux public.
    # ──────────────────────────────────────────────────────────────────────────

    async def _observe_skinport(self, session: aiohttp.ClientSession) -> None:
        use_playwright = getattr(config, "SKINPORT_USE_PLAYWRIGHT", False)
        email = getattr(config, "SKINPORT_EMAIL", "")
        password = getattr(config, "SKINPORT_PASSWORD", "")

        if use_playwright:
            from skinport_cf_bypass import SkinportPlaywrightIngestor
            logger.info("Skinport Observation : mode Playwright (login=%s)...", bool(email and password))
        else:
            from skinport_ingestion import SkinportIngestor
            logger.info("Skinport Observation : connexion au flux WebSocket saleFeed...")

        # Cache mémoire sale_id -> listing_data pour lookup O(1) lors du "sold"
        # (complément du stockage DB — survit aux courtes interruptions réseau)
        _pending: dict = {}

        def on_listed(listing: dict) -> None:
            float_value = listing.get("float_value")
            if not float_value or float_value <= 0:
                return

            sale_id = listing["sale_id"]
            listing_id = listing["id"]  # "skinport_{sale_id}"

            # Mémoriser dans le cache local
            _pending[sale_id] = listing

            # Persister en DB pour survivre à un redémarrage du bot
            stickers = listing.get("stickers") or []
            is_tgt = (listing["market_hash_name"] in self.target_skins) if self.target_skins else True
            saved = self.observer._db.save_observed_listing(
                listing_id=listing_id,
                market_hash_name=listing["market_hash_name"],
                price_cents=listing["price"],
                platform="skinport",
                float_value=float_value,
                paint_seed=listing.get("paint_seed"),
                sticker_count=len(stickers),
                sticker_names=[s["name"] for s in stickers if isinstance(s, dict) and "name" in s],
                timestamp=listing["ingested_at"],
                listed_at=None,
                is_target=is_tgt,
            )
            if saved:
                logger.debug(f"Skinport listed: {listing['market_hash_name']} (sale_id={sale_id})")

        def on_sold(listing: dict) -> None:
            sale_id = listing["sale_id"]
            listing_id = listing["id"]

            # Déduplication inter-événements
            sale_sig = f"skinport_sold_{listing_id}"
            if not self._matched_sales_cache.add(sale_sig):
                return

            # Lookup : cache mémoire d'abord, fallback DB
            original = _pending.pop(sale_id, None)
            if original is None:
                original = self.observer._db.get_observed_listing_by_id(listing_id)
            if original is None:
                # Item listé avant le démarrage du bot — TTD inconnu
                logger.debug(f"Skinport sold inconnu (listé avant démarrage): {listing['market_hash_name']}")
                return

            # Calcul TTD
            try:
                if isinstance(original, dict) and "ingested_at" in original:
                    listed_ts = datetime.fromisoformat(
                        original["ingested_at"].replace("Z", "+00:00")
                    ).timestamp()
                else:
                    listed_ts = datetime.fromisoformat(
                        original["timestamp"].replace("Z", "+00:00")
                    ).timestamp()
            except Exception:
                return

            now_ts = time.time()
            ttd_ms = max(0.0, (now_ts - listed_ts) * 1000)

            if ttd_ms > config.OBSERVER_MAX_TTD_SEC * 1000:
                self.observer._db.delete_observed_listings([listing_id])
                return

            if ttd_ms < config.OBS_BOT_SNIPE_TTD_MS:
                category = "BOT_SNIPE"
            elif ttd_ms < config.OBS_FAST_HUMAN_TTD_MS:
                category = "FAST_HUMAN"
            else:
                category = "NORMAL_SALE"

            float_value = (
                original.get("float_value")
                if isinstance(original, dict)
                else original["float_value"]
            )
            paint_seed = (
                original.get("paint_seed")
                if isinstance(original, dict)
                else original["paint_seed"]
            )
            price_cents = (
                original.get("price")
                if isinstance(original, dict) and "price" in original
                else original.get("price_cents", 0)
            )
            market_hash_name = (
                original.get("market_hash_name", "")
            )

            sticker_names_raw = (
                original.get("sticker_names")
                if isinstance(original, dict)
                else original.get("sticker_names", "[]")
            )
            if isinstance(sticker_names_raw, list):
                sticker_names = sticker_names_raw
            else:
                try:
                    sticker_names = json.loads(sticker_names_raw or "[]")
                except Exception:
                    sticker_names = []

            sticker_count = (
                len(original.get("stickers", []))
                if isinstance(original, dict) and "stickers" in original
                else original.get("sticker_count", 0)
            )

            logger.info(
                f"MATCH Skinport: {market_hash_name} | "
                f"Prix: ${price_cents / 100.0:.2f} | "
                f"TTD: {ttd_ms / 1000:.1f}s | Catégorie: {category} | Confidence: HIGH"
            )

            self.observer._db.save_transaction(
                market_hash_name=market_hash_name,
                price_usd=price_cents / 100.0,
                ttd_ms=ttd_ms,
                platform="skinport",
                category=category,
                float_value=float_value,
                paint_seed=paint_seed,
                sticker_count=sticker_count,
                sticker_names=sticker_names,
                confidence="HIGH",
            )
            self.observer._db.delete_observed_listings([listing_id])

        if use_playwright:
            ingestor = SkinportPlaywrightIngestor(
                on_listed=on_listed,
                on_sold=on_sold,
                headless=getattr(config, "SKINPORT_PLAYWRIGHT_HEADLESS", True),
                email=email,
                password=password,
            )
        else:
            ingestor = SkinportIngestor(on_listed=on_listed, on_sold=on_sold)
        await ingestor.start(session=session)

        try:
            while self.is_running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await ingestor.stop()

            await asyncio.sleep(120)
