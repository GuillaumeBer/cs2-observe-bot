"""
CSFloat Ingestor — Polling de l'API REST CSFloat

Stratégie :
  - Poll GET /api/v1/listings?sort_by=most_recent toutes les POLL_INTERVAL secondes
  - Nouveaux listings (id jamais vu) → callback on_listed
  - Listings disparus du snapshot → callback on_sold avec TTD approximatif
  - Normalise au même format que DMarket / Skinport

Prérequis :
  - Clé API gratuite : https://csfloat.com/profile → onglet "Developer"
  - Variable d'environnement : CSFLOAT_API_KEY

Rate limit : ~200 req/h → intervalle minimum recommandé : 60s
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Optional

import aiohttp
import config

logger = logging.getLogger("cs2_sniper.csfloat_ingestion")

_BASE_URL = "https://csfloat.com/api/v1"
_POLL_INTERVAL = 60  # secondes
_REQUEST_TIMEOUT = 15


class CSFloatIngestor:
    """
    Écoute les nouveaux listings CSFloat par polling de l'API REST.

    Interface identique à SkinportIngestor / DMarketIngestor :
      on_listed(normalized_dict) — item mis en vente
      on_sold(normalized_dict)   — item vendu (TTD approximatif ±poll_interval)
    """

    def __init__(
        self,
        on_listed: Callable[[dict], None],
        on_sold: Callable[[dict], None],
        api_key: str = "",
        poll_interval: int = _POLL_INTERVAL,
    ):
        self.on_listed = on_listed
        self.on_sold = on_sold
        self._api_key = api_key
        self._poll_interval = poll_interval
        self._tracked: dict[str, dict] = {}  # listing_id -> {created_at, market_hash_name, price, ...}
        self._is_running = False
        self._main_task: Optional[asyncio.Task] = None

    async def start(self, session=None):
        self._is_running = True
        self._main_task = asyncio.create_task(self._run())

    async def stop(self):
        self._is_running = False
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        logger.info("CSFloatIngestor stopped.")

    def _headers(self) -> dict:
        h = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        if self._api_key:
            h["Authorization"] = self._api_key
        return h

    async def _run(self):
        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(
            headers=self._headers(),
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT),
        ) as session:
            while self._is_running:
                try:
                    await self._poll(session)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("CSFloat: erreur polling: %s", e)

                try:
                    await asyncio.sleep(self._poll_interval)
                except asyncio.CancelledError:
                    break

    async def _poll(self, session: aiohttp.ClientSession):
        params = {
            "sort_by": "most_recent",
            "limit": 50,
            "type": "buy_now",
        }
        url = f"{_BASE_URL}/listings"

        async with session.get(url, params=params) as resp:
            if resp.status == 403:
                body = await resp.text()
                logger.error("CSFloat: 403 Forbidden — clé API manquante ou invalide: %s", body[:120])
                return
            if resp.status == 429:
                logger.warning("CSFloat: rate limit atteint, pause 120s")
                await asyncio.sleep(120)
                return
            if resp.status != 200:
                logger.warning("CSFloat: status inattendu %d", resp.status)
                return

            listings = await resp.json()

        if not isinstance(listings, list):
            listings = listings.get("data", []) if isinstance(listings, dict) else []

        now = datetime.now(timezone.utc)
        current_ids: set[str] = set()

        for listing in listings:
            lid = str(listing.get("id", ""))
            if not lid:
                continue
            current_ids.add(lid)

            if lid not in self._tracked:
                # Nouveau listing
                normalized = self._normalize(listing)
                if normalized:
                    self._tracked[lid] = {
                        "created_at": datetime.fromisoformat(
                            listing["created_at"].replace("Z", "+00:00")
                        ),
                        "market_hash_name": normalized["market_hash_name"],
                        "price": normalized["price"],
                    }
                    self.on_listed(normalized)

        # Listings disparus → probablement vendus
        sold_ids = set(self._tracked.keys()) - current_ids
        for lid in sold_ids:
            entry = self._tracked.pop(lid)
            ttd_sec = (now - entry["created_at"]).total_seconds()
            sold_normalized = {
                "id": f"csfloat_{lid}",
                "sale_id": lid,
                "market_hash_name": entry["market_hash_name"],
                "price": entry["price"],
                "ttd_seconds": ttd_sec,
                "platform": "csfloat",
                "ingested_at": now.isoformat(),
            }
            logger.debug(
                "CSFloat sold: %s @ %.2f€ TTD=%.0fs",
                entry["market_hash_name"],
                entry["price"] / 100,
                ttd_sec,
            )
            self.on_sold(sold_normalized)

        logger.debug(
            "CSFloat poll: %d listings, %d nouveaux, %d vendus, %d trackés",
            len(listings),
            len([i for i in current_ids if i not in self._tracked or True]),
            len(sold_ids),
            len(self._tracked),
        )

    def _normalize(self, listing: dict) -> Optional[dict]:
        item = listing.get("item", {})
        market_hash_name = item.get("market_hash_name", "")
        if not market_hash_name:
            return None

        price_cents = listing.get("price", 0)
        try:
            price_cents = int(price_cents)
        except (ValueError, TypeError):
            return None

        price_usd = price_cents / 100.0
        if price_usd < config.MIN_PRICE_USD or price_usd > config.MAX_PRICE_USD:
            return None

        float_value = item.get("float_value")
        paint_seed = item.get("paint_seed")
        try:
            float_value = float(float_value) if float_value is not None else None
        except (ValueError, TypeError):
            float_value = None
        try:
            paint_seed = int(paint_seed) if paint_seed is not None else None
        except (ValueError, TypeError):
            paint_seed = None

        stickers = self._parse_stickers(item.get("stickers"), item.get("scm", {}))

        # Prix Steam Community Market (pour évaluer la décote)
        scm_price = item.get("scm", {}).get("price", 0)

        from datetime import datetime, timezone
        return {
            "id": f"csfloat_{listing['id']}",
            "sale_id": str(listing["id"]),
            "asset_id": item.get("asset_id"),
            "market_hash_name": market_hash_name,
            "price": price_cents,
            "float_value": float_value,
            "paint_seed": paint_seed,
            "stickers": stickers,
            "scm_price": scm_price,
            "platform": "csfloat",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    def _parse_stickers(self, stickers_raw, scm_data=None) -> list:
        if not stickers_raw or not isinstance(stickers_raw, list):
            return []
        result = []
        for s in stickers_raw:
            if not isinstance(s, dict):
                continue
            scm = s.get("scm", {}) or {}
            result.append({
                "name": s.get("name", ""),
                "wear": float(s.get("wear") or 0.0),
                "value": scm.get("price", 0),
                "slot": s.get("slot", 0),
            })
        return result
