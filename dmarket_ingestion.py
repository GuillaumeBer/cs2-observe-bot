import asyncio
import logging
import time
import json
from typing import Callable, Optional
import aiohttp
import nacl.signing
import nacl.encoding
import config
from utils import FIFOUniqueCache

logger = logging.getLogger("cs2_sniper.dmarket_ingestion")


def generate_dmarket_headers(
    public_key: str, 
    secret_key: str, 
    method: str, 
    path_with_queries: str, 
    body_dict: Optional[dict] = None
) -> dict:
    """
    Génère les headers d'authentification signés Ed25519 requis par DMarket.
    """
    if not public_key or not secret_key:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
    timestamp = str(int(time.time()))
    body_str = json.dumps(body_dict) if body_dict else ""
    
    # Formule de signature DMarket : Method + Path + Body + Timestamp
    string_to_sign = f"{method}{path_with_queries}{body_str}{timestamp}"
    
    try:
        # Si la clé privée fait 128 caractères hex (seed + clé publique), on extrait les 64 premiers caractères (seed de 32 octets)
        seed_hex = secret_key[:64] if len(secret_key) == 128 else secret_key
        private_key = nacl.signing.SigningKey(bytes.fromhex(seed_hex))
        signature = private_key.sign(string_to_sign.encode('utf-8'))
        signature_hex = signature.signature.hex()
        
        return {
            "X-Api-Key": public_key,
            "X-Sign-Date": timestamp,
            "X-Request-Sign": f"dmar ed25519 {signature_hex}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    except Exception as e:
        logger.error(f"Failed to generate DMarket signature: {e}")
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }


class DMarketIngestor:
    def __init__(self, callback: Callable[[dict], None]):
        """
        callback: Fonction appelée lorsqu'un nouvel item DMarket est détecté et normalisé.
                  Signature : callback(normalized_listing_dict)
        """
        self.callback = callback
        self.is_running = False
        self._processed_ids = FIFOUniqueCache(maxsize=1000)
        self._loop_task: Optional[asyncio.Task] = None

    async def start(self, session: aiohttp.ClientSession):
        """
        Démarre l'ingestion DMarket en mode polling.
        """
        self.is_running = True
        logger.info("Starting DMarket Ingestion (REST Polling)...")
        self._loop_task = asyncio.create_task(self._run_polling(session))

    async def stop(self):
        """
        Arrête l'ingestion.
        """
        self.is_running = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        logger.info("DMarket Ingestion stopped.")

    async def _run_polling(self, session: aiohttp.ClientSession):
        """
        Boucle de polling sur DMarket (/exchange/v1/market/items).
        """
        min_price_cents = int(config.MIN_PRICE_USD * 100)
        max_price_cents = int(config.MAX_PRICE_USD * 100)
        path = f"/exchange/v1/market/items?gameId=a8db&limit=15&orderBy=updated&orderDir=desc&types=dmarket,p2p&currency=USD&priceFrom={min_price_cents}&priceTo={max_price_cents}"
        url = f"https://api.dmarket.com{path}"

        warmup = True  # Premier poll = initialisation des IDs connus, sans callback

        while self.is_running:
            try:
                headers = generate_dmarket_headers(
                    public_key=config.DMARKET_PUBLIC_KEY,
                    secret_key=config.DMARKET_SECRET_KEY,
                    method="GET",
                    path_with_queries=path
                )

                request_start = time.perf_counter()
                async with session.get(url, headers=headers, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                        items = data.get("objects", [])

                        new_count = 0

                        for item in items:
                            item_id = item.get("itemId")
                            if not item_id:
                                continue
                            if self._processed_ids.add(item_id):
                                if warmup:
                                    continue  # On mémorise l'ID mais on n'évalue pas
                                normalized = self._normalize_item(item)
                                if normalized:
                                    normalized["_request_start"] = request_start
                                    self.callback(normalized)
                                    new_count += 1

                        if warmup:
                            logger.info(f"DMarket: {len(items)} IDs initialisés (warmup — pas d'évaluation au démarrage).")
                            warmup = False
                        elif new_count > 0:
                            logger.info(f"DMarket: {new_count} nouveaux listings détectés (total fetched: {len(items)})")
                    elif response.status == 401 or response.status == 403:
                        err_text = await response.text()
                        logger.error(f"DMarket Authentication failed (HTTP {response.status}). Body: {err_text}. Check DMarket API keys in .env")
                        await asyncio.sleep(10)
                    elif response.status == 429:
                        retry_after = 10
                        try:
                            retry_after = int(response.headers.get("Retry-After", 10))
                        except (ValueError, TypeError):
                            pass
                        logger.warning(f"DMarket Rate limited. Waiting {retry_after} seconds...")
                        await asyncio.sleep(retry_after + 2)
                    else:
                        err_text = await response.text()
                        logger.error(f"DMarket Polling error: HTTP {response.status}. Body: {err_text}")
            except Exception as e:
                logger.error(f"Exception during DMarket polling: {e}")

            await asyncio.sleep(0.2)

    def _normalize_item(self, item: dict) -> Optional[dict]:
        """
        Normalise un item DMarket au format commun compatible avec filters.py et pricing.py.
        """
        title = item.get("title")
        item_id = item.get("itemId")
        price_info = item.get("price", {})
        
        if not title or not item_id:
            return None

        amount_str = price_info.get("USD") or price_info.get("amount")
        if not amount_str:
            return None
            
        try:
            if "." in amount_str:
                price_cents = int(round(float(amount_str) * 100))
            else:
                price_cents = int(amount_str)
        except ValueError:
            return None

        extra = item.get("extra", {})
        
        raw_float = extra.get("floatPartValue") or extra.get("floatValue")
        if raw_float is not None:
            try:
                float_value = float(raw_float)
            except ValueError:
                float_value = None
        else:
            float_value = None

        stickers = []
        dmarket_stickers = extra.get("stickers", [])
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

        return {
            "id": f"dmarket_{item_id}",
            "price": price_cents,
            "market_hash_name": title,
            "created_at": str(int(time.time())),
            "type": "buy_now",
            "item": {
                "float_value": float_value,
                "stickers": stickers
            }
        }
