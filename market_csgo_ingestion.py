import asyncio
import logging
import time
from typing import Callable, Optional, Any
import aiohttp
from datetime import datetime, timezone
import centrifuge
import config
from utils import FIFOUniqueCache

logger = logging.getLogger("cs2_sniper.market_csgo_ingestion")


class MarketCSGOIngestor:
    def __init__(self, callback: Callable[[dict], None], on_snapshot_callback: Optional[Callable[[list, str], None]] = None):
        """
        callback: Fonction appelée lorsqu'un nouvel item Market.CSGO est détecté, inspecté et normalisé.
                  Signature : callback(normalized_listing_dict)
        on_snapshot_callback: Callback pour propager les snapshots complets de listings par skin.
        """
        self.callback = callback
        self.on_snapshot_callback = on_snapshot_callback
        self.is_running = False
        self.api_key = config.MARKET_CSGO_API_KEY
        self.name_id_map = {}
        self._processed_ids = FIFOUniqueCache(maxsize=5000)
        self._skin_cooldowns = {}
        self._request_semaphore = asyncio.Semaphore(2)
        
        self._client: Optional[centrifuge.Client] = None
        self._sub: Optional[centrifuge.Subscription] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._loop_task: Optional[asyncio.Task] = None

    async def start(self, session: aiohttp.ClientSession):
        """
        Démarre l'ingestion Market.CSGO via Centrifugo WebSocket.
        """
        if not self.api_key:
            logger.error("Market.CSGO API Key (MARKET_CSGO_API_KEY) is missing in .env.")
            return

        self.is_running = True
        self._session = session
        logger.info("Starting Market.CSGO Ingestion (WebSocket Centrifugo + REST API)...")

        # 1. Charger le dictionnaire de traduction des ID
        await self._load_dictionary()

        # 2. Lancer la boucle WebSocket
        self._loop_task = asyncio.create_task(self._run_websocket())

    async def stop(self):
        """
        Arrête l'ingestion et ferme les connexions.
        """
        self.is_running = False
        if self._sub:
            try:
                await self._sub.unsubscribe()
            except Exception as e:
                logger.debug(f"Error during unsubscribe: {e}")
        if self._client:
            try:
                await self._client.disconnect()
            except Exception as e:
                logger.debug(f"Error during client disconnect: {e}")
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        logger.info("Market.CSGO Ingestion stopped.")

    async def _load_dictionary(self):
        """
        Télécharge le dictionnaire de correspondances name_id -> market_hash_name.
        """
        url = "https://market.csgo.com/api/v2/dictionary/names.json"
        logger.info("Loading Market.CSGO name dictionary from API...")
        try:
            async with self._session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get("items", [])
                    self.name_id_map = {
                        str(item["id"]): item["hash_name"]
                        for item in items
                        if "id" in item and "hash_name" in item
                    }
                    logger.info(f"Loaded {len(self.name_id_map)} item mappings from Market.CSGO dictionary.")
                else:
                    logger.error(f"Failed to load Market.CSGO dictionary: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Exception while loading Market.CSGO dictionary: {e}")

    async def _fetch_ws_token(self) -> str:
        """
        Récupère un jeton frais pour la connexion WebSocket Centrifugo.
        """
        url = "https://market.csgo.com/api/v2/get-ws-token"
        try:
            async with self._session.get(url, params={"key": self.api_key}, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success") and "token" in data:
                        return data["token"]
                    else:
                        raise ValueError(f"Failed response: {data}")
                else:
                    raise ConnectionError(f"HTTP {response.status}")
        except Exception as e:
            logger.error(f"Failed to fetch WebSocket token: {e}")
            raise

    async def _run_websocket(self):
        """
        Met en place le client Centrifugo et gère la connexion/abonnements.
        """
        class MarketClientHandler(centrifuge.ClientEventHandler):
            async def on_connected(self, ctx: centrifuge.ConnectedContext) -> None:
                logger.info("Connected to Market.CSGO Centrifugo WebSocket.")

            async def on_connecting(self, ctx: centrifuge.ConnectingContext) -> None:
                logger.info(f"Connecting to Market.CSGO Centrifugo... Reason: {ctx.reason}")

            async def on_disconnected(self, ctx: centrifuge.DisconnectedContext) -> None:
                logger.warning(f"Disconnected from Market.CSGO Centrifugo. Reason: {ctx.reason}")

            async def on_error(self, ctx: centrifuge.ErrorContext) -> None:
                logger.error(f"Market.CSGO Centrifugo Client Error: {ctx.error}")

        class MarketSubscriptionHandler(centrifuge.SubscriptionEventHandler):
            def __init__(self, ingestor: "MarketCSGOIngestor"):
                self.ingestor = ingestor

            async def on_publication(self, ctx: centrifuge.PublicationContext) -> None:
                if self.ingestor.is_running:
                    await self.ingestor.handle_publication(ctx.pub.data)

            async def on_subscribed(self, ctx: centrifuge.SubscribedContext) -> None:
                logger.info("Subscribed to channel public:items:730:usd")

            async def on_unsubscribed(self, ctx: centrifuge.UnsubscribedContext) -> None:
                logger.warning(f"Unsubscribed from channel: Code {ctx.code}, Reason: {ctx.reason}")

            async def on_error(self, ctx: centrifuge.SubscriptionErrorContext) -> None:
                logger.error(f"Subscription Error: {ctx.error}")

        async def token_provider():
            return await self._fetch_ws_token()

        try:
            initial_token = await self._fetch_ws_token()
            
            self._client = centrifuge.Client(
                address="wss://wsprice.csgo.com/connection/websocket",
                events=MarketClientHandler(),
                token=initial_token,
                get_token=token_provider
            )

            self._sub = self._client.new_subscription(
                channel="public:items:730:usd",
                events=MarketSubscriptionHandler(self)
            )

            await self._client.connect()
            await self._sub.subscribe()

            while self.is_running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.exception(f"Fatal error in WebSocket loop: {e}")
            if self.is_running:
                logger.info("Attempting connection restart in 10 seconds...")
                await asyncio.sleep(10)
                self._loop_task = asyncio.create_task(self._run_websocket())

    async def handle_publication(self, data: Any):
        try:
            if isinstance(data, list):
                for item in data:
                    await self._process_ws_item(item)
            elif isinstance(data, dict):
                if "items" in data:
                    for item in data["items"]:
                        await self._process_ws_item(item)
                else:
                    await self._process_ws_item(data)
        except Exception as e:
            logger.error(f"Error handling publication data: {e}")

    async def _process_ws_item(self, item: dict):
        """
        Traite un événement de changement de prix Market.CSGO.
        Déclenche un snapshot REST pour tout changement de prix au-dessus du seuil MIN_PRICE_USD.
        """
        item_id = str(item.get("name_id") or item.get("id") or "")
        min_price_usd_str = item.get("min") or item.get("price")

        if not item_id or min_price_usd_str is None:
            return

        try:
            min_price_usd = float(min_price_usd_str)
        except (ValueError, TypeError):
            return

        if min_price_usd < config.MIN_PRICE_USD or min_price_usd > config.MAX_PRICE_USD:
            return

        market_hash_name = self.name_id_map.get(item_id)
        if not market_hash_name:
            return

        asyncio.create_task(self._fetch_and_process_specific_listings(market_hash_name))

    async def _fetch_and_process_specific_listings(self, market_hash_name: str):
        # Cooldown per skin to avoid spamming the REST API
        now = time.perf_counter()
        last_fetched = self._skin_cooldowns.get(market_hash_name, 0.0)
        if now - last_fetched < 10.0:
            return
        self._skin_cooldowns[market_hash_name] = now

        url = "https://market.csgo.com/api/v2/search-item-by-hash-name-specific"
        params = {"key": self.api_key, "hash_name": market_hash_name}
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
        async with self._request_semaphore:
            try:
                request_start = time.perf_counter()
                async with self._session.get(url, params=params, headers=headers, timeout=15) as response:
                    if response.status != 200:
                        logger.error(f"HTTP error during details search: {response.status}")
                        return

                    data = await response.json()
                    listings = data.get("data", [])
                    logger.info(f"Market.CSGO: successfully fetched {len(listings)} listings for {market_hash_name} in {time.perf_counter() - request_start:.2f}s")

                    if self.on_snapshot_callback:
                        normalized_listings = []
                        for listing in listings:
                            norm = self._normalize_listing(listing, market_hash_name)
                            if norm:
                                normalized_listings.append(norm)
                        self.on_snapshot_callback(normalized_listings, market_hash_name)

                    for listing in listings:
                        listing_id = str(listing.get("id"))
                        if not listing_id:
                            continue

                        if not self._processed_ids.add(listing_id):
                            continue
                        
                        normalized = self._normalize_listing(listing, market_hash_name)
                        if normalized:
                            normalized["_request_start"] = request_start
                            self.callback(normalized)

            except Exception as e:
                import traceback
                logger.error(f"Exception while retrieving specific listings for {market_hash_name}: {e}\n{traceback.format_exc()}")

    def _normalize_listing(self, listing: dict, market_hash_name: str) -> Optional[dict]:
        listing_id = listing.get("id")
        price_raw = listing.get("price")
        extra = listing.get("extra", {})

        if not listing_id or price_raw is None:
            return None

        try:
            price_cents = int(price_raw / 10)
        except (ValueError, TypeError):
            return None

        float_value = extra.get("float")
        if float_value is not None:
            try:
                float_value = float(float_value)
            except ValueError:
                float_value = None

        if not float_value or float_value <= 0:
            return None

        raw_stickers = extra.get("stickers")
        stickers = self.parse_market_stickers(raw_stickers)

        return {
            "id": f"market_csgo_{listing_id}",
            "price": price_cents,
            "market_hash_name": market_hash_name,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "type": "buy_now",
            "item": {
                "float_value": float_value,
                "stickers": stickers,
                "paint_seed": int(listing.get("class", 0))
            }
        }

    def parse_market_stickers(self, stickers_raw: Any) -> list:
        parsed = []
        if not stickers_raw:
            return parsed

        if isinstance(stickers_raw, list):
            for i, s in enumerate(stickers_raw):
                if isinstance(s, dict):
                    name = s.get("name") or s.get("sticker_name") or s.get("market_hash_name") or ""
                    try:
                        wear = float(s.get("wear") or s.get("sticker_wear") or 0.0)
                    except ValueError:
                        wear = 0.0
                    try:
                        val_usd = s.get("value") or s.get("price") or 0
                        val_cents = int(float(val_usd) * 100) if "." in str(val_usd) else int(val_usd)
                    except ValueError:
                        val_cents = 0

                    parsed.append({
                        "name": name,
                        "wear": wear,
                        "value": val_cents,
                        "slot": s.get("slot") or i
                    })
                elif isinstance(s, str):
                    parsed.append({
                        "name": s,
                        "wear": 0.0,
                        "value": 0,
                        "slot": i
                    })
        elif isinstance(stickers_raw, str):
            parts = [p.strip() for p in stickers_raw.split(";") if p.strip()]
            for i, p in enumerate(parts):
                parsed.append({
                    "name": p,
                    "wear": 0.0,
                    "value": 0,
                    "slot": i
                })

        return parsed
