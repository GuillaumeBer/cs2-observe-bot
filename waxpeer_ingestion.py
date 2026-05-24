import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
import aiohttp
import config
from utils import FIFOUniqueCache

logger = logging.getLogger("cs2_sniper.waxpeer_ingestion")


def _ws_url() -> str:
    return f"wss://waxpeer.com/socket.io/?EIO=4&transport=websocket&api={config.WAXPEER_API_KEY}"


class WaxpeerIngestor:
    def __init__(self, callback: Callable[[dict], None], on_removed: Optional[Callable[[dict], None]] = None):
        self.callback = callback
        self.on_removed = on_removed
        self.is_running = False
        self._processed_ids = FIFOUniqueCache(maxsize=2000)
        self._loop_task: Optional[asyncio.Task] = None
        self._ws = None  # référence pour envoyer pong depuis _handle_message

    async def start(self, session: aiohttp.ClientSession):
        self.is_running = True
        logger.info("Starting Waxpeer Ingestion (Socket.IO WebSocket)...")
        self._loop_task = asyncio.create_task(self._run_websocket())

    async def stop(self):
        self.is_running = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        logger.info("Waxpeer Ingestion stopped.")

    async def _run_websocket(self):
        retry_delay = 2

        while self.is_running:
            try:
                async with websockets.connect(
                    _ws_url(),
                    additional_headers={"authorization": config.WAXPEER_API_KEY},
                    ping_interval=None,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    logger.info("Waxpeer WebSocket connecté.")
                    retry_delay = 2

                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    logger.debug(f"Waxpeer EIO open: {raw[:300]}")
                    ping_interval = 25.0
                    ping_timeout = 20.0
                    if isinstance(raw, str) and raw.startswith("0"):
                        try:
                            hs = json.loads(raw[1:])
                            ping_interval = hs.get("pingInterval", 25000) / 1000
                            ping_timeout = hs.get("pingTimeout", 20000) / 1000
                            logger.info(f"Waxpeer EIO: pingInterval={ping_interval}s pingTimeout={ping_timeout}s")
                        except Exception:
                            pass

                    # Socket.IO connect
                    await ws.send("40")

                    # Wait for SIO connect ACK
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        logger.debug(f"Waxpeer post-40: {raw[:200]}")
                        if raw == "2":
                            await ws.send("3")  # EIO pong
                            continue
                        if raw.startswith("40"):
                            break  # SIO connected

                    # Subscribe to CS:GO new listings
                    await ws.send('42["subscribe",{"name":"csgo"}]')
                    logger.info("Waxpeer: abonné au flux CS:GO en temps réel.")

                    last_ping = time.time()
                    msg_count = 0

                    while self.is_running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            msg_count += 1
                            logger.debug(f"Waxpeer msg[{msg_count}]: {raw[:200]}")
                            await self._handle_message(ws, raw)
                        except asyncio.TimeoutError:
                            pass

                        if time.time() - last_ping >= ping_interval - 2:
                            await ws.send('42["ping",{"name":"ping"}]')
                            logger.debug("Waxpeer: SIO ping envoyé.")
                            last_ping = time.time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Waxpeer WebSocket erreur: {e} — reconnexion dans {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    async def _handle_message(self, ws, raw: str):
        if not isinstance(raw, str):
            return
        if raw == "2":
            await ws.send("3")
            return
        if raw == "3":
            return
        if not raw.startswith("42"):
            return
        try:
            payload = json.loads(raw[2:])
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(payload, list) or len(payload) < 2:
            return

        event_type = payload[0]
        if event_type == "removed":
            data = payload[1]
            if isinstance(data, dict):
                item_id = str(data.get("item_id", ""))
                if item_id and self.on_removed:
                    self.on_removed({
                        "id": f"waxpeer_{item_id}",
                        "name": data.get("name", ""),
                        "price": data.get("price", 0),
                    })
            return

        if event_type != "new":
            return
        data = payload[1]
        if not isinstance(data, dict):
            return
        if data.get("game") != "csgo":
            return

        item_id = str(data.get("item_id", ""))
        if not item_id or not self._processed_ids.add(item_id):
            return

        normalized = self._normalize_item(data)
        if normalized:
            self.callback(normalized)

    def _normalize_item(self, data: dict) -> Optional[dict]:
        name = data.get("name")
        price_raw = data.get("price")
        if not name or not price_raw:
            return None

        # Waxpeer: 1000 = $1 → cents = price / 10
        price_cents = round(price_raw / 10)

        float_value = data.get("float")
        if isinstance(float_value, str):
            try:
                float_value = float(float_value)
            except ValueError:
                float_value = None

        sticker_names = data.get("sticker_names") or []
        stickers = [{"name": sn, "wear": 0.0, "value": 0, "slot": i} for i, sn in enumerate(sticker_names)]

        raw_ts = (data.get("time") or data.get("created_at") or data.get("listed_at")
                  or data.get("timestamp") or data.get("date"))
        if raw_ts is not None:
            try:
                listed_at = float(raw_ts)
                if listed_at > 1e12:
                    listed_at /= 1000.0
            except (ValueError, TypeError):
                listed_at = None
        else:
            listed_at = None

        return {
            "id": f"waxpeer_{data['item_id']}",
            "market_hash_name": name,
            "price": price_cents,
            "listed_at": listed_at,
            "type": "buy_now",
            "item": {
                "float_value": float_value,
                "stickers": stickers,
                "paint_seed": data.get("paint_index"),
            },
            "_request_start": time.perf_counter(),
        }
