"""
Skinport Ingestion — WebSocket brut (Socket.IO EIO4 / JSON, sans python-socketio)

Architecture TTD :
  - Événement "listed"  → save_observed_listing() en DB + cache mémoire
  - Événement "sold"    → lookup par sale_id → TTD = sold_at - listed_at → HIGH confidence

Le WebSocket public ne nécessite PAS de clé API pour l'observation.
Activation : mettre SKINPORT_ENABLED = True dans config.py
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional, Any
from datetime import datetime, timezone
import websockets
import config

logger = logging.getLogger("cs2_sniper.skinport_ingestion")

_WS_URL = "wss://skinport.com/socket.io/?EIO=4&transport=websocket"


class SkinportIngestor:
    """
    Écoute le flux temps réel Skinport (saleFeed) via WebSocket Socket.IO brut.

    Callbacks :
      on_listed(normalized_dict) — item mis en vente
      on_sold(normalized_dict)   — item vendu (HIGH confidence, événement confirmé)
    """

    def __init__(
        self,
        on_listed: Callable[[dict], None],
        on_sold: Callable[[dict], None],
    ):
        self.on_listed = on_listed
        self.on_sold = on_sold
        self.is_running = False
        self._ws = None
        self._loop_task: Optional[asyncio.Task] = None

    async def start(self, session=None):
        self.is_running = True
        self._loop_task = asyncio.create_task(self._run_websocket())

    async def stop(self):
        self.is_running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        logger.info("Skinport Ingestion stopped.")

    async def _run_websocket(self):
        retry_delay = 5

        while self.is_running:
            try:
                async with websockets.connect(
                    _WS_URL,
                    additional_headers={
                        "Origin": "https://skinport.com",
                        "Referer": "https://skinport.com/",
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                    },
                    ping_interval=None,
                    max_size=10 * 1024 * 1024,
                    open_timeout=15,
                ) as ws:
                    self._ws = ws
                    logger.info("Skinport WebSocket connecté.")
                    retry_delay = 5

                    # EIO handshake : recevoir le paquet "0{...}" avec sid/pingInterval
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    ping_interval = 25.0
                    if isinstance(raw, str) and raw.startswith("0"):
                        try:
                            hs = json.loads(raw[1:])
                            ping_interval = hs.get("pingInterval", 25000) / 1000
                            logger.info(f"Skinport EIO: pingInterval={ping_interval}s")
                        except Exception:
                            pass

                    # Socket.IO namespace connect
                    await ws.send("40")

                    # Attendre l'ACK de connexion "40{...}"
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        if isinstance(raw, str) and raw == "2":
                            await ws.send("3")
                            continue
                        if isinstance(raw, str) and raw.startswith("40"):
                            break

                    # S'abonner au flux saleFeed CS2
                    await ws.send('42["saleFeedJoin",{"appid":730,"currency":"EUR","locale":"en"}]')
                    logger.info("Skinport: abonné au saleFeed CS2.")

                    last_ping = time.time()

                    while self.is_running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            await self._handle_message(ws, raw)
                        except asyncio.TimeoutError:
                            pass

                        # EIO ping keepalive
                        if time.time() - last_ping >= ping_interval - 2:
                            await ws.send("2")
                            last_ping = time.time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Skinport WebSocket erreur: {e} — reconnexion dans {retry_delay}s")
                if self.is_running:
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60)

    async def _handle_message(self, ws, raw: Any):
        if isinstance(raw, bytes):
            # Skinport peut envoyer des frames binaires (msgpack) — on les ignore
            # si on ne peut pas les parser sans msgpack
            try:
                import msgpack
                data = msgpack.unpackb(raw[1:], raw=False)  # skip EIO prefix byte
                await self._dispatch(data)
            except Exception:
                logger.debug(f"Skinport: frame binaire ignorée ({len(raw)} bytes)")
            return

        if not isinstance(raw, str):
            return
        if raw == "2":          # EIO ping
            await ws.send("3")
            return
        if raw == "3":          # EIO pong
            return
        if not raw.startswith("42"):
            return

        # Socket.IO event : "42[\"eventName\", payload]"
        try:
            payload = json.loads(raw[2:])
        except Exception:
            return

        if not isinstance(payload, list) or len(payload) < 2:
            return

        event_name = payload[0]
        data = payload[1]

        if event_name == "saleFeed":
            await self._dispatch(data)

    async def _dispatch(self, data: Any):
        if not isinstance(data, dict):
            return

        event_type = data.get("eventType") or data.get("type", "")
        sales = data.get("sales") or data.get("items") or []

        if isinstance(sales, dict):
            sales = [sales]
        if not isinstance(sales, list):
            return

        for sale in sales:
            try:
                normalized = self._normalize(sale)
                if normalized is None:
                    continue
                if event_type == "listed":
                    self.on_listed(normalized)
                elif event_type == "sold":
                    self.on_sold(normalized)
            except Exception as e:
                logger.error(f"Skinport: erreur traitement {event_type}: {e}")

    def _normalize(self, sale: dict) -> Optional[dict]:
        sale_id = (
            sale.get("sale_id")
            or sale.get("id_")
            or sale.get("id")
            or sale.get("short_id")
        )
        market_hash_name = sale.get("market_hash_name", "")

        if not sale_id or not market_hash_name:
            return None

        raw_price = sale.get("sale_price") or sale.get("price") or 0
        try:
            price_cents = int(raw_price)
        except (ValueError, TypeError):
            return None

        price_approx_usd = price_cents / 100.0
        if price_approx_usd < config.MIN_PRICE_USD or price_approx_usd > config.MAX_PRICE_USD:
            return None

        wear = sale.get("wear")
        try:
            float_value = float(wear) if wear is not None else None
        except (ValueError, TypeError):
            float_value = None
        if float_value is not None and float_value <= 0:
            float_value = None

        pattern = sale.get("pattern")
        try:
            paint_seed = int(pattern) if pattern is not None else None
        except (ValueError, TypeError):
            paint_seed = None

        stickers = self._parse_stickers(sale.get("stickers"))

        return {
            "id": f"skinport_{sale_id}",
            "sale_id": str(sale_id),
            "asset_id": sale.get("asset_id") or sale.get("assetid"),
            "market_hash_name": market_hash_name,
            "price": price_cents,
            "float_value": float_value,
            "paint_seed": paint_seed,
            "stickers": stickers,
            "platform": "skinport",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    def _parse_stickers(self, stickers_raw: Any) -> list:
        if not stickers_raw or not isinstance(stickers_raw, list):
            return []
        result = []
        for i, s in enumerate(stickers_raw):
            if isinstance(s, dict):
                result.append({
                    "name": s.get("name") or s.get("market_hash_name") or "",
                    "wear": float(s.get("wear") or 0.0),
                    "value": 0,
                    "slot": s.get("slot") or i,
                })
        return result
