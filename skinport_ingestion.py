"""
Skinport Ingestion — WebSocket saleFeed (Socket.IO + msgpack)

Architecture TTD :
  - Événement "listed"  → save_observed_listing() en DB + cache mémoire
  - Événement "sold"    → lookup par sale_id → TTD = sold_at - listed_at → HIGH confidence

Le WebSocket public ne nécessite PAS de clé API pour l'observation.
Activation : mettre SKINPORT_ENABLED = True dans config.py

Dépendances à ajouter dans requirements.txt :
  python-socketio[asyncio_client]>=5.0
  msgpack>=1.0
"""

import asyncio
import logging
import time
from typing import Callable, Optional, Any
from datetime import datetime, timezone
import socketio
import config

logger = logging.getLogger("cs2_sniper.skinport_ingestion")

# URL Socket.IO Skinport
_SKINPORT_WS_URL = "https://skinport.com"
_SOCKETIO_PATH = "/socket.io"


class SkinportIngestor:
    """
    Écoute le flux temps réel Skinport (saleFeed) pour détecter
    les listings et les ventes avec leur timestamp exact.

    Callbacks :
      on_listed(normalized_dict) — appelé quand un item est mis en vente
      on_sold(normalized_dict)   — appelé quand un item est vendu (HIGH confidence)
    """

    def __init__(
        self,
        on_listed: Callable[[dict], None],
        on_sold: Callable[[dict], None],
    ):
        self.on_listed = on_listed
        self.on_sold = on_sold
        self.is_running = False
        self._sio: Optional[socketio.AsyncClient] = None
        self._loop_task: Optional[asyncio.Task] = None

    async def start(self, session=None):
        """Démarre la connexion WebSocket en tâche de fond."""
        self.is_running = True
        self._loop_task = asyncio.create_task(self._run())

    async def stop(self):
        """Arrête proprement la connexion."""
        self.is_running = False
        if self._sio and self._sio.connected:
            try:
                await self._sio.disconnect()
            except Exception as e:
                logger.debug(f"Skinport disconnect error: {e}")
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        logger.info("Skinport Ingestion stopped.")

    async def _run(self):
        """Boucle principale avec reconnexion automatique."""
        while self.is_running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Skinport WebSocket error: {e}")
                if self.is_running:
                    logger.info("Skinport: reconnexion dans 30s...")
                    await asyncio.sleep(30)

    async def _connect_and_listen(self):
        """Établit la connexion Socket.IO et écoute les événements."""
        self._sio = socketio.AsyncClient(
            logger=False,
            engineio_logger=False,
            serializer="msgpack",       # Skinport utilise socket.io-msgpack-parser
            reconnection=False,         # On gère la reconnexion nous-mêmes
        )

        @self._sio.event
        async def connect():
            logger.info("Skinport WebSocket connecté.")
            # S'abonner au flux de ventes (app 730 = CS2, devise EUR)
            await self._sio.emit("saleFeedJoin", {
                "appid": 730,
                "currency": "EUR",
                "locale": "en",
            })
            logger.info("Skinport: abonné au saleFeed CS2.")

        @self._sio.event
        async def disconnect():
            logger.warning("Skinport WebSocket déconnecté.")

        @self._sio.event
        async def connect_error(data):
            logger.error(f"Skinport WebSocket erreur de connexion: {data}")

        @self._sio.on("saleFeed")
        async def on_sale_feed(data):
            if self.is_running:
                await self._handle_sale_feed(data)

        await self._sio.connect(
            _SKINPORT_WS_URL,
            transports=["websocket"],
            socketio_path=_SOCKETIO_PATH,
        )
        await self._sio.wait()

    async def _handle_sale_feed(self, data: Any):
        """Dispatche les événements listed/sold."""
        if not isinstance(data, dict):
            return

        event_type = data.get("eventType") or data.get("type", "")
        sales = data.get("sales") or data.get("items", [])

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
                logger.error(f"Skinport: erreur traitement événement {event_type}: {e}")

    def _normalize(self, sale: dict) -> Optional[dict]:
        """
        Normalise un objet SaleFeedSale Skinport vers le format interne du bot.

        Champs Skinport connus :
          id_ / sale_id / short_id — identifiants du listing
          asset_id / assetid       — Steam asset ID
          market_hash_name
          wear                     — float value (0.0–1.0)
          pattern                  — paint seed (entier)
          sale_price               — prix en centimes (EUR)
          stattrak                 — booléen
          stickers                 — liste de dicts {name, wear, ...}
        """
        # Identifiant stable : on préfère sale_id, fallback id_
        sale_id = (
            sale.get("sale_id")
            or sale.get("id_")
            or sale.get("id")
            or sale.get("short_id")
        )
        market_hash_name = sale.get("market_hash_name", "")

        if not sale_id or not market_hash_name:
            return None

        # Prix en centimes → USD (Skinport renvoie en centimes dans la devise demandée)
        raw_price = sale.get("sale_price") or sale.get("price") or 0
        try:
            price_cents_eur = int(raw_price)
            # Conversion EUR→USD approximative : on stocke en USD comme les autres plateformes
            # À ajuster si Skinport renvoie directement en USD selon le paramètre currency
            price_cents = price_cents_eur  # stocker tel quel, convertir à l'affichage
        except (ValueError, TypeError):
            return None

        # Filtre de prix (config.MIN/MAX_PRICE_USD en USD, Skinport en EUR — approximation)
        price_approx_usd = price_cents / 100.0
        if price_approx_usd < config.MIN_PRICE_USD or price_approx_usd > config.MAX_PRICE_USD:
            return None

        # Float value
        wear = sale.get("wear")
        try:
            float_value = float(wear) if wear is not None else None
        except (ValueError, TypeError):
            float_value = None

        if float_value is not None and float_value <= 0:
            float_value = None

        # Paint seed
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
            "price": price_cents,               # en centimes EUR
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
