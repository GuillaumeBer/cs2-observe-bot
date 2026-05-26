"""
Skinport Playwright Ingestor — Interception WebSocket via navigateur réel

Résultats de l'investigation (2026-05-25) :
  - Le saleFeed Socket.IO de Skinport NÉCESSITE un login utilisateur.
  - Sans authentification, le polling /socket.io/?transport=polling retourne 403.
  - Le namespace connect Socket.IO ("40") est rejeté (1005) sans session polling préalable.
  - La page Skinport elle-même ne connecte JAMAIS le Socket.IO sans être loguée.
  - Oracle Cloud = blocage IP dur (erreur 1005 Cloudflare, non contournable).

Stratégie (avec credentials) :
  1. Playwright navigue sur skinport.com et se connecte au compte Skinport
  2. Une fois loggé, la page ouvre automatiquement le Socket.IO → on intercepte les frames
  3. Callbacks on_listed / on_sold reçoivent les événements saleFeed avec TTD HIGH confidence

Prérequis :
  - Compte Skinport (email + password) → SKINPORT_EMAIL + SKINPORT_PASSWORD dans .env
  - IP résidentielle (pas de datacenter Oracle Cloud)
  - pip install playwright playwright-stealth && playwright install chromium

Limitations :
  - Nécessite Playwright + Chromium (~200MB)
  - NE FONCTIONNE PAS depuis Oracle Cloud (erreur 1005 = blocage IP datacenter)
  - Fonctionne depuis une IP résidentielle

Installation :
  pip install playwright playwright-stealth && playwright install chromium

Usage : instancier SkinportPlaywrightIngestor en lieu et place de SkinportIngestor
        quand SKINPORT_USE_PLAYWRIGHT = True
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger("cs2_sniper.skinport_cf_bypass")

_SKINPORT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_SKINPORT_MARKET_URL = "https://skinport.com/market?cat=Rifle&sort=date&order=desc"

# Délai de reconnexion après fermeture du WS (la page Skinport reconnecte souvent)
_RECONNECT_HOLD_SEC = 10
# Délai de reload de la page si plus aucun WS actif
_PAGE_RELOAD_SEC = 120


class SkinportPlaywrightIngestor:
    """
    Écoute le flux saleFeed Skinport en interceptant le WebSocket
    que la page skinport.com/market ouvre nativement via Playwright.

    Même interface que SkinportIngestor :
      on_listed(normalized_dict) — item mis en vente
      on_sold(normalized_dict)   — item vendu
    """

    def __init__(
        self,
        on_listed: Callable[[dict], None],
        on_sold: Callable[[dict], None],
        headless: bool = True,
        email: str = "",
        password: str = "",
    ):
        self.on_listed = on_listed
        self.on_sold = on_sold
        self.is_running = False
        self._headless = headless
        self._email = email
        self._password = password
        self._main_task: Optional[asyncio.Task] = None
        self._last_frame_ts: float = 0.0
        self._ws_count: int = 0

    async def start(self, session=None):
        self.is_running = True
        self._main_task = asyncio.create_task(self._run())

    async def stop(self):
        self.is_running = False
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        logger.info("SkinportPlaywrightIngestor stopped.")

    async def _run(self):
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError(
                "Playwright non installé.\n"
                "Exécuter : pip install playwright playwright-stealth && playwright install chromium"
            )

        while self.is_running:
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        headless=self._headless,
                        args=[
                            "--no-sandbox",
                            "--disable-dev-shm-usage",
                            "--disable-blink-features=AutomationControlled",
                        ],
                    )
                    context = await browser.new_context(
                        user_agent=_SKINPORT_UA,
                        viewport={"width": 1280, "height": 800},
                        extra_http_headers={
                            "Accept-Language": "en-US,en;q=0.9",
                        },
                    )

                    await context.add_init_script("""
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
                        window.chrome = {runtime: {}, loadTimes: function(){}, app: {}};
                    """)

                    try:
                        from playwright_stealth import stealth_async
                        page = await context.new_page()
                        await stealth_async(page)
                    except ImportError:
                        page = await context.new_page()

                    self._last_frame_ts = time.time()
                    self._ws_count = 0

                    # Intercepter les WebSockets ouverts par la page
                    page.on("websocket", self._handle_ws_opened)

                    logger.info("SkinportPlaywright: navigation vers skinport.com...")
                    await page.goto("https://skinport.com/", wait_until="domcontentloaded", timeout=45_000)

                    # Attendre que le challenge CF soit résolu
                    for _ in range(20):
                        title = await page.title()
                        if not any(w in title.lower() for w in ["just a moment", "challenge", "attention"]):
                            logger.info("SkinportPlaywright: page chargée (title='%s')", title)
                            break
                        await asyncio.sleep(1.5)

                    # Connexion au compte Skinport si credentials fournis
                    if self._email and self._password:
                        logged_in = await self._login(page)
                        if logged_in:
                            logger.info("SkinportPlaywright: connecté en tant que %s", self._email)
                        else:
                            logger.warning("SkinportPlaywright: échec login — saleFeed inaccessible sans auth")
                    else:
                        logger.warning(
                            "SkinportPlaywright: aucun credentials fournis "
                            "(SKINPORT_EMAIL/SKINPORT_PASSWORD) — le saleFeed nécessite un login"
                        )

                    # Naviguer vers le market pour déclencher le Socket.IO saleFeed
                    await page.goto(_SKINPORT_MARKET_URL, wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(3)

                    logger.info("SkinportPlaywright: en écoute des frames WebSocket...")

                    # Garder la page vivante et surveiller l'activité
                    while self.is_running:
                        await asyncio.sleep(10)
                        idle_sec = time.time() - self._last_frame_ts

                        if idle_sec > _PAGE_RELOAD_SEC and self._ws_count == 0:
                            logger.warning(
                                "SkinportPlaywright: aucun WS actif depuis %.0fs → reload page",
                                idle_sec,
                            )
                            try:
                                await page.reload(wait_until="domcontentloaded", timeout=30_000)
                                self._last_frame_ts = time.time()
                            except Exception as e:
                                logger.error("SkinportPlaywright: reload échoué: %s", e)
                                break  # On va relancer le browser

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("SkinportPlaywright: erreur browser: %s — redémarrage dans 30s", e)
                if self.is_running:
                    await asyncio.sleep(30)

    async def _login(self, page: Any) -> bool:
        """
        Se connecte au compte Skinport via le formulaire /signin.
        Le formulaire utilise Cloudflare Turnstile (CAPTCHA) — fonctionne mieux en mode headed.
        Retourne True si le login a réussi.
        """
        try:
            logger.info("SkinportPlaywright: navigation vers /signin...")
            await page.goto("https://skinport.com/signin", wait_until="domcontentloaded", timeout=20_000)
            await asyncio.sleep(3)

            title = await page.title()
            url = page.url
            logger.info("SkinportPlaywright: signin page: url=%s  title='%s'", url[:60], title[:40])

            # Accepter les cookies si le banner est présent
            try:
                cookie_btn = page.locator('button[type="submit"]:has-text("ACCEPTE"), button:has-text("Accept")')
                if await cookie_btn.count() > 0:
                    await cookie_btn.first.click(timeout=3000)
                    await asyncio.sleep(1)
            except Exception:
                pass

            # Remplir le champ email/username
            email_field = page.locator('input[name="email"], input#email')
            await email_field.first.fill(self._email, timeout=8000)
            logger.info("SkinportPlaywright: email rempli")

            # Remplir le mot de passe
            pwd_field = page.locator('input[type="password"], input#password, input[name="password"]')
            await pwd_field.first.fill(self._password, timeout=5000)
            logger.info("SkinportPlaywright: password rempli")

            # Attendre un peu pour le Turnstile (Cloudflare CAPTCHA invisible)
            await asyncio.sleep(3)

            # Cliquer sur SE CONNECTER
            submit = page.locator('button[type="submit"]:has-text("SE CONNECTER"), button[type="submit"]:has-text("Sign in"), button[type="submit"]:has-text("Log in")')
            count = await submit.count()
            if count == 0:
                # Fallback : premier bouton submit
                submit = page.locator('button[type="submit"]')
            await submit.first.click(timeout=5000)
            logger.info("SkinportPlaywright: bouton submit cliqué")

            # Attendre la redirection post-login
            await asyncio.sleep(5)

            current_url = page.url
            title = await page.title()
            # Succès si on n'est plus sur la page signin
            is_logged = ("signin" not in current_url and "se-connecter" not in current_url
                         and "login" not in current_url.lower())
            logger.info("SkinportPlaywright: post-login url=%s  logged=%s", current_url[:60], is_logged)

            if not is_logged:
                # Vérifier s'il y a un message d'erreur
                error_text = await page.evaluate("""
                    () => {
                        const err = document.querySelector('.FormGroup-error, .error-message, [class*="error"]');
                        return err ? err.innerText.trim() : '';
                    }
                """)
                if error_text:
                    logger.warning("SkinportPlaywright: erreur formulaire: %s", error_text[:100])

            return is_logged

        except Exception as e:
            logger.error("SkinportPlaywright: erreur login: %s", e)
            return False

    def _handle_ws_opened(self, ws: Any):
        """Appelé quand la page ouvre un WebSocket."""
        url = ws.url
        if "socket.io" not in url and "skinport" not in url:
            return

        self._ws_count += 1
        logger.info("SkinportPlaywright: WebSocket intercepté [#%d]: %s", self._ws_count, url[:80])

        ws.on("framereceived", self._handle_frame_received)
        ws.on("framesent", self._handle_frame_sent)
        ws.on("close", lambda: self._handle_ws_closed())

    def _handle_ws_closed(self):
        self._ws_count = max(0, self._ws_count - 1)
        logger.debug("SkinportPlaywright: WS fermé (restants: %d)", self._ws_count)

    def _handle_frame_sent(self, frame: Any):
        payload = frame.get("payload", "") if isinstance(frame, dict) else str(frame)
        logger.debug("SkinportPlaywright → SENT: %s", str(payload)[:120])

    def _handle_frame_received(self, frame: Any):
        """Appelé pour chaque frame reçue depuis le serveur Skinport."""
        self._last_frame_ts = time.time()

        payload = frame.get("payload", "") if isinstance(frame, dict) else str(frame)

        if not isinstance(payload, str):
            return
        if not payload.startswith("42"):
            return

        try:
            data_list = json.loads(payload[2:])
        except Exception:
            return

        if not isinstance(data_list, list) or len(data_list) < 2:
            return

        event_name = data_list[0]
        data = data_list[1]

        if event_name != "saleFeed":
            return

        self._dispatch(data)

    def _dispatch(self, data: Any):
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
                logger.error("SkinportPlaywright: erreur dispatch %s: %s", event_type, e)

    def _normalize(self, sale: dict):
        import config

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
