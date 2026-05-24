import argparse
import asyncio
import sys
import logging
import time
from datetime import datetime
import aiohttp
from colorama import init, Fore, Back, Style

# Initialiser colorama pour Windows
init(autoreset=True)

# Configurer stdout et stderr pour UTF-8 sur Windows
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

import config
from observer import MarketObserver
from observation_ingestion import ObservationIngestor

# Setup logging
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Handler console avec couleurs
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter(
    f"{Fore.LIGHTBLACK_EX}%(asctime)s [%(levelname)s] %(name)s: %(message)s{Style.RESET_ALL}"
)
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)

# Handler fichier sans couleurs
file_handler = logging.FileHandler("cs2_observer.log", encoding="utf-8")
file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
file_handler.setFormatter(file_formatter)
root_logger.addHandler(file_handler)

logger = logging.getLogger("cs2_sniper.observer_main")


def print_banner(platform: str):
    banner = f"""
{Fore.CYAN}{Style.BRIGHT}======================================================================
{Fore.MAGENTA}{Style.BRIGHT}        COUNTER-STRIKE 2 - COLLECTE DE DATASET D'OBSERVATION
{Fore.CYAN}{Style.BRIGHT}======================================================================
{Fore.WHITE} Mode             : {Fore.GREEN}Collecte Indépendante de Dataset
{Fore.WHITE} Plateforme(s)    : {Fore.YELLOW}{platform.upper()}
{Fore.WHITE} BD de destination: {Fore.GREEN}{config.OBSERVER_DB_PATH}
{Fore.WHITE} Seuil Snipe Bot  : {Fore.RED}< {config.OBS_BOT_SNIPE_TTD_MS}ms
{Fore.WHITE} Seuil Achat Rap. : {Fore.YELLOW}< {config.OBS_FAST_HUMAN_TTD_MS // 1000}s
{Fore.WHITE} Limite TTD Max   : {Fore.GREEN}{config.OBSERVER_MAX_TTD_SEC // 60} minutes ({config.OBSERVER_MAX_TTD_SEC}s)
{Fore.WHITE} Double-vérif API : {Fore.BLUE}Activée uniquement pour TTD < 30s (anti rate-limit)
{Fore.CYAN}{Style.BRIGHT}======================================================================
{Fore.LIGHTBLACK_EX} Démarrage du tracking... (Ctrl+C pour arrêter et exporter le dataset)
"""
    print(banner)


async def main():
    parser = argparse.ArgumentParser(description="CS2 Sniper Bot - Collecteur de Dataset d'Observation")
    parser.add_argument(
        "--platform",
        choices=["dmarket", "csfloat", "waxpeer", "market_csgo", "all"],
        default="all",
        help="Plateforme à observer (par défaut : 'all')"
    )
    args = parser.parse_args()

    print_banner(args.platform)

    # Création du client HTTP de session avec cache DNS et réutilisation TCP Keep-Alive
    connector = aiohttp.TCPConnector(
        use_dns_cache=True,
        ttl_dns_cache=300,
        limit=100,
        keepalive_timeout=30
    )
    
    async with aiohttp.ClientSession(connector=connector) as session:
        observer = MarketObserver()
        ingestor = ObservationIngestor(observer=observer, platform=args.platform)
        
        await ingestor.start(session=session)
        
        try:
            # Garder le script en marche
            while ingestor.is_running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt reçu. Arrêt propre de l'observer...")
        finally:
            await ingestor.stop()
            print(f"\n{Fore.GREEN}{Style.BRIGHT}✔ Dataset exporté et enregistré avec succès dans {config.OBSERVER_DB_PATH} !")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nObservation arrêtée proprement.")
