"""
observer.py — Mode Observation : Bot Velocity Tracking

Surveille les snapshots successifs des listings de marché et détecte
quels items disparaissent rapidement, révélant le comportement des bots concurrents.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable
import config
from transaction_db import TransactionDatabase

logger = logging.getLogger("cs2_sniper.observer")


class MarketObserver:
    """
    Suit les snapshots successifs de listings et détecte les items
    qui disparaissent dans un délai court (snipe bot vs achat humain).

    Utilisation :
        observer = MarketObserver()
        # À chaque cycle de polling :
        observer.record_snapshot(listings_list, platform="dmarket")
        # Pour exporter :
        observer.export_results()
    """

    def __init__(self, db_path: Optional[str] = None, new_listing_callback: Optional[Callable[[dict], None]] = None):
        self.new_listing_callback = new_listing_callback
        # Snapshot courant : {listing_id: {name, price_cents, first_seen_ts, platform}}
        self._active_listings: Dict[str, dict] = {}

        # Items disparus du top-100 en attente de confirmation : {listing_id: {..., absent_cycles}}
        # Un item absent N cycles consécutifs sans réapparaître est considéré vraiment vendu.
        self._pending_verification: Dict[str, dict] = {}

        # Items en cours de double-vérification (pour éviter les faux-positifs)
        self._under_verification: Dict[str, dict] = {}

        # Catalogue des items chauds : {market_hash_name: {stats...}}
        self._hot_items: Dict[str, dict] = {}

        # Historique de prix observés par skin : {market_hash_name: [price_cents, ...]}
        # Limité aux 50 derniers prix pour calculer la référence médiane.
        self._price_history: Dict[str, List[int]] = {}
        
        # Base de données SQLite pour les transactions réelles (décorrélée par défaut)
        self._db = TransactionDatabase(db_path=db_path) if db_path else TransactionDatabase(db_path=config.OBSERVER_DB_PATH)

        # Anti-doublons du callback : {listing_id: timestamp de dernier déclenchement}
        # Un listing ne déclenche le callback qu'une fois toutes les CALLBACK_COOLDOWN_SEC secondes.
        self._callback_cooldown: Dict[str, float] = {}
        self._CALLBACK_COOLDOWN_SEC: float = 300.0  # 5 minutes

        # Compteurs globaux de session
        self._session_start: float = time.time()
        self._total_listings_seen: int = 0
        self._total_disappearances: int = 0
        self._bot_snipes: int = 0
        self._fast_humans: int = 0
        self._poll_cycles: int = 0

        # Timestamp du dernier export
        self._last_export_time: float = time.time()

        logger.info(
            f"MarketObserver initialisé. "
            f"Seuil snipe bot : {config.OBS_BOT_SNIPE_TTD_MS}ms | "
            f"Seuil achat rapide : {config.OBS_FAST_HUMAN_TTD_MS}ms | "
            f"Confirmation : {config.OBS_CONFIRMATION_CYCLES} cycles absents"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # INTERFACE PRINCIPALE
    # ──────────────────────────────────────────────────────────────────────────

    def record_snapshot(self, listings: List[dict], platform: str = "unknown", auto_confirm: bool = True, skin_name: Optional[str] = None) -> List[dict]:
        """
        Compare le nouveau snapshot avec l'état précédent.
        Chaque listing qui a disparu est analysé (calcul du TTD).

        :param listings: Liste de dicts normalisés (doivent avoir 'id', 'market_hash_name', 'price').
        :param platform: Nom de la plateforme ("dmarket", "csfloat", etc.).
        :param auto_confirm: Si True, confirme la vente sans double-vérification (pour compatibilité / tests).
        :return: Liste de dicts des candidats à la double-vérification.
        """
        self._poll_cycles += 1
        now = time.time()

        # Construire l'index du snapshot courant
        current_ids: Dict[str, dict] = {}
        for listing in listings:
            listing_id = listing.get("id")
            name = listing.get("market_hash_name")
            price = listing.get("price")  # en cents

            if not listing_id or not name or price is None:
                continue

            current_ids[listing_id] = {
                "name": name,
                "price_cents": price,
                "platform": platform,
                "offer_id": listing.get("offer_id", listing_id),
                "listed_at": listing.get("listed_at"),
                "float_value": listing.get("float_value"),
                "paint_seed": listing.get("paint_seed"),
                "sticker_count": listing.get("sticker_count", 0),
                "sticker_names": listing.get("sticker_names", []),
                "cheapest_by_sa": listing.get("cheapest_by_sa", False),
            }

        self._total_listings_seen += len(current_ids)
        current_id_set = set(current_ids.keys())

        # Étape 1 : traiter les items en attente de confirmation
        # Un item qui réapparaît dans le top-100 est un faux positif (il avait juste été
        # évincé par de nouveaux listings, sans avoir été vendu).
        confirmed_this_cycle: Dict[str, dict] = {}  # market_hash_name → best candidate
        candidates_to_verify: List[dict] = []
        _dbg_reappeared = _dbg_slow = _dbg_fast = 0

        for listing_id in list(self._pending_verification.keys()):
            entry = self._pending_verification[listing_id]
            
            # Ne décompter l'absence d'un skin B que si le snapshot courant concerne Skin B (ou est global)
            if skin_name is not None and entry["name"] != skin_name:
                continue
                
            absent_first_seen = entry.get("absent_first_seen_ts", now)
            ttd_ms = (absent_first_seen - entry["first_seen_ts"]) * 1000

            if listing_id in current_id_set:
                del self._pending_verification[listing_id]
                _dbg_reappeared += 1
            else:
                _dbg_fast += 1
                entry["absent_cycles"] += 1
                if entry["absent_cycles"] >= config.OBS_CONFIRMATION_CYCLES:
                    confirmed = self._pending_verification.pop(listing_id)
                    confirmed["listing_id"] = listing_id
                    
                    first_seen = confirmed["first_seen_ts"]
                    last_seen = confirmed.get("last_seen_ts", first_seen)
                    
                    ttd_min_ms = max(0.0, (last_seen - first_seen) * 1000)
                    ttd_max_ms = max(ttd_min_ms, (absent_first_seen - first_seen) * 1000)
                    
                    confirmed["ttd_ms"] = ttd_max_ms
                    confirmed["ttd_min_ms"] = ttd_min_ms
                    confirmed["ttd_max_ms"] = ttd_max_ms
                    
                    if auto_confirm:
                        name = confirmed["name"]
                        # Garder uniquement le listing au prix le plus bas pour ce skin
                        if name not in confirmed_this_cycle or confirmed["price_cents"] < confirmed_this_cycle[name]["price_cents"]:
                            confirmed_this_cycle[name] = confirmed
                    else:
                        self._under_verification[listing_id] = confirmed
                        candidates_to_verify.append(confirmed)

        if _dbg_reappeared or _dbg_slow or _dbg_fast or confirmed_this_cycle or candidates_to_verify:
            logger.debug(
                f"Cycle {self._poll_cycles} — pending traité : "
                f"{_dbg_reappeared} réapparus | {_dbg_slow} ventes lentes | "
                f"{_dbg_fast} en cours | {len(confirmed_this_cycle)} confirmés | "
                f"{len(candidates_to_verify)} à vérifier"
            )

        for confirmed in confirmed_this_cycle.values():
            self._total_disappearances += 1
            self._process_disappearance(
                market_hash_name=confirmed["name"],
                price_cents=confirmed["price_cents"],
                ttd_ms=confirmed["ttd_ms"],
                platform=confirmed["platform"],
                ttd_from_listing=confirmed.get("ttd_from_listing", False),
                float_value=confirmed.get("float_value"),
                paint_seed=confirmed.get("paint_seed"),
                sticker_count=confirmed.get("sticker_count", 0),
                sticker_names=confirmed.get("sticker_names", []),
                cheapest_by_sa=confirmed.get("cheapest_by_sa", False),
                ttd_min_ms=confirmed.get("ttd_min_ms"),
                ttd_max_ms=confirmed.get("ttd_max_ms"),
            )

        # Étape 2 : déplacer les items nouvellement absents vers la file d'attente
        for listing_id in list(self._active_listings.keys()):
            entry = self._active_listings[listing_id]
            if entry["platform"] == platform:
                # Si c'est un snapshot partiel spécifique à un skin, on ne compare que les items de ce skin
                if skin_name is not None and entry["name"] != skin_name:
                    continue
                if listing_id not in current_id_set:
                    entry = self._active_listings.pop(listing_id)
                    self._pending_verification[listing_id] = {
                        **entry,
                        "absent_cycles": 1,
                        "absent_first_seen_ts": now,
                    }

        # Étape 3 : enregistrer les nouveaux items visibles dans le snapshot actif
        # et mettre à jour l'historique de prix pour la référence médiane.
        for listing_id, data in current_ids.items():
            name = data["name"]
            price = data["price_cents"]

            # Mise à jour de l'historique de prix (50 derniers prix max)
            hist = self._price_history.setdefault(name, [])
            hist.append(price)
            if len(hist) > 50:
                hist.pop(0)

            if listing_id not in self._active_listings:
                listed_at = data.get("listed_at")
                first_seen_ts = listed_at if listed_at and listed_at <= now else now
                self._active_listings[listing_id] = {
                    "name": name,
                    "price_cents": price,
                    "platform": data["platform"],
                    "offer_id": data["offer_id"],
                    "first_seen_ts": first_seen_ts,
                    "last_seen_ts": now,
                    "ttd_from_listing": listed_at is not None and listed_at <= now,
                    "float_value": data.get("float_value"),
                    "paint_seed": data.get("paint_seed"),
                    "sticker_count": data.get("sticker_count", 0),
                    "sticker_names": data.get("sticker_names", []),
                    "cheapest_by_sa": data.get("cheapest_by_sa", False),
                }
                if self.new_listing_callback:
                    last_fired = self._callback_cooldown.get(listing_id, 0.0)
                    if now - last_fired >= self._CALLBACK_COOLDOWN_SEC:
                        self._callback_cooldown[listing_id] = now
                        try:
                            self.new_listing_callback(data)
                        except Exception as e:
                            logger.error(f"Erreur dans le callback de nouveau listing : {e}")
                    else:
                        logger.debug(
                            f"Callback ignoré pour {listing_id} (cooldown {self._CALLBACK_COOLDOWN_SEC:.0f}s)"
                        )
            else:
                self._active_listings[listing_id]["last_seen_ts"] = now

        # Export automatique si l'intervalle est écoulé
        if now - self._last_export_time >= config.OBS_EXPORT_INTERVAL_SEC:
            self.export_results()
            self._last_export_time = now

        return candidates_to_verify

    def confirm_disappearance(self, listing_id: str, is_still_active: bool) -> None:
        """
        Traite le résultat de la double-vérification d'un item disparu.
        """
        entry = self._under_verification.pop(listing_id, None)
        if not entry:
            return

        if is_still_active:
            # Faux positif, l'item est toujours en vente, on l'oublie
            logger.debug(f"Verification : {entry['name']} est toujours actif sur le marché (faux positif évité)")
            # Restaure son first_seen_ts d'origine s'il a réapparu dans le snapshot actif
            if listing_id in self._active_listings:
                self._active_listings[listing_id]["first_seen_ts"] = entry["first_seen_ts"]
                self._active_listings[listing_id]["ttd_from_listing"] = entry.get("ttd_from_listing", False)
        else:
            # Vente confirmée !
            logger.debug(f"Verification : {entry['name']} a disparu du marché (vente confirmée)")
            self._total_disappearances += 1
            self._process_disappearance(
                market_hash_name=entry["name"],
                price_cents=entry["price_cents"],
                ttd_ms=entry["ttd_ms"],
                platform=entry["platform"],
                ttd_from_listing=entry.get("ttd_from_listing", False),
                float_value=entry.get("float_value"),
                paint_seed=entry.get("paint_seed"),
                sticker_count=entry.get("sticker_count", 0),
                sticker_names=entry.get("sticker_names", []),
                cheapest_by_sa=entry.get("cheapest_by_sa", False),
                ttd_min_ms=entry.get("ttd_min_ms"),
                ttd_max_ms=entry.get("ttd_max_ms"),
                confidence=entry.get("confidence", "MEDIUM"),
            )

    # ──────────────────────────────────────────────────────────────────────────
    # LOGIQUE DE CLASSIFICATION DES DISPARITIONS
    # ──────────────────────────────────────────────────────────────────────────

    def _median_price_usd(self, market_hash_name: str, exclude_price_cents: int) -> Optional[float]:
        """
        Retourne le prix médian observé pour ce skin en USD,
        en excluant le prix du snipe lui-même pour éviter un biais vers le bas.
        Retourne None si l'historique est insuffisant (< 3 observations distinctes).
        """
        hist = self._price_history.get(market_hash_name, [])
        # Exclure une occurrence du prix snipé pour ne pas biaiser la médiane
        candidates = list(hist)
        try:
            candidates.remove(exclude_price_cents)
        except ValueError:
            pass
        if len(candidates) < 3:
            return None
        candidates.sort()
        mid = len(candidates) // 2
        if len(candidates) % 2 == 0:
            median_cents = (candidates[mid - 1] + candidates[mid]) / 2
        else:
            median_cents = candidates[mid]
        return median_cents / 100.0

    def _log_snipe(
        self,
        market_hash_name: str,
        price_usd: float,
        ttd_ms: float,
        platform: str,
        category: str,
        ref_price_usd: Optional[float],
        discount_pct: Optional[float],
        float_value: Optional[float] = None,
        paint_seed: Optional[int] = None,
        sticker_count: int = 0,
        sticker_names: Optional[List[str]] = None,
        cheapest_by_sa: bool = False,
        ttd_min_ms: Optional[float] = None,
        ttd_max_ms: Optional[float] = None,
    ) -> None:
        """Persiste un événement de snipe dans data/snipe_log.jsonl."""
        if ttd_min_ms is None:
            ttd_min_ms = ttd_ms
        if ttd_max_ms is None:
            ttd_max_ms = ttd_ms

        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "market_hash_name": market_hash_name,
            "price_usd": round(price_usd, 2),
            "ttd_ms": round(ttd_ms, 0),
            "ttd_min_ms": round(ttd_min_ms, 0),
            "ttd_max_ms": round(ttd_max_ms, 0),
            "platform": platform,
            "category": category,
            "ref_price_usd": round(ref_price_usd, 2) if ref_price_usd is not None else None,
            "discount_pct": round(discount_pct, 1) if discount_pct is not None else None,
            "float_value": round(float_value, 6) if float_value is not None else None,
            "paint_seed": paint_seed,
            "sticker_count": sticker_count,
            "sticker_names": sticker_names or [],
            "cheapest_by_sa": cheapest_by_sa,
        }
        try:
            config.OBSERVATION_SNIPE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(config.OBSERVATION_SNIPE_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error(f"Erreur écriture snipe_log : {e}")

    def _process_disappearance(
        self,
        market_hash_name: str,
        price_cents: int,
        ttd_ms: float,
        platform: str,
        ttd_from_listing: bool = False,
        float_value: Optional[float] = None,
        paint_seed: Optional[int] = None,
        sticker_count: int = 0,
        sticker_names: Optional[List[str]] = None,
        cheapest_by_sa: bool = False,
        ttd_min_ms: Optional[float] = None,
        ttd_max_ms: Optional[float] = None,
        confidence: str = "LOW",
    ) -> None:
        """
        Classifie une disparition et met à jour le catalogue _hot_items.
        """
        if ttd_min_ms is None:
            ttd_min_ms = ttd_ms
        if ttd_max_ms is None:
            ttd_max_ms = ttd_ms

        price_usd = price_cents / 100.0
        ttd_source = "depuis mise en vente" if ttd_from_listing else "depuis 1ère obs."

        ref_price_usd = self._median_price_usd(market_hash_name, price_cents)
        if ref_price_usd and ref_price_usd > 0:
            discount_pct = (ref_price_usd - price_usd) / ref_price_usd * 100
        else:
            discount_pct = None

        # Indicateurs annexes pour le log console
        sa_flag = " [SA:cheapest]" if cheapest_by_sa else ""
        float_str = f" float:{float_value:.4f}" if float_value is not None else ""
        sticker_str = f" stickers:{sticker_count}" if sticker_count > 0 else ""
        discount_str = f" | ref ${ref_price_usd:.2f} -{discount_pct:.0f}%" if (
            ref_price_usd and discount_pct is not None
        ) else " | ref: insuff."

        # Format TTD
        if ttd_min_ms != ttd_max_ms:
            ttd_range_str = f"[{ttd_min_ms/1000:.1f}s - {ttd_max_ms/1000:.1f}s]"
        else:
            ttd_range_str = f"{ttd_ms/1000:.1f}s"

        if ttd_max_ms < config.OBS_BOT_SNIPE_TTD_MS:
            category = "BOT_SNIPE"
            self._bot_snipes += 1
            logger.info(
                f"BOT SNIPE -- {market_hash_name} "
                f"| TTD: {ttd_range_str} ({ttd_source}) | ${price_usd:.2f}{discount_str}"
                f"{sa_flag}{float_str}{sticker_str} | {platform.upper()}"
            )
            self._log_snipe(
                market_hash_name, price_usd, ttd_ms, platform, category,
                ref_price_usd, discount_pct,
                float_value, paint_seed, sticker_count, sticker_names, cheapest_by_sa,
                ttd_min_ms=ttd_min_ms, ttd_max_ms=ttd_max_ms,
            )
            self._update_hot_item(
                market_hash_name, price_usd, ttd_ms, platform, category,
                ttd_min_ms=ttd_min_ms, ttd_max_ms=ttd_max_ms
            )

        elif ttd_max_ms < config.OBS_FAST_HUMAN_TTD_MS:
            category = "FAST_HUMAN"
            self._fast_humans += 1
            logger.debug(
                f"Achat rapide -- {market_hash_name} "
                f"| TTD: {ttd_range_str} ({ttd_source}) | ${price_usd:.2f}{discount_str}"
                f"{sa_flag}{float_str}{sticker_str} | {platform.upper()}"
            )
            self._log_snipe(
                market_hash_name, price_usd, ttd_ms, platform, category,
                ref_price_usd, discount_pct,
                float_value, paint_seed, sticker_count, sticker_names, cheapest_by_sa,
                ttd_min_ms=ttd_min_ms, ttd_max_ms=ttd_max_ms,
            )
            self._update_hot_item(
                market_hash_name, price_usd, ttd_ms, platform, category,
                ttd_min_ms=ttd_min_ms, ttd_max_ms=ttd_max_ms
            )
        else:
            category = "NORMAL_SALE"

        # Enregistrer la transaction dans la base de données SQLite locale pour l'historique
        if not float_value or float_value <= 0:
            logger.warning(f"SKIP_NO_FLOAT: {market_hash_name} fv={float_value!r} platform={platform}")
            return
        self._db.save_transaction(
            market_hash_name=market_hash_name,
            price_usd=price_usd,
            ttd_ms=ttd_ms,
            platform=platform,
            category=category,
            float_value=float_value,
            paint_seed=paint_seed,
            sticker_count=sticker_count,
            sticker_names=sticker_names,
            confidence=confidence,
        )

    def _update_hot_item(
        self,
        market_hash_name: str,
        price_usd: float,
        ttd_ms: float,
        platform: str,
        category: str,
        ttd_min_ms: Optional[float] = None,
        ttd_max_ms: Optional[float] = None,
    ) -> None:
        if ttd_min_ms is None:
            ttd_min_ms = ttd_ms
        if ttd_max_ms is None:
            ttd_max_ms = ttd_ms

        now_iso = datetime.now(timezone.utc).isoformat()

        if market_hash_name not in self._hot_items:
            self._hot_items[market_hash_name] = {
                "market_hash_name": market_hash_name,
                "bot_snipe_count": 0,
                "fast_human_count": 0,
                "total_count": 0,
                "avg_ttd_ms": 0.0,
                "avg_ttd_min_ms": 0.0,
                "avg_ttd_max_ms": 0.0,
                "min_price_usd": price_usd,
                "max_price_usd": price_usd,
                "sum_price_usd": 0.0,
                "first_observed": now_iso,
                "last_observed": now_iso,
                "platforms": [],
            }

        item = self._hot_items[market_hash_name]

        # Compteurs
        if category == "BOT_SNIPE":
            item["bot_snipe_count"] += 1
        else:
            item["fast_human_count"] += 1
        item["total_count"] += 1

        # Moyenne glissante
        n = item["total_count"]

        if "avg_ttd_min_ms" not in item:
            item["avg_ttd_min_ms"] = item["avg_ttd_ms"]
        if "avg_ttd_max_ms" not in item:
            item["avg_ttd_max_ms"] = item["avg_ttd_ms"]

        item["avg_ttd_ms"] = item["avg_ttd_ms"] * (n - 1) / n + ttd_ms / n
        item["avg_ttd_min_ms"] = item["avg_ttd_min_ms"] * (n - 1) / n + ttd_min_ms / n
        item["avg_ttd_max_ms"] = item["avg_ttd_max_ms"] * (n - 1) / n + ttd_max_ms / n

        # Prix
        item["min_price_usd"] = min(item["min_price_usd"], price_usd)
        item["max_price_usd"] = max(item["max_price_usd"], price_usd)
        item["sum_price_usd"] += price_usd
        item["last_observed"] = now_iso

        # Plateformes
        if platform not in item["platforms"]:
            item["platforms"].append(platform)

    # ──────────────────────────────────────────────────────────────────────────
    # AFFICHAGE LIVE
    # ──────────────────────────────────────────────────────────────────────────

    def print_live_stats(self, top_n: int = 10) -> None:
        elapsed_sec = time.time() - self._session_start
        elapsed_str = _format_duration(elapsed_sec)

        sorted_items = sorted(
            self._hot_items.values(),
            key=lambda x: (-x["bot_snipe_count"], x["avg_ttd_ms"]),
        )[:top_n]

        print("\n" + "═" * 74)
        print(f"   🔭 MODE OBSERVATION — TOP {top_n} ITEMS LES PLUS SNIPÉS")
        print(
            f"   Durée : {elapsed_str} | "
            f"Cycles : {self._poll_cycles} | "
            f"Snipes bot : {self._bot_snipes} | "
            f"Achats rapides : {self._fast_humans}"
        )
        print("═" * 74)
        print(f" {'#':>2}  {'Item':<38}  {'Snipes':>6}  {'TTD Moy':>11}  {'Prix Moy':>8}")
        print("─" * 74)

        if not sorted_items:
            print("   (Aucun item qualifié pour le moment — observation en cours...)")
        else:
            for rank, item in enumerate(sorted_items, 1):
                n = item["total_count"]
                avg_price = item["sum_price_usd"] / n if n > 0 else 0.0
                
                avg_ttd_min_s = item.get("avg_ttd_min_ms", item["avg_ttd_ms"]) / 1000
                avg_ttd_max_s = item.get("avg_ttd_max_ms", item["avg_ttd_ms"]) / 1000
                ttd_range_str = f"[{avg_ttd_min_s:.1f}-{avg_ttd_max_s:.1f}]s"
                
                name = item["market_hash_name"]
                if len(name) > 38:
                    name = name[:35] + "..."
                print(
                    f" {rank:>2}  {name:<38}  {item['bot_snipe_count']:>6}  "
                    f"{ttd_range_str:>11}  ${avg_price:>7.2f}"
                )

        print("═" * 74)
        print(
            f"   Actifs : {len(self._active_listings)} | "
            f"En attente : {len(self._pending_verification)} | "
            f"Double-vérif : {len(self._under_verification)} | "
            f"Hot items : {len(self._hot_items)}"
        )
        print("═" * 74 + "\n")

    # ──────────────────────────────────────────────────────────────────────────
    # EXPORT JSON
    # ──────────────────────────────────────────────────────────────────────────

    def export_results(self) -> None:
        """
        Sauvegarde les résultats d'observation dans les fichiers JSON de sortie.
        """
        # Purge de la fuite de mémoire : supprimer les entrées du cooldown expiré
        now = time.time()
        self._callback_cooldown = {
            lid: ts for lid, ts in self._callback_cooldown.items()
            if now - ts < self._CALLBACK_COOLDOWN_SEC
        }

        self._export_hot_items()
        self._export_report()

    def _export_hot_items(self) -> None:
        qualified = [
            item for item in self._hot_items.values()
            if item["bot_snipe_count"] >= config.OBS_MIN_SNIPE_COUNT
        ]

        for item in qualified:
            n = item["total_count"]
            item["avg_price_usd"] = round(item["sum_price_usd"] / n, 2) if n > 0 else 0.0
            item["avg_ttd_ms"] = round(item["avg_ttd_ms"], 0)
            item["avg_ttd_min_ms"] = round(item.get("avg_ttd_min_ms", item["avg_ttd_ms"]), 0)
            item["avg_ttd_max_ms"] = round(item.get("avg_ttd_max_ms", item["avg_ttd_ms"]), 0)
            item["min_price_usd"] = round(item["min_price_usd"], 2)
            item["max_price_usd"] = round(item["max_price_usd"], 2)

        qualified.sort(key=lambda x: -x["bot_snipe_count"])

        try:
            config.OBSERVATION_HOT_ITEMS_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(config.OBSERVATION_HOT_ITEMS_PATH, "w", encoding="utf-8") as f:
                json.dump(qualified, f, ensure_ascii=False, indent=2)
            logger.info(
                f"📁 Hot items exportés : {len(qualified)} items qualifiés "
                f"→ {config.OBSERVATION_HOT_ITEMS_PATH}"
            )
        except Exception as e:
            logger.error(f"Erreur lors de l'export hot items : {e}")

    def _export_report(self) -> None:
        elapsed_sec = time.time() - self._session_start
        report = {
            "session_start": datetime.fromtimestamp(
                self._session_start, tz=timezone.utc
            ).isoformat(),
            "session_duration_minutes": round(elapsed_sec / 60, 1),
            "poll_cycles": self._poll_cycles,
            "total_listings_observed": self._total_listings_seen,
            "total_disappearances": self._total_disappearances,
            "bot_snipes_detected": self._bot_snipes,
            "fast_human_purchases": self._fast_humans,
            "hot_items_qualified": sum(
                1 for item in self._hot_items.values()
                if item["bot_snipe_count"] >= config.OBS_MIN_SNIPE_COUNT
            ),
            "total_unique_items_tracked": len(self._hot_items),
            "obs_bot_snipe_ttd_ms": config.OBS_BOT_SNIPE_TTD_MS,
            "obs_fast_human_ttd_ms": config.OBS_FAST_HUMAN_TTD_MS,
        }

        try:
            with open(config.OBSERVATION_REPORT_PATH, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            logger.info(
                f"📊 Rapport exporté → {config.OBSERVATION_REPORT_PATH}"
            )
        except Exception as e:
            logger.error(f"Erreur lors de l'export du rapport : {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # INTERFACE DE MISE À VOIR DIRECTE PAR ÉVÉNEMENT (WEBSOCKETS)
    # ──────────────────────────────────────────────────────────────────────────

    def record_addition(self, listing: dict, platform: str) -> None:
        listing_id = listing.get("id")
        name = listing.get("market_hash_name")
        price = listing.get("price")
        if not listing_id or not name or price is None:
            return

        now = time.time()
        listed_at = listing.get("listed_at")
        first_seen_ts = listed_at if listed_at and listed_at <= now else now

        self._active_listings[listing_id] = {
            "name": name,
            "price_cents": price,
            "platform": platform,
            "offer_id": listing.get("offer_id", listing_id),
            "first_seen_ts": first_seen_ts,
            "last_seen_ts": now,
            "ttd_from_listing": listed_at is not None and listed_at <= now,
            "float_value": listing.get("float_value"),
            "paint_seed": listing.get("paint_seed"),
            "sticker_count": listing.get("sticker_count", 0),
            "sticker_names": listing.get("sticker_names", []),
        }

        # Prix dans l'historique
        hist = self._price_history.setdefault(name, [])
        hist.append(price)
        if len(hist) > 50:
            hist.pop(0)

        # Déclencher le callback
        if self.new_listing_callback:
            last_fired = self._callback_cooldown.get(listing_id, 0.0)
            if now - last_fired >= self._CALLBACK_COOLDOWN_SEC:
                self._callback_cooldown[listing_id] = now
                try:
                    self.new_listing_callback({
                        "id": listing_id,
                        "offer_id": listing.get("offer_id", listing_id),
                        "name": name,
                        "price_cents": price,
                        "platform": platform,
                        "float_value": listing.get("float_value"),
                        "paint_seed": listing.get("paint_seed"),
                        "sticker_names": listing.get("sticker_names", []),
                    })
                except Exception as e:
                    logger.error(f"Erreur dans le callback de nouveau listing : {e}")

    def record_removal(self, listing_id: str, platform: str, auto_confirm: bool = True) -> Optional[dict]:
        now = time.time()
        entry = self._active_listings.pop(listing_id, None)
        if not entry:
            return None

        first_seen = entry["first_seen_ts"]
        ttd_ms = (now - first_seen) * 1000

        entry["listing_id"] = listing_id
        entry["ttd_ms"] = ttd_ms
        entry["ttd_min_ms"] = ttd_ms
        entry["ttd_max_ms"] = ttd_ms
        entry["absent_first_seen_ts"] = now

        if auto_confirm:
            self._total_disappearances += 1
            self._process_disappearance(
                market_hash_name=entry["name"],
                price_cents=entry["price_cents"],
                ttd_ms=ttd_ms,
                platform=platform,
                ttd_from_listing=entry.get("ttd_from_listing", False),
                float_value=entry.get("float_value"),
                paint_seed=entry.get("paint_seed"),
                sticker_count=entry.get("sticker_count", 0),
                sticker_names=entry.get("sticker_names", []),
                cheapest_by_sa=False,
                ttd_min_ms=ttd_ms,
                ttd_max_ms=ttd_ms,
                confidence="LOW",
            )
            return None
        else:
            self._under_verification[listing_id] = entry
            return entry


# ──────────────────────────────────────────────────────────────────────────────
# UTILITAIRES DE FORMATE
# ──────────────────────────────────────────────────────────────────────────────

def _format_duration(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours}h {minutes}min"
    elif minutes > 0:
        return f"{minutes}min {secs}s"
    else:
        return f"{secs}s"
