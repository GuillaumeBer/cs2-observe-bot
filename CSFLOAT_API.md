# CSFloat API — Documentation d'Intégration

Investigation menée le 2026-05-25.

---

## Résumé Exécutif

| Critère | Valeur |
|---|---|
| Auth requise | **Oui** — clé API gratuite |
| Obtenir une clé | https://csfloat.com/profile → onglet "Developer" |
| Fonctionne depuis Oracle Cloud | **Oui** (pas de blocage IP) |
| Temps réel (WebSocket/push) | Non — polling uniquement |
| Données par listing | float, paint_seed, stickers, prix SCM, `created_at` |
| TTD calculable | **Oui** (via `created_at` + tracking de disparition) |
| Rate limit | ~200 req/h par clé |

---

## 1. Authentification

La clé API doit être incluse dans **toutes** les requêtes via l'en-tête HTTP :

```
Authorization: <API-KEY>
```

Pour obtenir une clé : connectez-vous sur https://csfloat.com → Profil → onglet **Developer**.

---

## 2. Endpoint principal : `/api/v1/listings`

**URL** : `GET https://csfloat.com/api/v1/listings`

**Paramètres de filtrage** :

| Paramètre | Type | Description |
|---|---|---|
| `limit` | int | Nombre de résultats (max 50, défaut 50) |
| `sort_by` | string | Tri : `most_recent`, `lowest_price`, `highest_price`, `best_deal`, `highest_discount`, `lowest_float`, `highest_float`, `float_rank` |
| `market_hash_name` | string | Filtrer par nom d'item (ex: `AK-47 \| Redline (Field-Tested)`) |
| `min_price` / `max_price` | int | Fourchette de prix en **centimes** |
| `min_float` / `max_float` | float | Fourchette de float value (0.0–1.0) |
| `paint_seed` | int | Filtrer par pattern ID |
| `paint_index` | int | Filtrer par paint index |
| `type` | string | `buy_now` ou `auction` |
| `category` | int | `0`=tous, `1`=normal, `2`=stattrak, `3`=souvenir |
| `stickers` | string | Format : `ID\|SLOT[,ID\|SLOT...]` |
| `cursor` | string | Curseur opaque pour la pagination |

**Structure d'une réponse** :

```json
[
  {
    "id": "324288155723370196",
    "created_at": "2021-06-13T20:45:21.311794Z",
    "type": "buy_now",
    "price": 26000,
    "state": "listed",
    "seller": {
      "steam_id": "76561198084749846",
      "username": "Step7750",
      "statistics": {
        "median_trade_time": 236,
        "total_trades": 24,
        "total_verified_trades": 24
      }
    },
    "item": {
      "asset_id": "22547095285",
      "market_hash_name": "M4A4 | Poseidon (Factory New)",
      "float_value": 0.027965,
      "paint_seed": 700,
      "paint_index": 449,
      "is_stattrak": false,
      "is_souvenir": false,
      "rarity": 5,
      "stickers": [
        {
          "stickerId": 1060,
          "slot": 3,
          "name": "Sticker | Team EnVyUs (Holo) | MLG Columbus 2016",
          "wear": 0.0,
          "scm": { "price": 736, "volume": 1 }
        }
      ],
      "scm": { "price": 175076, "volume": 0 },
      "inspect_link": "steam://rungame/730/..."
    },
    "is_seller": false,
    "min_offer_price": 22100,
    "watchers": 0
  }
]
```

**Champs clés** :
- `price` — prix en **centimes** (26000 = 260,00 €)
- `created_at` — timestamp ISO 8601 (base du calcul TTD)
- `item.float_value` — float value exact
- `item.paint_seed` — numéro de pattern
- `item.scm.price` — prix Steam Community Market en centimes (référence)
- `item.stickers[].scm.price` — valeur de chaque sticker sur le SCM

---

## 3. Calcul du TTD

Le TTD (Time To Deal) peut être calculé par **tracking de disparition des listings** :

1. Polling avec `sort_by=most_recent&limit=50` toutes les 60 secondes
2. Tout nouveau `id` non vu précédemment → `listed_at = created_at`
3. Tout `id` absent du snapshot suivant → vendu, `sold_at ≈ now`
4. `TTD = sold_at - listed_at`

**Précision** : ±60s (résolution = intervalle de polling).

```python
from datetime import datetime, timezone

tracked = {}  # listing_id -> {"created_at": datetime, "market_hash_name": str, "price": int}

def process_snapshot(listings: list):
    current_ids = set()
    for listing in listings:
        lid = listing["id"]
        current_ids.add(lid)
        if lid not in tracked:
            # Nouveau listing
            tracked[lid] = {
                "created_at": datetime.fromisoformat(listing["created_at"].replace("Z", "+00:00")),
                "market_hash_name": listing["item"]["market_hash_name"],
                "price": listing["price"],
            }

    # Listings disparus = vendus (approximation)
    sold_ids = set(tracked.keys()) - current_ids
    now = datetime.now(timezone.utc)
    for lid in sold_ids:
        entry = tracked.pop(lid)
        ttd_sec = (now - entry["created_at"]).total_seconds()
        yield {
            "market_hash_name": entry["market_hash_name"],
            "price": entry["price"],
            "ttd_seconds": ttd_sec,
        }
```

---

## 4. Détecter des opportunités (price vs SCM)

CSFloat fournit `item.scm.price` (prix Steam Market) dans chaque listing, ce qui permet de détecter directement les items sous-évalués :

```python
def find_opportunities(listings: list, discount_threshold: float = 0.85) -> list:
    results = []
    for listing in listings:
        price = listing["price"]
        scm_price = listing["item"].get("scm", {}).get("price", 0)
        if scm_price > 0 and price < scm_price * discount_threshold:
            discount_pct = (1 - price / scm_price) * 100
            results.append({
                "market_hash_name": listing["item"]["market_hash_name"],
                "price_eur": price / 100,
                "scm_price_eur": scm_price / 100,
                "discount_pct": discount_pct,
                "float_value": listing["item"].get("float_value"),
                "paint_seed": listing["item"].get("paint_seed"),
                "listing_id": listing["id"],
                "created_at": listing["created_at"],
            })
    return sorted(results, key=lambda x: x["discount_pct"], reverse=True)
```

---

## 5. Implémentation complète — `csfloat_ingestion.py`

Voir le fichier `observation_bot_deploy/csfloat_ingestion.py`.

Fonctionnement :
- Polling `most_recent` toutes les 60 secondes
- Détecte les nouveaux listings → callback `on_listed`
- Détecte les listings disparus (vendus) → callback `on_sold` avec TTD approximatif
- Normalise les données au même format que DMarket/Skinport

---

## 6. Variables d'environnement

```env
CSFLOAT_API_KEY=votre_cle_api_ici
```

Obtenir la clé : https://csfloat.com/profile → onglet **Developer** (gratuit).

---

## 7. Limites

- **Pas de WebSocket** : polling uniquement → latence 30–60s pour détecter un nouveau listing
- **Rate limit** : ~200 req/h → un poll toutes les 18s maximum (recommandé : 60s)
- **TTD approximatif** : ±60s selon l'intervalle de polling
- **Listings disparus ≠ forcément vendus** : peut être une annulation (mais rare)
- **Pagination** : max 50 items par requête, utiliser `cursor` pour aller plus loin
