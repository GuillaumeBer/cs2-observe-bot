# Skinport API — Documentation d'Intégration

Résultat de l'investigation menée le 2026-05-25.

---

## Résumé Exécutif

| Approche | Auth requise | Fonctionne depuis Oracle Cloud | Données |
|---|---|---|---|
| REST `/v1/items` | Non | Oui | Listings actuels agrégés (cache 5 min) |
| REST `/v1/sales/history` | Non | Oui | Stats historiques agrégées (cache 5 min) |
| WebSocket saleFeed | **Oui** (session login) | Non (IP datacenter bloquée) | Événements temps réel individuels |

---

## 1. API REST Publique (sans authentification)

### Base URL

```
https://api.skinport.com/v1/
```

### Compression obligatoire

Toutes les réponses utilisent la compression **Brotli**. L'en-tête `Accept-Encoding: br` est requis, sinon la réponse est illisible.

```python
import urllib.request, json, brotli

def skinport_get(path: str) -> list | dict:
    req = urllib.request.Request(
        f"https://api.skinport.com/v1/{path}",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "br",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(brotli.decompress(resp.read()))
```

### Rate limits

**8 requêtes par 5 minutes** par groupe d'endpoints → maximum 1 requête toutes les 37 secondes.

---

### Endpoint 1 : `/v1/items` — Listings actuels

**URL** : `GET https://api.skinport.com/v1/items?app_id=730&currency=EUR&tradable=0`

**Paramètres** :
| Paramètre | Valeur | Description |
|---|---|---|
| `app_id` | `730` | CS2 |
| `currency` | `EUR` / `USD` | Devise de retour |
| `tradable` | `0` (défaut) / `1` | `1` = seulement les items échangeables |

**Cache serveur** : 5 minutes.

**Structure de réponse** : liste de ~20 000 objets, un par `market_hash_name`.

```json
{
  "market_hash_name": "AWP | Dragon Lore (Field-Tested)",
  "version": null,
  "currency": "EUR",
  "suggested_price": 1250.00,
  "item_page": "https://skinport.com/item/awp-dragon-lore-field-tested",
  "market_page": "https://skinport.com/market?item=...",
  "min_price": 1180.00,
  "max_price": 1450.00,
  "mean_price": 1280.00,
  "median_price": 1260.00,
  "quantity": 3,
  "created_at": 1661324437,
  "updated_at": 1779736513
}
```

**Champs clés** :
- `suggested_price` : prix de référence Skinport (prix "juste marché")
- `min_price` : prix du listing le moins cher actuellement en vente
- `quantity` : nombre de copies actuellement listées
- `updated_at` : timestamp Unix de la dernière mise à jour de cet item

**Utilisation pour détecter des opportunités** :

```python
# Détecter les items listés en-dessous du prix suggéré (opportunité snipe)
discount_threshold = 0.85  # -15%
opportunities = [
    item for item in items
    if item["min_price"] and item["suggested_price"]
    and item["min_price"] < item["suggested_price"] * discount_threshold
    and item["quantity"] > 0
]
```

---

### Endpoint 2 : `/v1/sales/history` — Historique des ventes

**URL** : `GET https://api.skinport.com/v1/sales/history?app_id=730&currency=EUR`

**Cache serveur** : 5 minutes.

**Structure de réponse** : liste de ~33 000 objets, un par `market_hash_name`.

```json
{
  "market_hash_name": "AK-47 | Redline (Field-Tested)",
  "currency": "EUR",
  "last_24_hours": {
    "min": 12.50,
    "max": 15.00,
    "avg": 13.20,
    "median": 13.00,
    "volume": 8
  },
  "last_7_days": {
    "min": 11.80,
    "max": 16.50,
    "avg": 13.10,
    "median": 13.00,
    "volume": 47
  },
  "last_30_days": { "min": ..., "max": ..., "avg": ..., "median": ..., "volume": 180 },
  "last_90_days": { "min": ..., "max": ..., "avg": ..., "median": ..., "volume": 520 }
}
```

**Champs clés** :
- `volume` : nombre de ventes sur la période (indicateur de liquidité)
- `avg` / `median` : prix moyen/médian de vente
- `min` : prix de vente le plus bas enregistré sur la période

**Utilisation pour estimer la liquidité** :

```python
# Combiner items + historique pour identifier les items liquides sous-évalués
history_map = {h["market_hash_name"]: h for h in history}

for item in items:
    name = item["market_hash_name"]
    hist = history_map.get(name)
    if not hist:
        continue
    daily_volume = hist["last_7_days"]["volume"] / 7
    # Item liquide (>1 vente/jour) et sous-évalué (-10%)
    if (daily_volume >= 1.0
            and item["min_price"]
            and item["suggested_price"]
            and item["min_price"] < item["suggested_price"] * 0.90):
        print(f"OPPORTUNITÉ: {name} @ {item['min_price']}€ "
              f"(suggéré {item['suggested_price']}€, {daily_volume:.1f} ventes/j)")
```

---

## 2. WebSocket saleFeed (authentification requise)

### Pourquoi le WebSocket nécessite une auth

Le endpoint `/socket.io/?EIO=4&transport=polling` retourne **HTTP 403** même depuis une IP résidentielle sans session authentifiée. Skinport a configuré Cloudflare pour bloquer tout accès non-authentifié au Socket.IO.

En plus, les IPs de datacenter (Oracle Cloud, AWS, GCP) sont bloquées par Cloudflare Bot Management avec l'erreur **1005** — elles ne peuvent pas accéder au WebSocket même avec une session valide.

### Stratégie de connexion (IP résidentielle uniquement)

L'approche fonctionnelle utilise **patchright** (fork de Playwright qui bypasse Cloudflare Turnstile) :

1. Ouvrir un navigateur Chromium réel
2. Se connecter au compte Skinport (email + mot de passe)
3. Cloudflare Turnstile se résout **automatiquement** par patchright — ne pas cliquer sur l'iframe manuellement
4. Naviguer vers `/market` → la page ouvre automatiquement le Socket.IO
5. Intercepter les frames WebSocket via `page.on("websocket")`

```python
# NE PAS faire (interfère avec le bypass automatique) :
# await turnstile_iframe_locator.click()

# FAIRE (laisser patchright gérer) :
await asyncio.sleep(5)  # délai pour le bypass automatique
await submit_button.click()
```

### Structure des événements saleFeed

Chaque frame reçue est une chaîne Socket.IO commençant par `42` :

```
42["saleFeed", {"eventType": "listed", "sales": [...]}]
42["saleFeed", {"eventType": "sold", "sales": [...]}]
```

Structure d'un item dans `sales` :

```json
{
  "sale_id": 12345678,
  "market_hash_name": "AK-47 | Redline (Field-Tested)",
  "sale_price": 1320,
  "wear": 0.2341,
  "pattern": 517,
  "asset_id": 987654321,
  "stickers": [
    {"name": "Virtus.pro | Katowice 2015", "wear": 0.0, "slot": 0}
  ]
}
```

**Note** : `sale_price` est en **centimes** (1320 = 13,20 €).

### Calcul du TTD via WebSocket

```python
from datetime import datetime, timezone

listed_times = {}  # sale_id -> datetime

def on_listed(item):
    listed_times[item["sale_id"]] = datetime.now(timezone.utc)

def on_sold(item):
    sid = item["sale_id"]
    if sid in listed_times:
        ttd_seconds = (datetime.now(timezone.utc) - listed_times[sid]).total_seconds()
        print(f"{item['market_hash_name']}: TTD = {ttd_seconds:.0f}s")
        del listed_times[sid]
```

---

## 3. Calcul approximatif du TTD via REST (sans auth)

Sans le WebSocket, le TTD peut être estimé par **comparaison de snapshots successifs** :

```python
import time

prev_snapshot = {}  # market_hash_name -> {"quantity": int, "min_price": float, "seen_at": float}

def poll_and_detect(items: list):
    now = time.time()
    for item in items:
        name = item["market_hash_name"]
        qty = item["quantity"] or 0
        prev = prev_snapshot.get(name)

        if prev and prev["quantity"] > qty and qty == 0:
            # L'item a disparu → vendu
            ttd_approx = now - prev["seen_at"]
            print(f"VENDU (approx): {name} TTD ≤ {ttd_approx:.0f}s")

        prev_snapshot[name] = {"quantity": qty, "min_price": item["min_price"], "seen_at": now}
```

**Limites** :
- Résolution TTD : ~60 secondes (intervalle de polling)
- Impossible de distinguer "vendu" de "annulé"
- Pas d'IDs individuels → si plusieurs copies, impossible de savoir laquelle a été vendue
- Cache serveur 5 min → les items listés et vendus dans la même fenêtre de 5 min sont invisibles

---

## 4. Comparaison des approches

| Critère | REST polling | WebSocket (Playwright) |
|---|---|---|
| Auth requise | Non | Oui (email + password) |
| Fonctionne depuis Oracle Cloud | **Oui** | Non (IP bloquée) |
| Latence de détection | ~60s | <1s |
| TTD exact | Non (±60s) | Oui |
| IDs de vente individuels | Non | Oui |
| Infos float / stickers | Non | Oui |
| Complexité d'implémentation | Faible | Élevée (Playwright + Turnstile) |
| Stabilité | Haute | Moyenne (dépend du login) |

### Recommandation

- **Observation bot sur Oracle Cloud** → REST polling (`/v1/items` + `/v1/sales/history`)
  - Suffisant pour : tendances de liquidité, prix médians, items populaires
  - Limitation : pas de TTD exact, pas de signal temps réel

- **Sniper bot local (IP résidentielle)** → WebSocket via Playwright/patchright
  - Nécessaire pour : alertes en temps réel, TTD exact, détection d'items sous-évalués

---

## 5. Variables d'environnement

Aucun credential nécessaire pour l'API REST publique.

Pour le WebSocket (mode Playwright) :

```env
SKINPORT_ENABLED=true
SKINPORT_USE_PLAYWRIGHT=true
SKINPORT_PLAYWRIGHT_HEADLESS=true
SKINPORT_EMAIL=votre@email.com
SKINPORT_PASSWORD=votre_mot_de_passe
```

**Important** : ne jamais committer le fichier `.env` dans git.
