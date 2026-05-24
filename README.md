# 🔭 CS2 Observation Bot — Guide de Déploiement (Oracle Cloud Free Tier)

Ce dossier contient le code autonome du bot d'observation de vitesse de vente (TTD) pour CS2, isolé du reste du projet de sniping. Il est optimisé pour tourner en continu en tant que service d'arrière-plan (démon) sur un serveur Linux.

---

## 🏗️ Structure du Dossier de Déploiement

* **`observer_main.py`** : Point d'entrée pour démarrer l'observation.
* **`observer.py`** : Logique de tracking et d'enregistrement des transactions.
* **`observation_ingestion.py`** : Coordinateur d'ingestion (polling et websockets).
* **`transaction_db.py`** : Gestion de la base SQLite locale.
* **`dmarket_ingestion.py`**, **`waxpeer_ingestion.py`**, **`market_csgo_ingestion.py`** : Ingesteurs dédiés par plateforme.
* **`utils.py`** : Structures de données d'optimisation (caches d'exclusion).
* **`config.py`** : Paramètres du bot (seuils, chemins).
* **`requirements.txt`** : Liste des packages requis.
* **`.env.example`** : Modèle de configuration des variables d'environnement.
* **`systemd.service.example`** : Fichier de configuration pour le démon Linux.

---

## 🚀 Déploiement Pas-à-Pas sur Oracle Cloud (Ubuntu VM)

### 1. Prérequis sur l'Instance
Connectez-vous à votre instance Oracle Cloud Ubuntu via SSH :
```bash
ssh -i /path/to/key.key ubuntu@<IP_PUBLIQUE_ORACLE>
```

Mettez à jour le système et installez Python, pip et l'environnement virtuel :
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip python3-venv python3-dev build-essential -y
```

### 2. Cloner / Copier le Code sur le Serveur
Créez le répertoire cible et copiez-y les fichiers de ce dossier `observation_bot_deploy` (par exemple avec `scp`, `rsync` ou en créant un git temporaire) :
```bash
mkdir -p /home/ubuntu/cs2-observation-bot
```

### 3. Configurer l'Environnement Virtuel & Installer les Dépendances
Dans le dossier du projet sur le serveur :
```bash
cd /home/ubuntu/cs2-observation-bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Configurer les Variables d'Environnement
Copiez le fichier d'exemple et renseignez vos clés d'API :
```bash
cp .env.example .env
nano .env
```
Renseignez au minimum les clés API pour les plateformes que vous souhaitez écouter (`DMARKET_PUBLIC_KEY`/`SECRET_KEY`, `WAXPEER_API_KEY`, `MARKET_CSGO_API_KEY`). Sauvegardez et quittez (`Ctrl+O`, `Entrée`, `Ctrl+X`).

---

## ⚙️ Exécution en tant que Démon (Systemd Service)

Pour s'assurer que le bot tourne 24h/24, se lance au démarrage du serveur et redémarre automatiquement s'il plante :

### 1. Installer le service systemd
Copiez la configuration du service :
```bash
sudo cp systemd.service.example /etc/systemd/system/cs2-observer.service
```

### 2. Démarrer et Activer le Service
Rechargez systemd, démarrez le bot et activez-le pour le lancement automatique au reboot :
```bash
sudo systemctl daemon-reload
sudo systemctl start cs2-observer
sudo systemctl enable cs2-observer
```

### 3. Contrôler le Fonctionnement
* **Vérifier le statut** :
  ```bash
  sudo systemctl status cs2-observer
  ```
* **Suivre les logs en temps réel** (très utile) :
  ```bash
  sudo journalctl -u cs2-observer -f -n 50
  ```
* **Arrêter le bot** :
  ```bash
  sudo systemctl stop cs2-observer
  ```
* **Redémarrer le bot** :
  ```bash
  sudo systemctl restart cs2-observer
  ```

---

## 📊 Récupération des Données (Dataset)

* **Base de données SQLite** : Située par défaut dans `/home/ubuntu/cs2-observation-bot/data/observer_dataset.db`. Elle contient les tables `transactions` (historique des ventes validées) et `observed_listings`.
* **Rapports et Logs d'export** : Situés dans `/home/ubuntu/cs2-observation-bot/data/observed_hot_items.json` et `observation_report.json`.

Pour extraire le dataset de votre serveur vers votre machine locale :
```bash
scp -i /path/to/key.key ubuntu@<IP_PUBLIQUE_ORACLE>:/home/ubuntu/cs2-observation-bot/data/observer_dataset.db ./observer_dataset.db
```
