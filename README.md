# 🚀 Crypto Data Pipeline & Analytics Platform

## 📌 Contexte du projet

Le marché des cryptomonnaies génère quotidiennement des volumes massifs de données : prix, volumes d'échange, capitalisations boursières et indicateurs de performance. Ces données représentent une source précieuse d'information pour les investisseurs et les analystes, mais nécessitent une architecture robuste pour être exploitées efficacement.

Ce projet consiste à concevoir et implémenter une plateforme Data Engineering & Analytics complète permettant de collecter, transformer, stocker et visualiser des données de cryptomonnaies issues de l'API CoinGecko.

L'architecture suit une approche **Medallion Architecture (Bronze, Silver, Gold)** en utilisant MinIO comme Data Lake, Snowflake comme Data Warehouse et Tableau comme outil de Business Intelligence.

---

# 🎯 Objectifs

* Collecter automatiquement les données depuis l'API CoinGecko.
* Mettre en place une architecture Medallion.
* Construire un modèle dimensionnel analytique.
* Automatiser les traitements via Apache Airflow.
* Charger les données dans Snowflake.
* Créer des tableaux de bord interactifs dans Tableau.

---

# 🏗️ Architecture du projet

## Flux global

```text
CoinGecko API
      ↓
Bronze Layer (JSON)
      ↓
Silver Layer (Parquet)
      ↓
Gold Layer (Modèle Dimensionnel)
      ↓
Snowflake Data Warehouse
      ↓
Tableau Dashboard
```

---

## Architecture Medallion

### Bronze Layer (crypto-bronze)

Objectif : conserver les données brutes.

Format :

```text
JSON
```

Emplacement :

```text
crypto-bronze/YYYY/MM/DD/raw.json
```

Contenu :

* Réponse brute CoinGecko
* Horodatage de collecte
* Aucune transformation

---

### Silver Layer (crypto-silver)

Objectif : nettoyage et normalisation.

Format :

```text
Parquet
```

Transformations :

* Nettoyage des données
* Gestion des valeurs nulles
* Conversion des types
* Normalisation snake_case

Exemple :

```text
current_price
market_cap
total_volume
```

---

### Gold Layer (crypto-gold)

Objectif : modèle dimensionnel analytique.

Format :

```text
Parquet
```

Tables produites :

```text
DIM_CRYPTO
DIM_DATE
FACT_CRYPTO_SNAPSHOT
```

---

# 📊 Modèle Dimensionnel

## Grain de la table de faits

Une ligne représente :

```text
Une cryptomonnaie à une date et heure de collecte donnée.
```

---

## Table DIM_CRYPTO

| Colonne    | Type    |
| ---------- | ------- |
| CRYPTO_KEY | INTEGER |
| ID         | STRING  |
| SYMBOL     | STRING  |
| NAME       | STRING  |

---

## Table DIM_DATE

| Colonne  | Type    |
| -------- | ------- |
| DATE_KEY | INTEGER |
| DATE     | DATE    |
| YEAR     | INTEGER |
| MONTH    | INTEGER |
| DAY      | INTEGER |
| HOUR     | INTEGER |
| MINUTE   | INTEGER |
| WEEK     | INTEGER |
| QUARTER  | INTEGER |

---

## Table FACT_CRYPTO_SNAPSHOT

| Colonne                     | Type    |
| --------------------------- | ------- |
| CRYPTO_KEY                  | INTEGER |
| DATE_KEY                    | INTEGER |
| CURRENT_PRICE               | FLOAT   |
| MARKET_CAP                  | FLOAT   |
| MARKET_CAP_RANK             | INTEGER |
| TOTAL_VOLUME                | FLOAT   |
| HIGH_24H                    | FLOAT   |
| LOW_24H                     | FLOAT   |
| PRICE_CHANGE_24H            | FLOAT   |
| PRICE_CHANGE_PERCENTAGE_24H | FLOAT   |
| CIRCULATING_SUPPLY          | FLOAT   |
| MAX_SUPPLY                  | FLOAT   |
| ATH                         | FLOAT   |

---

# 🛠️ Technologies utilisées

## Data Ingestion

* Python
* Requests
* CoinGecko API

## Data Lake

* MinIO

## Transformation

* Pandas
* PyArrow

## Orchestration

* Apache Airflow

## Data Warehouse

* Snowflake

## Visualisation

* Tableau Desktop

---

# 📂 Structure du projet

```text
project/
│
├── dags/
│   └── crypto_pipeline_dag.py
│
├── scripts/
│   ├── ingest_bronze.py
│   ├── transform_silver.py
│   ├── build_gold_model.py
│   └── load_snowflake.py
│
├── data/
│
├── docs/
│
└── README.md
```

---

# ⚙️ Pipeline ETL

## Étape 1 — Ingestion Bronze

* Connexion API CoinGecko
* Collecte des données
* Stockage JSON brut dans MinIO

Sortie :

```text
crypto-bronze/YYYY/MM/DD/raw.json
```

---

## Étape 2 — Transformation Silver

* Lecture JSON
* Nettoyage avec Pandas
* Normalisation des colonnes
* Sauvegarde Parquet

Sortie :

```text
crypto-silver/crypto_clean.parquet
```

---

## Étape 3 — Construction Gold

Création :

* DIM_CRYPTO
* DIM_DATE
* FACT_CRYPTO_SNAPSHOT

Vérification des clés étrangères.

Sortie :

```text
crypto-gold/
```

---

## Étape 4 — Chargement Snowflake

Création des tables Snowflake.

Chargement :

```text
DIM_CRYPTO
DIM_DATE
FACT_CRYPTO_SNAPSHOT
```

Validation du chargement.

---

## Étape 5 — Orchestration Airflow

DAG :

```text
crypto_pipeline_dag
```

Workflow :

```text
ingest_bronze
        ↓
transform_silver
        ↓
build_gold_model
        ↓
load_snowflake
```

Planification :

```text
@daily
```

---

# 📈 Visualisations Tableau

## KPI Cards

* Prix actuel
* Market Cap
* Volume Total
* Variation 24h

---

## Graphique en ligne

Évolution du prix par cryptomonnaie.

---

## Graphique à barres

Top 10 cryptomonnaies par volume moyen.

---

## Heatmap

Variation journalière :

* Vert : hausse
* Rouge : baisse

---

## Scatter Plot

Analyse :

```text
Volume vs Variation du prix
```

---

## Dashboard Détail

Informations d'une cryptomonnaie :

* Prix actuel
* Market Cap
* Volume
* High 24h
* Low 24h
* ATH
* Circulating Supply

---

# 📊 Dashboards

## Dashboard Principal

Vue comparative multi-cryptos :

* KPI
* Heatmap
* Scatter Plot
* Top 10 Volumes

Filtres :

* Crypto
* Date

---

## Dashboard Détail

Zoom sur une cryptomonnaie :

* Toutes les métriques disponibles
* Analyse détaillée
* Navigation depuis le dashboard principal

---

# ✅ Résultats

Le projet met en œuvre une chaîne de traitement Data Engineering complète :

* Collecte automatisée
* Architecture Medallion
* Modélisation dimensionnelle
* Orchestration Airflow
* Stockage Snowflake
* Analyse Tableau

Cette solution permet une analyse efficace et centralisée des performances du marché des cryptomonnaies.


