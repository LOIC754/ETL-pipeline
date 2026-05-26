# 📈 CAC40 Real-Time Data Pipeline

> Pipeline de monitoring temps réel des actions CAC40 (LVMH, BNP Paribas, TotalEnergies, Sanofi, Airbus) sur **Modern Data Stack** — simulant un cas d'usage de banque d'investissement française.

---

## 🏗️ Stack technique

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694A?style=for-the-badge&logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

---

## 🎯 Contexte business

Une banque de financement et d'investissement (BFI) française — typiquement **BNP Paribas CIB**, **Société Générale CIB** ou **Crédit Agricole CIB** — doit surveiller en continu les cours des actions du CAC40 pour alimenter ses desks de trading, ses outils de gestion des risques de marché et ses reportings réglementaires (MiFID II, EMIR).

Les équipes Quant et Risk ont besoin :
- D'un flux de prix **quasi temps réel** pour calculer la VaR (Value at Risk) intraday
- D'un historique **propre et versionné** pour les backtests
- D'un tableau de bord **business-ready** pour les traders et risk managers

Ce projet industrialise cette chaîne de bout en bout sur une stack production-grade.

---

## 🏛️ Architecture

```
┌──────────────┐    ┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   yfinance   │───▶│    Kafka    │───▶│    MinIO     │───▶│   Airflow   │
│  (CAC40 API) │    │  (streaming)│    │  (Bronze S3) │    │ (DAG /1min) │
└──────────────┘    └─────────────┘    └──────────────┘    └─────────────┘
                                                                  │
                                                                  ▼
┌──────────────┐    ┌─────────────────────────────────────────────────┐
│   Power BI   │◀───│              Snowflake Data Warehouse           │
│ (DirectQuery)│    │                                                 │
└──────────────┘    │   Bronze (raw)  →  Silver (clean) →  Gold (KPI) │
                    │              ↑ dbt transformations              │
                    └─────────────────────────────────────────────────┘
```

### Architecture Medallion

| Couche | Description | Format |
|:---:|:---|:---:|
| 🥉 **Bronze** | JSON brut depuis Kafka, stocké en `VARIANT` Snowflake | Raw |
| 🥈 **Silver** | Données typées, nettoyées, enrichies (variations, dates) | View |
| 🥇 **Gold** | KPIs business-ready : `latest_quotes`, `volatility`, `candlestick OHLC` | Table |

---

## 📊 Symboles surveillés

| Ticker | Entreprise | Secteur |
|:---|:---|:---|
| `MC.PA` | LVMH | Luxe |
| `BNP.PA` | BNP Paribas | Banque |
| `TTE.PA` | TotalEnergies | Énergie |
| `SAN.PA` | Sanofi | Pharmaceutique |
| `AIR.PA` | Airbus | Aéronautique |

---

## 🚀 Démarrage rapide

### Prérequis
- Docker Desktop ≥ 4.0
- Python 3.11
- Compte Snowflake (free trial)
- Power BI Desktop

### 1. Cloner et installer

```bash
git clone https://github.com/<user>/cac40-pipeline.git
cd cac40-pipeline

python -m venv venv
source venv/Scripts/activate   # Windows Git Bash
pip install -r requirements.txt
```

### 2. Lancer l'infrastructure Docker

```bash
cd infra
docker compose up airflow-init   # init Airflow (one-shot)
docker compose up -d              # lancer tous les services
```

Services exposés :
| Service | URL | Login |
|:---|:---|:---|
| Kafdrop (Kafka UI) | http://localhost:9001 | — |
| MinIO Console | http://localhost:9002 | `admin / password123` |
| Airflow | http://localhost:8080 | `loic / password123` |

### 3. Créer le topic Kafka + bucket MinIO

```
Topic Kafka  : cac40_quotes        (3 partitions, RF=1)
Bucket MinIO : bronze-transactions
```

### 4. Lancer le producer (yfinance → Kafka)

```bash
cd producer
python producer.py
```

### 5. Lancer le consumer (Kafka → MinIO)

```bash
cd consumer
python consumer.py
```

### 6. Activer le DAG Airflow

DAG `minio_to_snowflake` programmé toutes les minutes → charge les JSON MinIO dans la table Bronze Snowflake.

### 7. Exécuter dbt

```bash
cd dbt_cac40/cac40_pipeline
dbt run
```

Résultat attendu :
```
✅ bronze_cac40_quotes   (view)
✅ silver_cac40_quotes   (view)
✅ gold_kpi_latest       (table)
✅ gold_volatility       (table)
✅ gold_candlestick      (table)
```

### 8. Connecter Power BI

Mode **DirectQuery** sur Snowflake → tables `GOLD_*` → dashboard avec KPI cards, treemap volatilité, chandelier OHLC.

---

## 📁 Structure du projet

```
CAC40_Pipeline/
├── infra/
│   └── docker-compose.yml          # 7 services (Kafka, Zookeeper, Kafdrop, MinIO, Postgres, Airflow x2)
├── producer/
│   └── producer.py                 # yfinance → Kafka (curl_cffi anti-rate-limit)
├── consumer/
│   └── consumer.py                 # Kafka → MinIO (boto3 S3-compatible)
├── dags/
│   └── minio_to_snowflake.py       # DAG Airflow : MinIO → Snowflake Bronze
├── dbt_cac40/
│   └── cac40_pipeline/
│       ├── dbt_project.yml
│       └── models/
│           ├── bronze/
│           │   ├── sources.yml
│           │   └── bronze_cac40_quotes.sql
│           ├── silver/
│           │   └── silver_cac40_quotes.sql
│           └── gold/
│               ├── gold_kpi_latest.sql
│               ├── gold_volatility.sql
│               └── gold_candlestick.sql
└── requirements.txt
```

---

## 🛠️ Composants techniques

### Ingestion — `producer.py`
- API `yfinance` avec session `curl_cffi` impersonating Chrome (contourne le rate-limiting Yahoo)
- Polling toutes les 30 secondes
- Sérialisation JSON → bytes UTF-8 pour Kafka
- 5 symboles CAC40 en parallèle

### Streaming — Apache Kafka
- Topic `cac40_quotes` partitionné (3 partitions)
- Zookeeper pour coordination
- Kafdrop pour monitoring visuel
- Replication factor 1 (projet local)

### Stockage Bronze — MinIO
- S3-compatible (compatibilité `boto3`)
- Clé objet : `{symbole}/{timestamp}.json`
- Volume persistant Docker

### Orchestration — Apache Airflow
- DAG `minio_to_snowflake` : schedule `*/1 * * * *`
- 2 tâches Python séquentielles :
  - `download_from_minio` : récupère les JSON
  - `load_to_snowflake` : `INSERT INTO ... PARSE_JSON()` dans la table `VARIANT`
- Retry policy : 1 retry, délai 5 minutes

### Transformation — dbt + Snowflake
- **Bronze** : projection des champs depuis `VARIANT`, casting des types
- **Silver** : arrondi 2 décimales, calcul `variation_absolue` / `variation_pct`, extraction `trade_date`
- **Gold** :
  - `gold_kpi_latest` — derniers prix par symbole (`ROW_NUMBER OVER PARTITION BY`)
  - `gold_volatility` — `STDDEV` + volatilité relative pour gradient de couleur
  - `gold_candlestick` — OHLC par jour via window functions

### Restitution — Power BI
- Connexion **DirectQuery** (pas d'import → données toujours fraîches)
- 4 visuels : cartes KPI, histogramme variation %, treemap volatilité, slicer symboles

---

## 📈 Dashboard Power BI

User stories couvertes :

> 🧑‍💼 *En tant que trader*, je veux voir d'un coup d'œil le prix actuel de chaque valeur surveillée.

> ⚠️ *En tant que risk manager*, je veux identifier les positions à risque par variation % et volatilité.

> 📅 *En tant qu'analyste*, je veux visualiser l'évolution OHLC en chandelier pour les backtests.

---

## 🎓 Compétences démontrées

- **Architecture** Modern Data Stack (Lakehouse + Medallion)
- **Streaming** event-driven avec Kafka
- **Orchestration** production-grade avec Airflow + DAGs
- **Modélisation** dbt en couches Bronze/Silver/Gold
- **Data Warehousing** Snowflake (VARIANT, window functions)
- **DataViz** Power BI DirectQuery
- **DevOps** Docker Compose multi-services, networking interne
- **Gouvernance** documentation, traçabilité des transformations, tests dbt

---

## 🏦 Secteurs cibles

Cette architecture est directement transposable en :
- **Banque d'investissement** (BFI) — monitoring portefeuilles, VaR intraday
- **Banque de détail** — détection de fraude carte temps réel
- **Assurance** — pricing dynamique, sinistralité
- **Retail / e-commerce** — analytics multi-canaux temps réel

---

## 👨‍💻 Auteur

**AKAMGA Loïc** — M1 Data Engineering & IA, EFREI Paris
🎯 Alternance Data Engineer — Démarrage Septembre 2026

📧 loicakamga@gmail.com
🔗 [LinkedIn](https://linkedin.com)
🐙 [GitHub](https://github.com)

---

## 📜 Licence

MIT — projet personnel à des fins pédagogiques. Les données yfinance restent la propriété de Yahoo Finance.
