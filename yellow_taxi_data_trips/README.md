<p align="center">
  <img src="https://img.shields.io/badge/Google%20Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white" alt="GCP"/>
  <img src="https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white" alt="BigQuery"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/SQL-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white" alt="SQL"/>
  <img src="https://img.shields.io/badge/Apache%20PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="PySpark"/>
</p>

<h1 align="center">🚕 NYC Yellow Taxi — Pipeline ELT sur GCP</h1>

<p align="center">
  <em>De l'ingestion des données brutes au Machine Learning — un projet Data Engineering de bout en bout sur Google Cloud Platform.</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-production-brightgreen?style=flat-square" alt="Status"/>
  <img src="https://img.shields.io/badge/data-2020--2025-blue?style=flat-square" alt="Data Range"/>
  <img src="https://img.shields.io/badge/rows-300M+-orange?style=flat-square" alt="Rows"/>
  <img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" alt="License"/>
</p>

---

## 📋 Table des Matières

- [Contexte Business](#-contexte-business)
- [Problématique Technique](#-problématique-technique)
- [Architecture Globale](#-architecture-globale)
- [Stack Technologique](#-stack-technologique)
- [Phase 1 — Extraction (Extract)](#-phase-1--extraction-extract)
- [Phase 2 — Chargement (Load)](#-phase-2--chargement-load)
- [Phase 3 — Transformation (Transform)](#-phase-3--transformation-transform)
- [Phase 4 — Orchestration (Airflow)](#-phase-4--orchestration-airflow)
- [Phase 5 — Analyse & Dashboarding](#-phase-5--analyse--dashboarding)
- [Phase 6 — Machine Learning (BigQuery ML)](#-phase-6--machine-learning-bigquery-ml)
- [Structure du Projet](#-structure-du-projet)
- [Déploiement & Exécution](#-déploiement--exécution)

---

## 🏙️ Contexte Business

La **New York City Taxi & Limousine Commission (TLC)** régule l'industrie du transport par taxi à New York. Chaque mois, des millions de courses effectuées par les taxis jaunes (*Yellow Cabs*) sont enregistrées : point de départ, destination, tarif, pourboire, nombre de passagers, mode de paiement, etc.

**Pourquoi ce projet est-il important ?**

Ces données représentent une mine d'or pour les décideurs du secteur des transports urbains. Elles permettent de répondre à des questions concrètes :

| Question Business | Valeur Ajoutée |
|---|---|
| Quels sont les horaires et zones de forte demande ? | Optimiser le positionnement des flottes de taxis |
| Comment le revenu évolue-t-il dans le temps ? | Anticiper les tendances saisonnières et ajuster les tarifs |
| Quels facteurs influencent le montant des pourboires ? | Améliorer la qualité de service pour maximiser les revenus |
| Quels aéroports génèrent le plus de courses ? | Allouer les ressources aux points stratégiques |
| Peut-on prédire le montant total d'une course ? | Fournir des estimations fiables aux passagers |

> **En résumé** : Ce projet construit un pipeline de données complet (**ELT**) qui collecte, nettoie, transforme et analyse les données de courses de taxis jaunes de New York, de 2020 à aujourd'hui, afin de produire des insights exploitables et des modèles prédictifs.

---

## 🔧 Problématique Technique

Traiter les données de taxis jaunes de NYC pose plusieurs défis techniques concrets :

```
📊 Volume        → Plus de 300 millions de lignes (2020 — 2025)
📁 Format        → Fichiers Parquet distribués mensuellement (~60 fichiers)
🔄 Fréquence     → Nouvelles données publiées chaque mois par la NYC TLC
⚠️ Qualité       → Valeurs manquantes, courses invalides, types incohérents
☁️ Infrastructure → Besoin d'un environnement scalable et managé (Cloud)
```

**L'approche ELT** (Extract → Load → Transform) a été choisie plutôt qu'un ETL classique. Pourquoi ?

- Les données brutes sont **chargées directement** dans BigQuery *avant* d'être transformées
- BigQuery, en tant que moteur analytique distribué, est **plus performant** pour les transformations SQL à grande échelle que des scripts Python locaux
- Cela permet de **conserver les données brutes** (traçabilité) tout en créant des vues et tables transformées pour l'analyse

```mermaid
graph LR
    A[🌐 Source NYC TLC] -->|Extract| B[☁️ Google Cloud Storage]
    B -->|Load| C[📊 BigQuery - Raw]
    C -->|Transform| D[✅ BigQuery - Cleaned]
    D --> E[📈 Vues Dashboard]
    D --> F[🤖 BigQuery ML]
```

---

## 🏗️ Architecture Globale

Le schéma ci-dessous illustre l'architecture complète du projet, de la source de données jusqu'aux résultats finaux :

```mermaid
flowchart TB
    subgraph SOURCE["🌐 SOURCE DE DONNÉES"]
        NYC["NYC TLC\nFichiers Parquet mensuels\n(2020 → aujourd'hui)"]
    end

    subgraph EXTRACT["📥 PHASE 1 — EXTRACTION"]
        DL["download_taxi_data.py\n• Téléchargement HTTP\n• Upload direct vers GCS\n• Détection de doublons\n• Logging dans GCS"]
    end

    subgraph STORAGE["☁️ GOOGLE CLOUD STORAGE"]
        GCS["Bucket : nyc-yellow-trips-data-buckets\n📁 dataset/trips/*.parquet\n📁 from-git/logs/*.log\n📁 from-git/*.py"]
    end

    subgraph LOAD["📤 PHASE 2 — CHARGEMENT"]
        CR["create_datasets.py\nCréation des datasets BigQuery"]
        LD["load_raw_trips_data.py\n• Détection incrémentale\n• Table temporaire + CAST\n• Insertion dans table finale"]
    end

    subgraph BQ_RAW["📊 BIGQUERY — RAW"]
        RAW["raw_yellowtrips.trips\n(données brutes + source_file)"]
        ZONE["raw_yellowtrips.taxi_zone\n(265 zones NYC)"]
    end

    subgraph TRANSFORM["🔄 PHASE 3 — TRANSFORMATION"]
        TR["transform_trips_data.py\nFiltrage SQL :\n• passenger_count > 0\n• trip_distance > 0\n• payment_type ≠ 6\n• total_amount > 0"]
    end

    subgraph BQ_CLEAN["✅ BIGQUERY — TRANSFORMED"]
        CLEAN["transformed_data.cleaned_and_filtered"]
    end

    subgraph VIEWS["📈 PHASE 5 — VUES ANALYTIQUES"]
        V1["views_fordashboard.*\n• demand_over_time\n• peak_hours_by_zone\n• popular_pickup_dropoff\n• total_fare_revenue\n• payment_type_trends\n• tipping_behavior\n• airport_trips_analysis\n• trip_duration_analysis\n• ..."]
    end

    subgraph ML["🤖 PHASE 6 — MACHINE LEARNING"]
        MLD["ml_dataset.trips_ml_data\n(données Nov 2024+)"]
        MOD["Modèles BigQuery ML :\n• Boosted Tree Regressor\n• Random Forest Regressor\n• DNN Regressor\n• AutoML Regressor"]
    end

    subgraph ORCH["⏰ PHASE 4 — ORCHESTRATION"]
        AIR["Apache Airflow DAG\nelt_pipeline_nyc_taxi\nDernier vendredi du mois, 23h"]
    end

    NYC --> DL
    DL --> GCS
    GCS --> CR
    CR --> LD
    LD --> RAW
    RAW --> TR
    TR --> CLEAN
    CLEAN --> VIEWS
    CLEAN --> MLD
    MLD --> MOD
    AIR -.->|orchestre| DL
    AIR -.->|orchestre| LD
    AIR -.->|orchestre| TR

    style SOURCE fill:#E3F2FD,stroke:#1565C0,color:#000
    style EXTRACT fill:#FFF3E0,stroke:#E65100,color:#000
    style STORAGE fill:#E8F5E9,stroke:#2E7D32,color:#000
    style LOAD fill:#F3E5F5,stroke:#6A1B9A,color:#000
    style BQ_RAW fill:#FCE4EC,stroke:#C62828,color:#000
    style TRANSFORM fill:#FFF8E1,stroke:#F57F17,color:#000
    style BQ_CLEAN fill:#E0F7FA,stroke:#00695C,color:#000
    style VIEWS fill:#F1F8E9,stroke:#33691E,color:#000
    style ML fill:#EDE7F6,stroke:#4527A0,color:#000
    style ORCH fill:#EFEBE9,stroke:#4E342E,color:#000
```

---

## 🛠️ Stack Technologique

| Catégorie | Outil | Rôle dans le projet |
|---|---|---|
| ☁️ **Cloud** | ![GCP](https://img.shields.io/badge/Google%20Cloud-4285F4?style=flat-square&logo=google-cloud&logoColor=white) | Infrastructure cloud (stockage, compute, ML) |
| 🗄️ **Stockage Objet** | ![GCS](https://img.shields.io/badge/Cloud%20Storage-AECBFA?style=flat-square&logo=googlecloudstorage&logoColor=black) | Data lake — stockage des fichiers Parquet bruts et des logs |
| 📊 **Data Warehouse** | ![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=flat-square&logo=googlebigquery&logoColor=white) | Entrepôt de données analytique, transformations SQL, ML intégré |
| 🐍 **Langage** | ![Python](https://img.shields.io/badge/Python%203-3776AB?style=flat-square&logo=python&logoColor=white) | Scripts d'extraction, chargement, et orchestration |
| 🗃️ **Requêtes** | ![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white) | Transformations, création de vues, entraînement de modèles ML |
| ⏰ **Orchestration** | ![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white) | Planification et exécution automatisée du pipeline ELT |
| 🔥 **Big Data** | ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white) | Analyse exploratoire sur fichiers Parquet volumineux |
| 📁 **Format** | ![Parquet](https://img.shields.io/badge/Apache%20Parquet-50ABF1?style=flat-square&logo=apacheparquet&logoColor=white) | Format columnar optimisé pour l'analytique |
| 🤖 **ML** | ![BQML](https://img.shields.io/badge/BigQuery%20ML-669DF6?style=flat-square&logo=googlebigquery&logoColor=white) | Entraînement et évaluation de modèles directement en SQL |

---

## 📥 Phase 1 — Extraction (Extract)

> **Objectif** : Télécharger les fichiers Parquet depuis le site de la NYC TLC et les stocker dans Google Cloud Storage.

**Script** : `download_taxi_data.py`

```mermaid
sequenceDiagram
    participant S as Script Python
    participant NYC as NYC TLC (Web)
    participant GCS as Google Cloud Storage

    S->>GCS: Le fichier existe déjà ?
    alt Fichier déjà présent
        GCS-->>S: ✅ Oui → Skip
    else Fichier absent
        S->>NYC: GET yellow_tripdata_YYYY-MM.parquet
        NYC-->>S: 📦 Fichier Parquet (~100 Mo)
        S->>GCS: Upload vers dataset/trips/
        S->>GCS: Upload log vers from-git/logs/
    end
```

### Étape 1 — Configuration

Le script définit les constantes du projet GCP : identifiant du projet, nom du bucket, et les chemins de stockage dans GCS.

```python
PROJECT_ID = "nyc-yellow-trips"
BUCKET_NAME = f"{PROJECT_ID}-data-buckets"
GCS_FOLDER = "dataset/trips/"
```

### Étape 2 — Vérification des doublons

Avant chaque téléchargement, le script vérifie si le fichier existe déjà dans GCS grâce à la méthode `blob.exists()`. Cela évite de re-télécharger des données déjà présentes et rend le processus **idempotent** (on peut le relancer sans risque de duplication).

### Étape 3 — Téléchargement & Upload

Pour chaque mois de 2020 à aujourd'hui, le script :
1. Construit l'URL du fichier Parquet sur le CDN de la NYC TLC
2. Effectue une requête HTTP `GET` avec streaming
3. Upload le contenu directement vers le bucket GCS (sans écriture locale sur disque)

### Étape 4 — Logging centralisé

Toutes les opérations sont tracées dans un fichier log uploadé dans GCS (`from-git/logs/`), ce qui permet un audit complet de chaque exécution.

---

## 📤 Phase 2 — Chargement (Load)

> **Objectif** : Charger les fichiers Parquet depuis GCS vers BigQuery, en gérant les incohérences de schéma.

**Scripts** : `create_datasets.py` + `load_raw_trips_data.py`

```mermaid
sequenceDiagram
    participant P as Script Python
    participant GCS as Cloud Storage
    participant BQ as BigQuery

    Note over P: Étape 1 - Créer les datasets
    P->>BQ: CREATE DATASET raw_yellowtrips
    P->>BQ: CREATE DATASET transformed_data
    P->>BQ: CREATE DATASET views_fordashboard

    Note over P: Étape 2 - Chargement incrémental
    P->>BQ: Quels fichiers sont déjà chargés ?
    BQ-->>P: Liste (via colonne source_file)
    P->>GCS: Quels fichiers sont disponibles ?
    GCS-->>P: Liste des .parquet

    Note over P: Étape 3 - Pour chaque nouveau fichier
    P->>BQ: Load Parquet → table_temp (autodetect)
    P->>BQ: INSERT INTO trips SELECT ... CAST(passenger_count AS FLOAT64) ... FROM table_temp
    P->>BQ: DROP table_temp
```

### Étape 1 — Création des Datasets BigQuery

Le script `create_datasets.py` initialise **3 datasets** dans BigQuery :

| Dataset | Rôle |
|---|---|
| `raw_yellowtrips` | Données brutes (table `trips` + table `taxi_zone`) |
| `transformed_data` | Données nettoyées et filtrées |
| `views_fordashboard` | Vues SQL pour les dashboards |

Le script vérifie l'existence de chaque dataset avant de le créer, ce qui le rend **idempotent**.

### Étape 2 — Détection incrémentale

Le script `load_raw_trips_data.py` compare :
- La liste des fichiers **déjà chargés** (via la colonne `source_file` dans BigQuery)
- La liste des fichiers **disponibles** dans le bucket GCS

Seuls les **nouveaux fichiers** sont traités. C'est ce qu'on appelle un **chargement incrémental** : à chaque exécution, seules les données fraîches sont ajoutées.

### Étape 3 — Chargement avec gestion de schéma

Certains fichiers Parquet ont des types de colonnes différents (par exemple, `passenger_count` peut être `INT64` dans un fichier et `FLOAT64` dans un autre). Pour gérer cela :

1. Le fichier est d'abord chargé dans une **table temporaire** avec `autodetect=True`
2. Un `INSERT INTO ... SELECT ... CAST(...)` harmonise les types avant l'insertion finale
3. La table temporaire est supprimée après usage

Ce pattern **table temporaire → transformation → insertion** est une pratique courante en Data Engineering pour gérer les schémas hétérogènes.

---

## 🔄 Phase 3 — Transformation (Transform)

> **Objectif** : Nettoyer les données brutes en appliquant des filtres de qualité pour produire un dataset fiable.

**Script** : `transform_trips_data.py`

```mermaid
flowchart LR
    A["🗄️ raw_yellowtrips.trips\n(données brutes)"] --> B{"🔍 Filtres de qualité"}
    B -->|passenger_count > 0| C["✅"]
    B -->|trip_distance > 0| C
    B -->|payment_type ≠ 6| C
    B -->|total_amount > 0| C
    C --> D["✅ transformed_data\n.cleaned_and_filtered"]

    style A fill:#FFCDD2,stroke:#C62828,color:#000
    style D fill:#C8E6C9,stroke:#2E7D32,color:#000
```

### Règles de filtrage appliquées

Le script exécute une requête SQL `CREATE OR REPLACE TABLE` qui applique 4 filtres :

| Filtre | Raison | Données exclues |
|---|---|---|
| `passenger_count > 0` | Une course sans passager est une anomalie | Courses fantômes / erreurs de capteur |
| `trip_distance > 0` | Une distance nulle indique un enregistrement invalide | Courses annulées non nettoyées |
| `payment_type ≠ 6` | Le type 6 = "Voided trip" (course annulée) | Courses officiellement annulées |
| `total_amount > 0` | Un montant négatif ou nul est incohérent | Erreurs de facturation / remboursements |

> 💡 **Pourquoi transformer dans BigQuery ?** Plutôt que de nettoyer en Python (ce qui nécessiterait de charger toutes les données en mémoire), la transformation est effectuée **directement en SQL dans BigQuery**. Le moteur distribué de BigQuery traite des centaines de millions de lignes en quelques secondes — c'est l'avantage principal du pattern **ELT** par rapport à l'ETL classique.

---

## ⏰ Phase 4 — Orchestration (Airflow)

> **Objectif** : Automatiser l'exécution séquentielle du pipeline ELT à intervalle régulier.

**Script** : `elt_dag_pipeline.py`

```mermaid
flowchart LR
    A["⏳ wait_for_last_friday\nTimeDeltaSensor"] --> B["📥 download_taxi_data\nBashOperator"]
    B --> C["📤 load_raw_trips_data\nBashOperator"]
    C --> D["🔄 transform_trips_data\nBashOperator"]

    style A fill:#EFEBE9,stroke:#4E342E,color:#000
    style B fill:#FFF3E0,stroke:#E65100,color:#000
    style C fill:#F3E5F5,stroke:#6A1B9A,color:#000
    style D fill:#E0F7FA,stroke:#00695C,color:#000
```

### Configuration du DAG

| Paramètre | Valeur | Explication |
|---|---|---|
| **Schedule** | `0 23 * * 5` | Chaque vendredi à 23h |
| **Condition** | Dernier vendredi du mois | Aligné sur le calendrier de publication NYC TLC |
| **Retries** | 2 tentatives | Résilience face aux erreurs réseau |
| **Retry delay** | 5 minutes | Temps d'attente entre chaque tentative |
| **Catchup** | `False` | N'exécute pas les runs passés manqués |

### Fonctionnement des tâches

Chaque tâche `BashOperator` :
1. **Récupère le script Python** depuis GCS (`gsutil cp`)
2. **Exécute le script** (`python3`)

Les scripts sont stockés dans GCS (`from-git/`) plutôt qu'en local sur la VM Airflow, ce qui facilite les mises à jour sans redéploiement.

### Chaîne de dépendances

```
wait_for_last_friday >> download_taxi_data >> load_raw_trips_data >> transform_trips_data
```

L'opérateur `>>` d'Airflow garantit que chaque tâche ne démarre qu'après le succès de la précédente. Si l'extraction échoue, le chargement n'est jamais déclenché.

---

## 📈 Phase 5 — Analyse & Dashboarding

> **Objectif** : Créer des vues SQL analytiques exploitables par un outil de visualisation (Looker Studio, Streamlit, etc.).

**Scripts** : `MarketDemand_and_CustomerBehavior.sql`, `Financial_and_Pricing.sql`, `CompetitiveInsights.sql`

Les analyses sont organisées en **4 axes stratégiques**, chacun répondant à des questions business précises :

```mermaid
mindmap
  root((📊 Analyses))
    🔥 Demande & Saisonnalité
      Fluctuation temporelle
      Heures de pointe par zone
      Impact météo / événements
    🧑‍💼 Comportement Client
      Zones populaires pickup/dropoff
      Distance moyenne par borough
      Passager unique vs multiple
    💰 Finance & Tarification
      Évolution du revenu total
      Tarif moyen par trip
      Modes de paiement
      Comportement pourboire
      Revenus surcharges
    🏆 Insights Opérationnels
      Volume par borough/zone
      Analyse aéroports
      Codes tarifaires
      Durée des courses
```

### Vues créées dans `views_fordashboard`

| # | Vue | Question Business |
|---|---|---|
| 1 | `demand_over_time` | Comment la demande fluctue-t-elle (jour, semaine, mois, saison) ? |
| 2 | `peak_hours_by_zone` | Quelles sont les heures de pointe par borough et zone ? |
| 3 | `popular_pickup_dropoff` | Quels sont les lieux de prise en charge et de dépose les plus populaires ? |
| 4 | `avg_trip_distance_analysis` | Quelle est la distance moyenne par borough, heure et saison ? |
| 5 | `passenger_trends_by_season` | Passager unique vs multiples : y a-t-il une saisonnalité ? |
| 6 | `total_fare_revenue_over_time` | Comment le revenu total évolue-t-il dans le temps ? |
| 7 | `avg_fare_analysis` | Quel est le tarif moyen par trip, borough et heure ? |
| 8 | `payment_type_trends` | Quelle proportion carte vs cash, et comment évolue-t-elle ? |
| 9 | `tipping_behavior_analysis` | Quels facteurs influencent les pourboires ? |
| 10 | `additional_charges_revenue` | Combien rapportent les surcharges (MTA, congestion, aéroport) ? |
| 11 | `trip_volume_by_borough` | Quels boroughs ont le plus/moins de courses ? |
| 12 | `airport_trips_analysis` | Fréquence et tarif moyen des courses aéroport (JFK, LGA, EWR) ? |
| 13 | `rate_code_analysis` | Distribution des codes tarifaires par borough ? |
| 14 | `trip_duration_analysis` | Durée moyenne des courses et tendance dans le temps ? |

> 💡 Les vues BigQuery sont **virtuelles** : elles ne stockent pas de données mais exécutent la requête à la demande. Cela évite la duplication tout en offrant des résultats toujours à jour.

---

## 🤖 Phase 6 — Machine Learning (BigQuery ML)

> **Objectif** : Prédire le montant total d'une course (`total_amount`) à l'aide de modèles ML entraînés directement dans BigQuery.

**Scripts** : `create_ml_dataset_table.py` + `modeling_queries.sql`

### Étape 1 — Préparation du dataset ML

Le script `create_ml_dataset_table.py` crée une table dédiée au ML en filtrant les données récentes (à partir de novembre 2024) et en ne conservant que les paiements par carte ou cash (`payment_type IN (1, 2)`).

### Étape 2 — Entraînement des modèles

Quatre modèles sont entraînés et comparés, tous avec `total_amount` comme variable cible :

```mermaid
flowchart TB
    DATA["📦 ml_dataset.preprocessed_train_data"] --> M1["🌲 Boosted Tree\nRegressor"]
    DATA --> M2["🌳 Random Forest\nRegressor"]
    DATA --> M3["🧠 DNN\nRegressor"]
    DATA --> M4["🤖 AutoML\nRegressor"]

    M1 --> EVAL["📊 ML.EVALUATE\n(sur test_data)"]
    M2 --> EVAL
    M3 --> EVAL
    M4 --> EVAL

    EVAL --> PRED["🔮 ML.PREDICT"]
    EVAL --> EXPLAIN["💡 ML.GLOBAL_EXPLAIN\n(feature importance)"]

    style DATA fill:#E8EAF6,stroke:#283593,color:#000
    style EVAL fill:#FFF8E1,stroke:#F57F17,color:#000
    style PRED fill:#E8F5E9,stroke:#2E7D32,color:#000
    style EXPLAIN fill:#FCE4EC,stroke:#C62828,color:#000
```

| Modèle | Type | Caractéristique |
|---|---|---|
| **Boosted Tree Regressor** | Gradient Boosting | Performant sur données tabulaires, rapide à entraîner |
| **Random Forest Regressor** | Ensemble de décision | Robuste aux outliers, bonne généralisation |
| **DNN Regressor** | Réseau de neurones profond | Capture des relations non-linéaires complexes |
| **AutoML Regressor** | Sélection automatique | Google choisit et optimise le meilleur algorithme |

### Étape 3 — Évaluation & Interprétabilité

- **`ML.EVALUATE`** : Calcule les métriques de performance (MAE, RMSE, R²) sur le jeu de test
- **`ML.PREDICT`** : Génère des prédictions sur de nouvelles données
- **`ML.GLOBAL_EXPLAIN`** : Identifie les variables les plus influentes (feature importance)

> 💡 **Avantage BigQuery ML** : Pas besoin de configurer un environnement Python séparé (TensorFlow, scikit-learn). L'entraînement, l'évaluation et les prédictions se font **entièrement en SQL**, directement sur les données stockées dans BigQuery.

---

## 📂 Structure du Projet

```
📦 taxitripapp/
│
├── 📥 EXTRACTION
│   └── download_taxi_data.py          # Téléchargement NYC TLC → GCS
│
├── 📤 CHARGEMENT
│   ├── create_datasets.py             # Initialisation des datasets BigQuery
│   └── load_raw_trips_data.py         # Chargement incrémental GCS → BigQuery
│
├── 🔄 TRANSFORMATION
│   └── transform_trips_data.py        # Nettoyage et filtrage des données
│
├── ⏰ ORCHESTRATION
│   └── elt_dag_pipeline.py            # DAG Airflow (planification mensuelle)
│
├── 📈 ANALYSE
│   ├── exploratory_data_analysis.py   # EDA avec PySpark
│   ├── MarketDemand_and_CustomerBehavior.sql
│   ├── Financial_and_Pricing.sql
│   └── CompetitiveInsights.sql
│
├── 🤖 MACHINE LEARNING
│   ├── create_ml_dataset_table.py     # Préparation du dataset ML
│   ├── modeling_queries.sql           # Entraînement / évaluation BigQuery ML
│   ├── Custom_Model.ipynb             # Modèle personnalisé (notebook)
│   └── Report_2/3/4.ipynb             # Rapports d'analyse
│
├── 📋 RÉFÉRENCE
│   ├── taxi_zone_lookup.csv           # Mapping des 265 zones NYC
│   └── README.md                      # Ce fichier
```

---

## 🚀 Déploiement & Exécution

### Prérequis

```bash
# 1. Avoir un projet GCP actif avec la facturation activée
gcloud projects list

# 2. Activer les APIs nécessaires
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable composer.googleapis.com  # Pour Airflow managé

# 3. Installer les dépendances Python
pip install google-cloud-bigquery google-cloud-storage requests pyarrow pyspark
```

### Exécution manuelle (étape par étape)

```bash
# Phase 1 — Extraction
python download_taxi_data.py

# Phase 2 — Chargement
python create_datasets.py
python load_raw_trips_data.py

# Phase 3 — Transformation
python transform_trips_data.py

# Phase 4 — ML (optionnel)
python create_ml_dataset_table.py
# Puis exécuter modeling_queries.sql dans la console BigQuery
```

### Exécution automatisée (Airflow)

Déployer le fichier `elt_dag_pipeline.py` dans le dossier `dags/` de votre instance Cloud Composer. Le pipeline s'exécutera automatiquement le dernier vendredi de chaque mois à 23h.

---

## 📊 Données de Référence

Le fichier `taxi_zone_lookup.csv` contient le mapping des **265 zones de taxi** de New York :

| Colonne | Description |
|---|---|
| `LocationID` | Identifiant unique de la zone (1–265) |
| `Borough` | Arrondissement (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR) |
| `Zone` | Nom de la zone (ex: "JFK Airport", "Times Sq/Theatre District") |
| `service_zone` | Type de zone de service (Yellow Zone, Boro Zone, Airports) |

---

<p align="center">
  <img src="https://img.shields.io/badge/Built%20with-Google%20Cloud%20Platform-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white" alt="Built with GCP"/>
</p>

<p align="center">
  <em>Projet réalisé par <strong>Josué Afouda</strong> — Pipeline ELT de bout en bout sur GCP</em>
</p>
