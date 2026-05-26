# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────
import json
import os
import boto3
import snowflake.connector

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────────
# CONFIGURATION MINIO
# ─────────────────────────────────────────────
MINIO_ENDPOINT   = "http://minio:9000"   # nom du conteneur Docker (pas localhost)
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME      = "bronze-transactions"

# dossier temporaire dans le conteneur Airflow
# pour stocker les fichiers téléchargés depuis MinIO
LOCAL_TMP_DIR = "/tmp/cac40_bronze"

# ─────────────────────────────────────────────
# CONFIGURATION SNOWFLAKE
# remplace les valeurs entre < >
# ─────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "user":      "LOIC",      # ex: LOIC
    "password":  "Merelouise2024",  # ton mot de passe Snowflake
    "account":   "BAOJXXC-WIB01844",  # ex: abc12345.eu-central-1
    "warehouse": "DBT_WAREHOUSE",
    "database":  "CAC40_MDS",
    "schema":    "COMMON",
}

# ─────────────────────────────────────────────
# TÂCHE 1 : télécharger les fichiers MinIO en local
# ─────────────────────────────────────────────
def download_from_minio():
    """
    Se connecte à MinIO via boto3 (API S3 compatible),
    liste tous les objets du bucket et les télécharge
    dans un dossier temporaire local du conteneur Airflow.
    """
    # connexion MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # créer le dossier temporaire s'il n'existe pas
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

    # lister tous les fichiers du bucket
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)

    if "Contents" not in response:
        print("[INFO] Bucket vide, rien à télécharger")
        return []

    fichiers_locaux = []

    for obj in response["Contents"]:
        key = obj["Key"]  # ex: MC.PA/1716300000.json

        # remplacer le slash par underscore pour le nom de fichier local
        nom_local = key.replace("/", "_")  # ex: MC.PA_1716300000.json
        chemin_local = os.path.join(LOCAL_TMP_DIR, nom_local)

        # téléchargement
        s3.download_file(BUCKET_NAME, key, chemin_local)
        fichiers_locaux.append(chemin_local)
        print(f"[OK] Téléchargé : {key} → {chemin_local}")

    print(f"[INFO] {len(fichiers_locaux)} fichiers téléchargés")
    return fichiers_locaux

# ─────────────────────────────────────────────
# TÂCHE 2 : charger les fichiers dans Snowflake
# ─────────────────────────────────────────────
def load_to_snowflake():
    """
    Lit chaque fichier JSON local et l'insère dans
    la table BRONZE_CAC40_QUOTES_RAW de Snowflake
    sous forme de VARIANT (JSON natif Snowflake).
    """
    # connexion Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    # lister les fichiers téléchargés
    if not os.path.exists(LOCAL_TMP_DIR):
        print("[INFO] Dossier temporaire vide, rien à charger")
        return

    fichiers = [f for f in os.listdir(LOCAL_TMP_DIR) if f.endswith(".json")]

    for nom_fichier in fichiers:
        chemin = os.path.join(LOCAL_TMP_DIR, nom_fichier)

        with open(chemin, "r") as f:
            contenu = json.load(f)

        # INSERT dans la table Bronze
        # PARSE_JSON() convertit la string JSON en type VARIANT Snowflake
        cursor.execute(
            "INSERT INTO BRONZE_CAC40_QUOTES_RAW (V) SELECT PARSE_JSON(%s)",
            (json.dumps(contenu),)
        )

        print(f"[OK] Inséré : {nom_fichier}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[INFO] {len(fichiers)} enregistrements chargés dans Snowflake")

# ─────────────────────────────────────────────
# DÉFINITION DU DAG AIRFLOW
# ─────────────────────────────────────────────
default_args = {
    "owner":            "airflow",
    "retries":          1,                        # 1 retry si la tâche échoue
    "retry_delay":      timedelta(minutes=5),     # attendre 5min avant retry
    "depends_on_past":  False,
}

with DAG(
    dag_id="minio_to_snowflake",                  # nom du DAG dans l'UI Airflow
    default_args=default_args,
    description="Charge les JSON CAC40 de MinIO vers Snowflake Bronze",
    schedule_interval="*/1 * * * *",              # toutes les minutes
    start_date=datetime(2026, 5, 21),             # date de début
    catchup=False,                                # ne pas rattraper les runs passés
    tags=["cac40", "bronze", "minio", "snowflake"],
) as dag:

    # tâche 1 : télécharger depuis MinIO
    tache_download = PythonOperator(
        task_id="download_from_minio",
        python_callable=download_from_minio,
    )

    # tâche 2 : charger dans Snowflake
    tache_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    # ordre d'exécution : download → load
    tache_download >> tache_load