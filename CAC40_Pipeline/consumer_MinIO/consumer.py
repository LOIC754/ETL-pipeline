# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────
import json
import time
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────
# CONFIGURATION MINIO
# port 9001 = port API S3 de MinIO (défini dans docker-compose)
# ─────────────────────────────────────────────
MINIO_ENDPOINT    = "http://localhost:9000"
MINIO_ACCESS_KEY  = "admin"
MINIO_SECRET_KEY  = "password123"
BUCKET_NAME       = "bronze-transactions"

# ─────────────────────────────────────────────
# CONFIGURATION KAFKA
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC           = "cac40_quotes"

# group_id : Kafka sauvegarde les offsets par groupe
# si le consumer redémarre, il reprend là où il s'est arrêté
GROUP_ID = "bronze-consumer"

# ─────────────────────────────────────────────
# CONNEXION MINIO (client S3 boto3)
# boto3 est la librairie AWS SDK — MinIO expose
# la même API que S3, donc boto3 fonctionne directement
# ─────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# ─────────────────────────────────────────────
# VÉRIFICATION : le bucket existe-t-il ?
# ─────────────────────────────────────────────
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    print(f"[INFO] Bucket '{BUCKET_NAME}' trouvé ✓")
except ClientError:
    # crée le bucket s'il n'existe pas encore
    s3.create_bucket(Bucket=BUCKET_NAME)
    print(f"[INFO] Bucket '{BUCKET_NAME}' créé ✓")

# ─────────────────────────────────────────────
# INITIALISATION DU CONSUMER KAFKA
# ─────────────────────────────────────────────
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,

    # auto_offset_reset='earliest' : relit tous les messages
    # depuis le début au premier démarrage du groupe
    auto_offset_reset="earliest",

    # enable_auto_commit : Kafka sauvegarde automatiquement
    # la position du consumer toutes les 5s
    enable_auto_commit=True,

    # désérialisation : convertit les bytes Kafka en dict Python
    # inverse du value_serializer du producer
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ─────────────────────────────────────────────
# BOUCLE PRINCIPALE : lecture Kafka → écriture MinIO
# ─────────────────────────────────────────────
print(f"[INFO] Consumer démarré — topic : {TOPIC}")
print(f"[INFO] Destination : MinIO bucket '{BUCKET_NAME}'\n")

for message in consumer:
    # message.value = dict Python (déjà désérialisé)
    record = message.value

    # récupération du symbole et du timestamp pour nommer le fichier
    symbole   = record.get("symbole", "UNKNOWN")
    timestamp = record.get("fetch_timestamp", int(time.time()))

    # clé S3 = chemin du fichier dans le bucket
    # format : symbole/timestamp.json  ex: MC.PA/1716300000.json
    # le slash "/" crée un "dossier virtuel" dans MinIO
    key = f"{symbole}/{timestamp}.json"

    # écriture du fichier JSON dans MinIO
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(record).encode("utf-8"),  # bytes JSON
        ContentType="application/json",
    )

    print(f"[OK] Sauvegardé → {BUCKET_NAME}/{key}")