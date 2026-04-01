# config/config.py

# ----------------------
# Catalog & Schemas
# ----------------------
CATALOG = "workspace"        # Nom du catalog Unity
RAW_SCHEMA = "raw_taxi"          # Schema temporaire pour RAW
BRONZE_SCHEMA = "01_bronze"     # Schema pour Bronze (raw)
SILVER_SCHEMA = "02_silver"     # Schema pour Silver (Delta nettoyé)
GOLD_SCHEMA = "03_gold"         # Schema pour Gold (BI-ready)
PROJET_NAME = "taxi"

# ----------------------
# Source Volume 
# ----------------------
VOLUME_RAW_NAME = f"{RAW_SCHEMA}"
VOLUME_SOURCE_PATH = f"/Volumes/{CATALOG}/datasets/"+ VOLUME_RAW_NAME # Volume contenant tous les CSV

# ----------------------
# Destination Ingestion Data couche Bronze 
# ----------------------
BRONZE_DATA = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/bronzevolume/data/{PROJET_NAME}/"
BRONZE_METADATA = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/bronzevolume/_metadata/{PROJET_NAME}/"

# ----------------------
# Diffentes couches 
# ----------------------
BRONZE_ZONE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER_ZONE = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD_ZONE = f"{CATALOG}.{GOLD_SCHEMA}"

# ----------------------
# PATH SAVE FILES GOLD
# ----------------------
GOLD_PATH_DATA = f"/Volumes/{CATALOG}/{GOLD_SCHEMA}/goldvolume/data/{PROJET_NAME}/"
