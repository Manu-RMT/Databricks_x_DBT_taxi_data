from _config_pipeline import *

### Etape pour :
#### La création des schemas
#### Mise en place du schemaLocalisation (on peut le definir à la main ou automatique)
#### Mise en place du schemaEvolutionMode -> en cas de nouvelle colonne, on la récupère dans une colonne "_rescued_data"
 

# Liste des données
entites = {
           "trips"     : "Historic of Trip Taxi",
           "vehicules" : "List of Vehicles",
           "payments"  : "Historic of Payment",
           "locations" : "Place of Taxi",
           "drivers"   : "List of Drivers",
           "customers" : "List of Customers" 
           }

# Création des tables de landing
for entity in entites:

    @dlt.table(
        name = f"taxi_landing_{entity}_incremental",
        comment = f"Taxi {entites[entity]} incremental"
    )

    def taxi_landing_incremental():
        raw_source_path = f"{VOLUME_SOURCE_PATH}/{entity}/"
        bronze_schema_autoload_path = f"{BRONZE_METADATA}schema_tracking/{entity}/"


        return (
            spark.readStream.format("cloudFiles")                       # Auto Loader pour ingestion incrémentale
                    .option("cloudFiles.format", "csv")         # Format source : CSV
                    .option("header", "true")                   # La 1ère ligne contient les en-têtes
                    .option("cloudFiles.includeExistingFiles", "true")  # Inclut les fichiers déjà présents au 1er run
                    .option("cloudFiles.schemaLocation", bronze_schema_autoload_path)  # Suivi du schéma entre runs
                    .option("cloudFiles.schemaEvolutionMode", "rescue") # Si nouvelle colonne, on le récupère dans cette colonne
                    .load(raw_source_path)                      # Chargement depuis le volume source
        )
