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


# 1. On définit une fonction qui va créer la table pour une entité spécifique
def create_landing_table(entity_name, description):

   raw_source_path = f"{VOLUME_SOURCE_PATH}/{entity_name}/"
   bronze_schema_autoload_path = f"{BRONZE_METADATA}schema_tracking/{entity_name}/"

   @dlt.table(
       name = f"taxi_landing_{entity_name}_incremental",
       comment = f"Taxi {description} incremental"
   )
   def taxi_landing_incremental():
       return (
           spark.readStream.format("cloudFiles")
               .option("cloudFiles.format", "csv")
               .option("header", "true")
               .option("cloudFiles.includeExistingFiles", "true")
               .option("cloudFiles.schemaLocation", bronze_schema_autoload_path)
               .option("cloudFiles.schemaEvolutionMode", "rescue") 
               .load(raw_source_path)
       )

# 2. On appelle cette fonction dans la boucle
# ATTENTION DLT : Evite le Late Binding ou Laison Tardive (dernière valeur du dictionnaire)
for entity, desc in entites.items():
   create_landing_table(entity, desc)

 