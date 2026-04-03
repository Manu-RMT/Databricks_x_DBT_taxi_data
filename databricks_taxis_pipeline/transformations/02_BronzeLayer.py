from _config_pipeline import *
from utilities.utils import *

## Etape pour 
#### Gerer tous les expectations 
#### Créer une table quarantaine en cas d'erreur
#### Nettoyage Simple, Upper, Trim


# Liste des données
entites = {
           "trips"     : "Historic of Trip Taxi",
           "vehicules" : "List of Vehicles",
           "payments"  : "Historic of Payment",
           "locations" : "Place of Taxi",
           "drivers"   : "List of Drivers",
           "customers" : "List of Customers" 
           }


valid_rules_or_fail = {
   "customers" : 
        {
        "valid_customer_id" : "CAST(customer_id as bigint)  IS NOT NULL"
        },
    "drivers" : 
        {
        "valid_driver_id"   : "CAST(driver_id as bigint) IS NOT NULL"
        },
     "locations" : 
        {
        "valid_location_id" : "CAST(location_id as bigint) IS NOT NULL"
        },
     "payments" :
        {
        "valid_payment_id"  : "CAST(payment_id as bigint) IS NOT NULL",
        "valid_customer_id" : "CAST(customer_id as bigint) IS NOT NULL",
        "valid_trip_id"     : "CAST(trip_id as bigint) IS NOT NULL"
        },
    "trips" :
        {
        "valid_trip_id"     : "CAST(trip_id as bigint) IS NOT NULL",
        "valid_customer_id" : "CAST(customer_id as bigint) IS NOT NULL",
        "valid_driver_id"   : "CAST(driver_id as bigint) IS NOT NULL",
        "valid_vehicule_id" : "CAST(vehicle_id as bigint) IS NOT NULL"
        },
    "vehicules" :
        {
        "valid_vehicule_id" : "CAST(vehicle_id as bigint) IS NOT NULL"
        }
}

valid_rules_or_drop = {

    "customers" : 
        {
        "valid_first_name"  : "first_name IS NOT NULL",
        "valid_last_name"   : "last_name IS NOT NULL",
        "valid_email":         r"email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9][a-zA-Z0-9._-]*@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}$'",
        "valid_phone":         "phone_number IS NOT NULL",
        "valid_city" :         "city IS NOT NULL",
        "signup_date" :        "CAST(signup_date as date) IS NOT NULL",
        "last_updated_timestamp": "CAST(last_updated_timestamp as timestamp) IS NOT NULL",
       
        },
    "drivers" : 
        {
        "valid_first_name"  : "first_name IS NOT NULL",
        "valid_last_name"   : "last_name IS NOT NULL",
        "valid_phone":        "phone_number IS NOT NULL",
        "valid_vehicule_id" : "CAST(vehicle_id as int) IS NOT NULL",
        "valid_driver_rating" : "CAST(driver_rating as double) IS NOT NULL",
        "valid_city" :        "city IS NOT NULL",
        "last_updated_timestamp": "CAST(last_updated_timestamp as timestamp) IS NOT NULL",
       
        },
    "locations" : 
        {
        "valid_city" : "city IS NOT NULL",
        "valid_state" : "state IS NOT NULL",
        "valid_country" : "country IS NOT NULL",
        "valid_latitude" : "CAST(latitude as double) IS NOT NULL",
        "valid_longitude" : "CAST(longitude as double) IS NOT NULL",
        "last_updated_timestamp": "CAST(last_updated_timestamp as timestamp) IS NOT NULL",
                 
        },
    "payments" :
        {
        "valid_payment_method" : "payment_method IS NOT NULL",
        "valid_payment_status" : "payment_status IS NOT NULL",
        "valid_payment_amount" : "CAST(amount as double) IS NOT NULL",
        "valid_transaction_time" : "CAST(transaction_time as timestamp) IS NOT NULL",
        "last_updated_timestamp": "CAST(last_updated_timestamp as timestamp) IS NOT NULL",
       
        },
    "trips" :
        {
        "valid_trip_start_time" : "CAST(trip_start_time as timestamp) IS NOT NULL",
        "valid_trip_end_time" : "CAST(trip_end_time as timestamp) IS NOT NULL",
        "valid_trip_start_location" : "start_location IS NOT NULL", 
        "valid_trip_end_location" : "end_location IS NOT NULL",
        "valid_trip_distance_km" : "CAST(distance_km as double) IS NOT NULL",
        "valid_fare_amount" : "CAST(fare_amount as double) IS NOT NULL",
        "valid_payment_method" : "payment_method IS NOT NULL",
        "valid_trip_status" : "trip_status IS NOT NULL",
        "last_updated_timestamp": "CAST(last_updated_timestamp as timestamp) IS NOT NULL",
        
        },
    "vehicules" :
        {
        "valid_license_plate" : "license_plate IS NOT NULL",
        "valid_year" : "CAST(year as int) IS NOT NULL",
        "valid_vehicule_type" : "vehicle_type IS NOT NULL",
        "last_updated_timestamp": "CAST(last_updated_timestamp as timestamp) IS NOT NULL",
        
        }  
}


def create_bronze_table(entity):
    # Table Data OK
    @dlt.table(
    name=f"taxi_bronze_{entity}_ingestion_cleaned",
    comment=f"Taxi {entity} ingested from bronze layer"
    )
    @dlt.expect_all_or_fail(valid_rules_or_fail[entity])
    @dlt.expect_all_or_drop(valid_rules_or_drop[entity])
    def taxi_bronze_ingestion_cleaned():
        df = dlt.read_stream(f"taxi_landing_{entity}_incremental")
        df = clean_data_df(entity,df)
        df = df.drop("_rescued_data")
        return df
 
    @dlt.table(
        name=f"taxi_bronze_{entity}_data_failed",
        comment=f"Failed : Taxi {entity} ingested from bronze layer"    
    )
    @dlt.expect_all_or_drop(valid_rules_or_drop[entity])
    def taxi_bronze_data_failed():
        df = dlt.read_stream(f"taxi_landing_{entity}_incremental")
        return create_quarantine(df, valid_rules_or_drop[entity])
   
for entity in entites:
    print(f"Traitement de la table : {entity}")
    create_bronze_table(entity)
 