from _config_pipeline import *
from utilities.utils import *
from pyspark import pipelines as dp

## Etape pour 
#### Gerer les transformations majeurs ( tel, domain, lignes dupliquées)



# Liste des données
entites = {
           "trips"     : "Historic of Trip Taxi cleaned",
           "vehicules" : "List of Vehicles cleaned",
           "payments"  : "Historic of Payment cleaned",
           "locations" : "Localisation of Taxi cleaned",
           "drivers"   : "Drivers cleaned",
           "customers" : "Customers Cleaned" 
           }


## Customer 

@dp.materialized_view(
    name = f"{SILVER_ZONE}.taxi_silver_customers",
    comment = entites["customers"],
  )

def silver_customers():
    df = spark.read.table(f"taxi_bronze_customers_ingestion_cleaned")
    df = delete_duplicates(df,['customer_id'],"last_updated_timestamp")
    df = df.withColumn('phone_number', regexp_replace(col('phone_number'), r'[^0-9+]', ''))
    df = df.withColumn('email', regexp_replace(col('email'), r'[^a-zA-Z0-9@.]', ''))
    df = df.withColumn('domain', regexp_replace(col('email'), r'^.*@', ''))
    df = df.withColumn('full_name',concat_ws(' ',col('first_name'),col('last_name')))
    df = df.withColumn('modified_at',current_timestamp())
  

    return df


  
## Driver 

@dp.materialized_view(
    name = f"{SILVER_ZONE}.taxi_silver_drivers",
    comment = entites["drivers"],
  )

def silver_drivers():
    df = spark.read.table(f"taxi_bronze_drivers_ingestion_cleaned")
    df = delete_duplicates(df,['driver_id','vehicle_id'],"last_updated_timestamp")
    df = df.withColumn('phone_number', regexp_replace(col('phone_number'), r'[^0-9+]', ''))
    df = df.withColumn('full_name',concat_ws(' ',col('first_name'),col('last_name')))
    df = df.withColumn('modified_at',current_timestamp())
    return df


## Locations

@dp.materialized_view(name=f"{SILVER_ZONE}.taxi_silver_locations", comment=entites["locations"])
def silver_locations():
    df = spark.read.table(f"taxi_bronze_locations_ingestion_cleaned")
    df = delete_duplicates(df,['location_id'],"last_updated_timestamp")
    df = df.withColumn('modified_at',current_timestamp())
    return df


## Payments

@dp.materialized_view(name=f"{SILVER_ZONE}.taxi_silver_payments", comment=entites["payments"])
def silver_payments():
    df = spark.read.table(f"taxi_bronze_payments_ingestion_cleaned")
    df = delete_duplicates(df,['payment_id','trip_id','customer_id'],"last_updated_timestamp")
    df = df.withColumn('payment_status_final', 
                        when( ((col("payment_method") == "CARD") & (col("payment_status") == "SUCCESS")),"ONLINE_SUCCESS")
                        .when( ((col("payment_method") == "CARD") & (col("payment_status") == "FAILED")),"ONLINE_FAILED")
                        .when( ((col("payment_method") == "CARD") & (col("payment_status") == "PENDING")),"ONLINE_PENDING")
                        .when( ((col("payment_method") != "CARD") & (col("payment_status") == "SUCCESS")),"OFFLINE_SUCCESS")
                        .when( ((col("payment_method") != "CARD") & (col("payment_status") == "FAILED")),"OFFLINE_FAILED")
                        .when( ((col("payment_method") != "CARD") & (col("payment_status") == "PENDING")),"ONLINE_PENDING")
                        .otherwise("INVALID")
                        )
    df = df.withColumn('amount', floor(col('amount'),2))
    df = df.withColumn('modified_at',current_timestamp())
    return df
  
## Trips

@dp.materialized_view(name=f"{SILVER_ZONE}.taxi_silver_trips", comment=entites["trips"])
def silver_trips():
    df = spark.read.table(f"taxi_bronze_trips_ingestion_cleaned")
    df = delete_duplicates(df,['trip_id','customer_id','driver_id','vehicle_id'],"last_updated_timestamp")
    df = df.withColumn('trip_status_final', 
                    when( ((col("payment_method") == "CARD") & (col("trip_status") == "COMPLETED")),"TRIP_ONLINE_SUCCESS")
                    .when( ((col("payment_method") == "CARD") & (col("trip_status") == "CANCELLED")),"TRIP_ONLINE_CANCELLED")
                    .when( ((col("payment_method") == "CARD") & (col("trip_status") == "ONGOING")),"TRIP_ONLINE_ONGOING")
                    .when( ((col("payment_method") != "CARD") & (col("trip_status") == "COMPLETED")),"TRIP_CASH_COMPLETED")
                    .when( ((col("payment_method") != "CARD") & (col("trip_status") == "CANCELLED")),"TRIP_CASH_CANCELLED")
                    .when( ((col("payment_method") != "CARD") & (col("trip_status") == "ONGOING")),"TRIP_CASH_ONGOING")
                    .otherwise("INVALID")
                    ) 
    df = df.withColumn('modified_at',current_timestamp())
    return df

## Vehicules

@dp.materialized_view(name=f"{SILVER_ZONE}.taxi_silver_vehicules", comment=entites["vehicules"])
def silver_vehicules():
    df = spark.read.table(f"taxi_bronze_vehicules_ingestion_cleaned")
    df = delete_duplicates(df,['vehicle_id'],"last_updated_timestamp")
    df = df.withColumn('modified_at',current_timestamp())
    return df
