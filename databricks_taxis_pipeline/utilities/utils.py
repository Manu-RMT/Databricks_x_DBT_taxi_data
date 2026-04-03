from _config_pipeline import *

@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    This function checks if the given email address has a valid format using regex.
    Returns True if valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None


def create_quarantine(df, rules):
   """
   Identifie les lignes qui ne respectent pas au moins une règle.
   Ajoute une colonne 'failed_rules' listant les noms des règles non respectées.
   """
   if not rules:
       return df.limit(0)
   
   # Création d'une condition OR pour filtrer tout ce qui échoue
   failed_condition = " OR ".join([f"NOT ({cond})" for cond in rules.values()])
   return (df
       .filter(expr(failed_condition))
       .withColumn("failed_rules",
           array_remove(array([
               when(~expr(cond), lit(name)) for name, cond in rules.items()
           ]), None)
       )
       .withColumn("error_type",
            when(expr("_rescued_data IS NOT NULL"), "technical")
            .otherwise("business")
            )
   )

def clean_data_df(entity,df): 
   
    match entity:
        case "customers":
            return (
                df
                    .withColumn('first_name',upper(col('first_name')))
                    .withColumn('last_name',upper(col('last_name')))
                    .withColumn('city', upper(col('city')))
                    .withColumn('modified_at', current_timestamp()) 
                    .withColumn("customer_id",col('customer_id').cast('bigint'))
                    
                         
            )
        case "drivers":
            return (
                df
                    .withColumn('first_name',upper(col('first_name')))
                    .withColumn('last_name',upper(col('last_name')))
                    .withColumn('city', upper(col('city')))
                    .withColumn('modified_at', current_timestamp())
                    .withColumn("driver_id",col('driver_id').cast('bigint'))
                    .withColumn("vehicle_id",col('vehicle_id').cast('bigint'))
                    .withColumn("driver_rating",col('driver_rating').cast('float'))  
                   
            )
        case "locations": 
            return (
                df
                    .withColumn('city', upper(col('city')))
                    .withColumn('state', upper(col('state')))
                    .withColumn('country', upper(col('country')))
                    .withColumn("modified_at", current_timestamp())
                    .withColumn("location_id",col('location_id').cast('bigint'))
                    .withColumn("latitude",col('latitude').cast('float'))
                    .withColumn("longitude",col('longitude').cast('float'))
                   
            )
        case "payments":
            return (
                df
                    .withColumn('payment_method', upper(col('payment_method')))
                    .withColumn('payment_status', upper(col('payment_status')))
                    .withColumn('modified_at', current_timestamp())
                    .withColumn("payment_id",col('payment_id').cast('bigint'))
                    .withColumn("customer_id",col('customer_id').cast('bigint'))
                    .withColumn("trip_id",col('trip_id').cast('bigint'))
                    .withColumn("amount",col('amount').cast('float'))
                   
            )
        case "trips":
            return (
                df
                    .withColumn('start_location', upper(col('start_location')))
                    .withColumn('end_location', upper(col('end_location')))
                    .withColumn('payment_method', upper(col('payment_method')))
                    .withColumn('trip_status', upper(col('trip_status')))
                    .withColumn('modified_at', current_timestamp())
                    .withColumn("trip_id",col('trip_id').cast('bigint'))
                    .withColumn("driver_id",col('driver_id').cast('bigint'))
                    .withColumn("customer_id",col('customer_id').cast('bigint'))
                    .withColumn("distance_km",col('distance_km').cast('float'))
                    .withColumn("fare_amount",col('fare_amount').cast('float'))
                    .withColumn("trip_start_time",col('trip_start_time').cast('timestamp'))
                    .withColumn("trip_end_time",col('trip_end_time').cast('timestamp'))
                    
                   
            )
        case "vehicules":
            return (
                df
                    .withColumn('vehicle_type', upper(col('vehicle_type')))
                    .withColumn('model', upper(col('model')))
                    .withColumn('modified_at', current_timestamp())
                    .withColumn('vehicle_id',col('vehicle_id').cast('bigint'))
                   
            )

    return df
            


