from _config_pipeline import *

## Etape pour 
#### Gerer tous les expectations 
#### Créer une table quarantaine en cas d'erreur

VALID_RULES= {

  "customers" : 
      {
      "valid_customer_id" : "customer_id IS NOT NULL",
      "vaild_first_name"  : "first_name IS NOT NULL",
      "valid_last_name"   : "last_name IS NOT NULL",
      "valid_email":         r"email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9][a-zA-Z0-9._-]*@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}$'",
      "valid_phone":         "phone_number IS NOT NULL",
      "valid_city" :         "city IS NOT NULL",
      "signup_date" :        "signup_date IS NOT NULL",
      "last_updated_timestamp": "last_updated_timestamp IS NOT NULL",
      },
  "drivers" : 
      {
      "valid_customer_id" : "test IS NOT NULL"
      }
}




