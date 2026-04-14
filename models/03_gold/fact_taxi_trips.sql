{{ config(materialize='incremental', uninque_key='trip_id') }}

{% set colonnes = [ 
'trip_id',
'driver_id',
'customer_id',
'vehicle_id',
'trip_start_time',
'trip_end_time',
'start_location',
'end_location',
'distance_km',
'fare_amount',
'payment_method',
'trip_status',
'trip_status_final',
'last_updated_timestamp',
'modified_at'
]
%}



-- Sécurité anti-full-refresh
{% if flags.FULL_REFRESH %}
 {{ exceptions.raise_compiler_error("Le full-refresh est interdit sur ce modèle pour protéger les coûts. Contactez l'admin.") }}
{% endif %}

SELECT 
    {% for col in colonnes %}
        {{ col }}
        {% if not loop.last  %},{% endif %}   
    {% endfor %}
FROM 
{{ source("silver_layer","taxi_silver_trips") }}

{% if is_incremental() %}
    where modified_at > (select max(modified_at) from {{this}})
{%endif %}