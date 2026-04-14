{{ config(materialized="ephemeral") }}

{% set cols = 
[
"driver_id",
"first_name",
"last_name",
"full_name",
"phone_number",
"city",
"vehicle_id",
"driver_rating",
"last_updated_timestamp",
"modified_at"
]
%}

select 
    {% for col in cols %}
        {{ col }}
        {% if not loop.last %},{% endif %}
    {% endfor%}
from 
    {{ source("silver_layer","taxi_silver_drivers") }}