{{ config(materialized="ephemeral") }}

{% set cols = 
[
'customer_id',
'first_name',
'last_name',
'full_name',
'email',
'domain',
'phone_number',
'city',
'signup_date',
'last_updated_timestamp',
'modified_at'
]
%}

select 
    {% for col in cols %}
        {{ col }}
        {% if not loop.last %},{% endif %}
    {% endfor%}
from 
    {{ source("silver_layer","taxi_silver_customers") }}