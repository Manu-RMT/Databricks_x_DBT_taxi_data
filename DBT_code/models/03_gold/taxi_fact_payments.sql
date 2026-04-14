{{ config(materialized='incremental', uninque_key=['payment_id','trip_id']) }}

{%
    set cols = 
[
'payment_id',
'trip_id',
'customer_id',
'payment_method',
'payment_status',
'payment_status_final',
'amount',
'transaction_time',
'last_updated_timestamp',
'modified_at'

]
%}


select 
    {% for col in cols %}
        {{ col }} 
        {% if not loop.last %},{% endif %}
    {% endfor %}
from 
    {{ source("silver_layer","taxi_silver_payments") }}
where 
    modified_at >= current_date() - interval '7 days' -- limit les full refresh involentaire

{% if is_incremental() %}
    AND modified_at > (select max(modified_at) from {{ this }})
{% endif %}