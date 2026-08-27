with __dbt__cte__eph_stores as (
select 
    distinct
    store_id,
    store_name,
    store_city,
    store_province,
    store_country,
    store_created_timestamp,
    store_updated_timestamp,
    store_is_active,
    store_processed_at,
    current_timestamp() AS store_gold_processed_at

FROM 
    `walmart`.`silver_b`.`obt_b`
) select * from __dbt__cte__eph_stores