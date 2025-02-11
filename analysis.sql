with _base as (
    select * from 
    my_table
), base as (
    SELECT
        "order id" as order_id,
        string_agg(distinct "sku") as sku,
        string_agg(distinct "type") as types,
        max("quantity") as quantity,
        sum(cast(replace("total",',','') as DOUBLE)) as sum_total,

    FROM _base
    WHERE "order id" is not null
    GROUP BY "order id"
    order by "order id"
), base2 as (
    select
        order_id,
        sku,
        types,
        quantity,
        sum_total,
        case when types like '%Refund%' then 0 else quantity end as real_quantity, 
    from base
)
select
    base2.sku,
    sum(sum_total) as total_paid_by_amazon,
    sum(real_quantity) as orders,
    sum(quantity)-sum(real_quantity) as returns,
    CASE 
        WHEN SUM(real_quantity) = 0 THEN 0  -- Handle division by zero
        ELSE SUM(sum_total) / SUM(real_quantity)
    END AS average_per_pc,
    name_sku.PP

from base2
left join name_sku
on base2.sku = name_sku.sku
group by base2.sku, name_sku.PP