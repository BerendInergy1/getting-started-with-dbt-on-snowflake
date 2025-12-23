{{
    config(
        materialized='incremental',
        unique_key='inventory_trans_recid',
        on_schema_change='sync_all_columns',
        cluster_by=['transaction_date', 'item_id', 'warehouse'],
        tags=['inventory', 'transactions', 'fact']
    )
}}

with inventory_trans as (
    select * from {{ ref('inventtrans_snapshot') }}
    where dbt_valid_to is null
    {% if is_incremental() %}
        and modifiedon >= (select max(last_modified_at) from {{ this }})
    {% endif %}
),

inventory_origin as (
    select * from {{ ref('inventtrans_origin_snapshot') }}
    where dbt_valid_to is null
),

settlements as (
    select
        transrecidissue,
        transrecidreceipt,
        qty as settled_qty,
        settledamount as settled_amount
    from {{ ref('inventsettlement_snapshot') }}
    where dbt_valid_to is null
),

-- Aggregate settlements for issues
issue_settlements as (
    select
        transrecidissue,
        sum(settled_qty) as total_settled_qty,
        sum(settled_amount) as total_settled_amount,
        count(*) as settlement_count
    from settlements
    group by transrecidissue
),

-- Aggregate settlements for receipts
receipt_settlements as (
    select
        transrecidreceipt,
        sum(settled_qty) as total_settled_qty,
        sum(settled_amount) as total_settled_amount,
        count(*) as settlement_count
    from settlements
    group by transrecidreceipt
),

final as (
    select
        -- Primary Key
        it.recid as inventory_trans_recid,
        
        -- Transaction Information
        it.inventtransid as inventory_transaction_id,
        it.datefinancial as transaction_date,
        date_trunc('month', it.datefinancial) as transaction_month,
        date_trunc('year', it.datefinancial) as transaction_year,
        it.datephysical as physical_date,
        
        -- Item Information
        it.itemid as item_id,
        it.qty as quantity,
        
        -- Transaction Type and Status
        it.statusissue as issue_status,
        it.statusreceipt as receipt_status,
        
        case
            when it.qty < 0 then 'Issue'
            when it.qty > 0 then 'Receipt'
            else 'Zero Transaction'
        end as transaction_type,
        
        -- Location Information
        it.inventlocationid as warehouse,
        it.wmslocationid as warehouse_location,
        
        -- Financial Information
        it.costamountposted as cost_amount_posted,
        it.costamountadjustment as cost_adjustment_amount,
        it.costamountposted + coalesce(it.costamountadjustment, 0) as total_cost_amount,
        
        -- Unit Cost Calculations
        case 
            when it.qty != 0 then (it.costamountposted + coalesce(it.costamountadjustment, 0)) / abs(it.qty)
            else 0 
        end as unit_cost,
        
        -- Settlement Information (for issues)
        coalesce(iss.total_settled_qty, 0) as issue_settled_qty,
        coalesce(iss.total_settled_amount, 0) as issue_settled_amount,
        coalesce(iss.settlement_count, 0) as issue_settlement_count,
        
        -- Settlement Information (for receipts)
        coalesce(rs.total_settled_qty, 0) as receipt_settled_qty,
        coalesce(rs.total_settled_amount, 0) as receipt_settled_amount,
        coalesce(rs.settlement_count, 0) as receipt_settlement_count,
        
        -- Open Quantity (not yet settled)
        case
            when it.qty < 0 then it.qty + coalesce(iss.total_settled_qty, 0)
            when it.qty > 0 then it.qty - coalesce(rs.total_settled_qty, 0)
            else 0
        end as open_quantity,
        
        -- Origin/Source Document Information
        io.referencecategory as source_document_category,
        io.referenceid as source_document_id,
        it.transrefid as transaction_reference_id,
        
        -- Metadata
        it.modifiedon as last_modified_at,
        current_timestamp() as dbt_loaded_at
        
    from inventory_trans it
    left join inventory_origin io
        on it.inventtransorigin = io.recid
    left join issue_settlements iss
        on it.recid = iss.transrecidissue
    left join receipt_settlements rs
        on it.recid = rs.transrecidreceipt
    
    where it.datefinancial is not null  -- Only financially posted transactions
)

select * from final