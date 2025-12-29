{{
    config(
        materialized='incremental',
        unique_key=['sales_invoice_id','sales_invoice_data_area_id'],
        on_schema_change='sync_all_columns',
        cluster_by=['invoice_date','customer_id'],
        tags=['sales', 'invoices', 'fact']
    )
}}

with standard_invoice_headers as (
    select * from {{ ref('stg_dyn_custinvoice_jour_snapshot') }}
    where dbt_valid_to is null
    {% if is_incremental() %}
        and modifiedon >= (select max(last_modified_at) from {{ this }})
    {% endif %}
),

free_text_invoice_headers as (
    select * from {{ ref('stg_dyn_custinvoice_snapshot') }}
    where dbt_valid_to is null
    {% if is_incremental() %}
        and modifiedon >= (select max(last_modified_at) from {{ this }})
    {% endif %}
),

-- Standard invoices from sales orders
standard_invoices as (
    select
        -- Primary Key
        ih.recid as sales_invoice_id,
        ih.dataareaid as sales_invoice_data_area_id,
        
        -- Invoice Information
        ih.invoiceid as invoice_id,
        ih.invoicedate as invoice_date,
        ih.duedate as due_date,
        datediff(day, ih.invoicedate, ih.duedate) as payment_terms_days,
        ih.salestype as sales_type,
        
        -- Amounts
        ih.currencycode as currency,
        ih.invoiceamountmst as invoice_amount_incl_vat,
        
        -- Customer Information
        ih.invoiceaccount as customer_id,
        
        -- Sales Order Information
        ih.salesid as sales_order_id,
        
        -- Metadata
        ih.modifiedon as last_modified_at,
        current_timestamp() as dbt_loaded_at
        
    from standard_invoice_headers ih
),

-- Free text invoices (no sales order)
free_text_invoices as (
    select
        -- Primary Key
        fti.recid as sales_invoice_id,
        fti.dataareaid as sales_invoice_data_area_id,
        
        -- Invoice Information
        fti.invoiceid as invoice_id,
        fti.invoicedate as invoice_date,
        fti.duedate as due_date,
        datediff(day, fti.invoicedate, fti.duedate) as payment_terms_days,
        null as sales_type,
        
        -- Amounts
        fti.currencycode as currency,
        fti.invoiceamountmst as invoice_amount_incl_vat,
        
        -- Customer Information
        fti.invoiceaccount as customer_id,
        
        -- Sales Order Information
        null as sales_order_id,
        
        -- Metadata
        fti.modifiedon as last_modified_at,
        current_timestamp() as dbt_loaded_at
        
    from free_text_invoice_headers fti
),

    select * from standard_invoices
    union all
    select * from free_text_invoices
