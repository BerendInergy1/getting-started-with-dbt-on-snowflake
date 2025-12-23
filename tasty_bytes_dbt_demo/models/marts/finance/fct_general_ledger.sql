{{
    config(
        materialized='incremental',
        unique_key='account_entry_recid',
        on_schema_change='sync_all_columns',
        cluster_by=['accounting_date', 'main_account'],
        tags=['finance', 'general_ledger', 'fact']
    )
}}

with journal_entries as (
    select * from {{ ref('generaljournal_entry_snapshot') }}
    where dbt_valid_to is null  -- Current records only
    {% if is_incremental() %}
        and modifiedon >= (select max(last_modified_at) from {{ this }})
    {% endif %}
),

account_entries as (
    select * from {{ ref('generaljournal_accountentry_snapshot') }}
    where dbt_valid_to is null
    {% if is_incremental() %}
        and modifiedon >= (select max(last_modified_at) from {{ this }})
    {% endif %}
),

dimension_combinations as (
    select * from {{ ref('dimension_attributevaluecombination_snapshot') }}
    where dbt_valid_to is null
),

main_accounts as (
    select 
        recid,
        mainaccountid,
        name as main_account_name,
        type as account_type,
        mainaccountcategory as account_category
    from {{ source('dynamics_365_fo', 'MainAccount') }}
),

subledger_vouchers as (
    select 
        generaljournalentry,
        subledgervoucher,
        subledgervoucherdataareaid as company_id
    from {{ source('dynamics_365_fo', 'SubledgerVoucherGeneralJournalEntry') }}
),

ledger_info as (
    select
        recid as ledger_recid,
        name as ledger_name,
        accountingcurrency as accounting_currency
    from {{ source('dynamics_365_fo', 'Ledger') }}
),

final as (
    select
        -- Primary Key
        ae.recid as account_entry_recid,
        
        -- Journal Entry Information
        je.recid as journal_entry_recid,
        je.journalnumber as journal_number,
        je.accountingdate as accounting_date,
        date_trunc('month', je.accountingdate) as accounting_month,
        date_trunc('year', je.accountingdate) as accounting_year,
        je.postinglayer as posting_layer,
        
        -- Subledger Information
        sv.subledgervoucher as subledger_voucher,
        sv.company_id,
        
        -- Account Dimension Information
        ae.ledgerdimension as ledger_dimension_recid,
        dc.displayvalue as account_combination,
        dc.mainaccount as main_account_recid,
        ma.mainaccountid as main_account,
        ma.main_account_name,
        ma.account_type,
        ma.account_category,
        
        -- Transaction Details
        ae.text as transaction_description,
        ae.postingtype as posting_type,
        
        -- Amounts
        ae.accountingcurrencyamount as accounting_amount,
        ae.transactioncurrencyamount as transaction_amount,
        ae.transactioncurrencycode as transaction_currency,
        
        -- Debit/Credit Split
        case 
            when ae.accountingcurrencyamount >= 0 then ae.accountingcurrencyamount 
            else 0 
        end as debit_amount,
        case 
            when ae.accountingcurrencyamount < 0 then abs(ae.accountingcurrencyamount) 
            else 0 
        end as credit_amount,
        
        -- Ledger Information
        li.ledger_name,
        li.accounting_currency,
        
        -- Metadata
        ae.modifiedon as last_modified_at,
        current_timestamp() as dbt_loaded_at
        
    from account_entries ae
    inner join journal_entries je
        on ae.generaljournalentry = je.recid
    left join dimension_combinations dc
        on ae.ledgerdimension = dc.recid
    left join main_accounts ma
        on dc.mainaccount = ma.recid
    left join subledger_vouchers sv
        on je.recid = sv.generaljournalentry
    left join ledger_info li
        on je.ledger = li.ledger_recid
)

select * from final