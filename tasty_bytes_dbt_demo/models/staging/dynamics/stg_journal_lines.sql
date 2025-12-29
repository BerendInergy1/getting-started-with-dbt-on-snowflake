{{
    config(
        materialized='view',
        tags=['finance', 'journal']
    )
}}

/*
Deze transformatie is enkel aangemaakt als voorbeeld. dynamics bevat in principe alleen snapshots omdat de dynamics source altijd de huidige stand laat zien.
*/

with account_entries as (
    select * from {{ ref('stg_dyn_generaljournal_accountentry_snapshot') }}
    where dbt_valid_to is null
),

dimension_combinations as (
    select * from {{ ref('stg_dyn_dimension_attributevaluecombination_snapshot') }}
    where dbt_valid_to is null
),

select
    ae.recid as journal_line_id,
    ae.generaljournalentry as journal_header_id
    ae.ledgerdimension,
    dc.displayvalue as account_combination,
    dc.mainaccount,
    ae.accountingcurrencyamount,
    ae.transactioncurrencyamount,
    ae.transactioncurrencycode,
    ae.text as transaction_text,
    ae.postingtype,
    ae.dbt_valid_from as valid_from,
from account_entries ae
left join dimension_combinations dc
    on ae.ledgerdimension = dc.recid
