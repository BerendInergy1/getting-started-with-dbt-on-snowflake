{% snapshot stg_dyn_generaljournal_entry_snapshot %}

{{
    config(
      target_schema='staging',
      unique_key='recid',
      strategy='timestamp',
      updated_at='modifiedon',
      invalidate_hard_deletes=True,
      tags=['finance', 'general_ledger']
    )
}}

select * from {{ source('dynamics_365_fo', 'GeneralJournalEntry') }}

{% endsnapshot %}