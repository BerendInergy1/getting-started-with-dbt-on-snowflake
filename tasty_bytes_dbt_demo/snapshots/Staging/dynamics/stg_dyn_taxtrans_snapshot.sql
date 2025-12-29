{% snapshot stg_dyn_taxtrans_snapshot %}

{{
    config(
      target_schema='staging',
      unique_key='recid',
      strategy='timestamp',
      updated_at='modifiedon',
      invalidate_hard_deletes=True,
      tags=['tax', 'transactions']
    )
}}

select * from {{ source('dynamics_365_fo', 'Taxtrans') }}

{% endsnapshot %}