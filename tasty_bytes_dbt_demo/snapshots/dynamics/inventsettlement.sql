{% snapshot inventsettlement_snapshot %}

{{
    config(
      target_schema='staging',
      unique_key='recid',
      strategy='timestamp',
      updated_at='modifiedon',
      invalidate_hard_deletes=True,
      tags=['inventory', 'settlements']
    )
}}

select * from {{ source('dynamics_365_fo', 'InventSettlement') }}

{% endsnapshot %}