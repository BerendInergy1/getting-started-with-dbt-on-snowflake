{% snapshot vendtable_snapshot %}

{{
    config(
      target_schema='staging',
      unique_key='recid',
      strategy='timestamp',
      updated_at='modifiedon',
      invalidate_hard_deletes=True,
      tags=['procurement', 'vendor_master']
    )
}}

select * from {{ source('dynamics_365_fo', 'VendTable') }}

{% endsnapshot %}