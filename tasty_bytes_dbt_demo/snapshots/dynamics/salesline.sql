{% snapshot salesline_snapshot %}

{{
    config(
      target_schema='staging',
      unique_key='recid',
      strategy='timestamp',
      updated_at='modifiedon',
      invalidate_hard_deletes=True,
      tags=['sales', 'orders']
    )
}}

select * from {{ source('dynamics_365_fo', 'SalesLine') }}

{% endsnapshot %}