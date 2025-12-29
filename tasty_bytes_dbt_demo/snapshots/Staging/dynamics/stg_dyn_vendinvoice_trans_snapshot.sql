{% snapshot stg_dyn_vendinvoice_trans_snapshot %}

{{
    config(
      target_schema='staging',
      unique_key='recid',
      strategy='timestamp',
      updated_at='modifiedon',
      invalidate_hard_deletes=True,
      tags=['procurement', 'invoices']
    )
}}

select * from {{ source('dynamics_365_fo', 'Vendinvoicetrans') }}

{% endsnapshot %}