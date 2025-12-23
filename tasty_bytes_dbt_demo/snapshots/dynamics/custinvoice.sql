{% snapshot custinvoice_snapshot %}

{{
    config(
      target_schema='staging',
      unique_key='recid',
      strategy='timestamp',
      updated_at='modifiedon',
      invalidate_hard_deletes=True,
      tags=['sales', 'invoices']
    )
}}

select * from {{ source('dynamics_365_fo', 'CustInvoice') }}

{% endsnapshot %}