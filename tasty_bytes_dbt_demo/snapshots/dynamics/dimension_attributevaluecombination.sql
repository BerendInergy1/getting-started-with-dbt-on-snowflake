{% snapshot dimension_attributevaluecombination_snapshot %}

{{
    config(
      target_schema='staging',
      unique_key='recid',
      strategy='timestamp',
      updated_at='modifiedon',
      invalidate_hard_deletes=True,
      tags=['finance', 'dimensions']
    )
}}

select * from {{ source('dynamics_365_fo', 'DimensionAttributeValueCombination') }}

{% endsnapshot %}