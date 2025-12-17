{% snapshot tst_snapshot_deletes %}

{{
    config(
      target_schema='dev',
      unique_key='ak_key',
      updated_at='aa_attribute',
      strategy='timestamp'
    )
}}

select top 1 
    1 as ak_key,
    current_timestamp() as aa_attribute
WHERE current_timestamp() NOT BETWEEN '2025-12-16 05:38:15.584' AND '2025-12-16 07:38:15.584'
UNION ALL 
select top 1 
    2 as ak_key,
    current_timestamp() as aa_attribute
WHERE current_timestamp() NOT BETWEEN '2025-12-16 05:38:15.584' AND '2025-12-16 07:38:15.584'
{% endsnapshot %}