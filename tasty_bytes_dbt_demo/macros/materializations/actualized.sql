{% materialization truncated, adapter='snowflake' %}
    
    {%- set identifier = model['alias'] -%}
    {%- set target_relation = api.Relation.create(
        database=database,
        schema=schema,
        identifier=identifier,
        type='table'
    ) -%}
    
    {%- set existing_relation = load_cached_relation(target_relation) -%}
    {%- set temp_relation = make_temp_relation(target_relation) -%}
    
    {{ run_hooks(pre_hooks) }}
    
    -- Build temp table with technical columns
    {% call statement('main') %}
        create or replace temporary table {{ temp_relation }} as
        select 
            *,
            current_timestamp() as {{ var('technical_columns.insert_datetime') }},
            current_timestamp() as {{ var('technical_columns.update_datetime') }},
            true as {{ var('technical_columns.actualstatus_indicator') }},
            false as {{ var('technical_columns.deleted_indicator') }},
            true as {{ var('technical_columns.lastdata_indicator') }},
            {{ calculate_hash(adapter.get_columns_in_relation(temp_relation), config.get('unique_key', [])) }},
            object_construct() as {{ var('technical_columns.metadata') }}
        from (
            {{ sql }}
        )
    {% endcall %}
    
    -- Create or replace target table
    {% call statement('create_table') %}
        create or replace table {{ target_relation }} as
        select * from {{ temp_relation }}
    {% endcall %}
    
    {{ run_hooks(post_hooks) }}
    
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}