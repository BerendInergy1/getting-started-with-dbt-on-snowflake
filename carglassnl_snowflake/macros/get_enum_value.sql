{% macro get_enum_value(enum_name, enum_value, language='en-us') %}
    (
        select enumvaluename
        from {{ source('dynamics_365_fo', 'srsanalysisenums') }}
        where enumname = '{{ enum_name }}'
          and enumvalue = {{ enum_value }}
          and lower(languageid) = '{{ language }}'
        limit 1
    )
{% endmacro %}