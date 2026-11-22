{% materialization warehouse_table, adapter='snowflake' %}
    {%- set target = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}
    {%- if target is none or not target.is_table -%}
        {{ exceptions.raise_compiler_error('Initialize the Snowflake warehouse table first: ' ~ this) }}
    {%- endif -%}

    {%- set columns = adapter.get_columns_in_relation(target) | map(attribute='quoted') | join(', ') -%}
    {% call statement('main', auto_begin=False) %}
        insert overwrite into {{ target }} ({{ columns }})
        select {{ columns }} from ({{ sql }}) as transformed
    {% endcall %}

    {{ return({'relations': [target]}) }}
{% endmaterialization %}
