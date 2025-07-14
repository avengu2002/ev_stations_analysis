-- macros/set_custom_query_tag.sql

{% macro set_custom_query_tag() %}
  {% set tag = "model=" ~ this.name ~ ", env=" ~ target.name %}
  {{ return("SET QUERY_TAG = '" ~ tag ~ "'") }}
{% endmacro %}
