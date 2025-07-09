-- macros/apply_column_tags.sql

{% macro apply_column_tags(model_name) %}
  {% set model_key = "model." ~ project_name ~ "." ~ model_name %}
  {% set model = graph.nodes[model_key] %}

  {% set database = model.database %}
  {% set schema = model.schema %}
  {% set name = model.name %}

  {% for column_name, column_meta in model.columns.items() %}
    {% set tags = column_meta.get('meta', {}).get('tags', []) %}
    {% for tag in tags %}
      ALTER TABLE {{ database }}.{{ schema }}.{{ name }}
      MODIFY COLUMN {{ column_name }} SET TAG {{ tag }} = 'TRUE';
    {% endfor %}
  {% endfor %}
{% endmacro %}
