{% test dbt_utils_placeholder_range(model, column_name, min, max, where=none) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min }} or {{ column_name }} > {{ max }})
  {% if where %} and ({{ where }}) {% endif %}
{% endtest %}
