{#
-- Returns true if the target is in the targets defined by the variable `full_dataset_target_names` in `dbt_project.yml`
-- This value can be overridden by passing --vars "{use_full_dataset: true}" to dbt.
-- e.g. `dbt run --vars "{use_full_dataset: true}"`
#}

{% macro use_full_dataset() %}
    {{ return(var('use_full_dataset', target.name in var('full_dataset_target_names'))) }}
{% endmacro %}
