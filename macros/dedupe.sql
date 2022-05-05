{%- macro dedupe(model, key_fields, sort_fields=[], field_list=['*'], filter_list=[]) -%}
{#-
Example usage:
with deduped as (
    {{ dedupe(
        source('raw_packet_db', 'addresses'),
        key_fields=['id'],
        sort_fields=['updated_at desc']
    ) }}
)
....
-#}
    select {{field_list|join(', ')}}
    from (
        select {{field_list|join(', ')}},
            row_number() over (partition by {{ key_fields|join(', ') }}{% if sort_fields %} order by {{ sort_fields|join(', ') }}{% endif %}) as rn
        from {{ model }}
        {%- if filter_list -%}
            where {{ filter_list|join('\n and ') }}
        {% endif %}
    ) where rn = 1 -- dedupe
{%- endmacro -%}
