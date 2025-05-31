{% test test_date_creation_valide(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE
    TRY_CAST({{ column_name }} as DATE) IS NOT NULL
    AND TRY_CAST({{ column_name }} as DATE) > CURRENT_TIMESTAMP

{% endtest %}