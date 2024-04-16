{{ config(materialized='view') }}

SELECT
    *
FROM
    {{ source('hp', 'raw_geodata') }}
