WITH features AS (
SELECT
    jsonb_array_elements(features) as feature
FROM {{source()}}
)
SELECT
  feature->'properties'->>'Description' AS description,
  feature->'properties'->>'Registration' AS registration,
  feature->'geometry'->>'type' AS geometry_type,
  feature->'geometry'->'coordinates' AS coordinates
FROM
  features;