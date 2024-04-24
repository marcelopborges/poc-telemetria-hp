{{ config(materialized='view') }}
with gld_idle_route as (
  SELECT
    sil.data,
    sil.carro,
    sil.first_travel,
    sil.last_travel,
    gil_ida.pontos_geolocalizacao AS geolocalizacao_ida,
    gil_volta.pontos_geolocalizacao AS geolocalizacao_volta
  FROM {{ ref('slv_identity_idle_line') }} AS sil
  LEFT JOIN {{ ref('slv_geo_linhas') }} AS gil_ida
    ON sil.first_travel = gil_ida.num_linha AND gil_ida.direcao = 'ida'
  LEFT JOIN {{ ref('slv_geo_linhas') }} AS gil_volta
    ON sil.last_travel = gil_volta.num_linha AND gil_volta.direcao = 'volta'
)
SELECT * FROM gld_idle_route
