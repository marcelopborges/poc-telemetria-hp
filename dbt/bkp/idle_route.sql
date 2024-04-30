-- SELECT
--     rt.id,
--     rt.linha_id,
--     rt.direcao,
--     rt.pontos_geolocalizacao AS idle_route,
--     rl.id AS linha_id_rl,
--     rl.num_linha,
--     rl.descricao
-- FROM {{ source('hp', 'raw_trajetos') }} AS rt
-- JOIN {{ source('hp', 'raw_linhas') }} AS rl
-- ON rt.linha_id = rl.id




