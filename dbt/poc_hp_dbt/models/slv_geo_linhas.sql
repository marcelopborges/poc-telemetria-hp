with slv_geo_linhas as (
	SELECT
	rt.id
	,rt.linha_id
	,rt.direcao
	,rt.pontos_geolocalizacao
	,rl.num_linha
	,rl.descricao
	FROM {{source('hp', 'raw_trajetos')}} AS rt
	JOIN {{source('hp', 'raw_linhas')}} AS rl on rl.id = rt.linha_id
)
select
	sgl.num_linha
	,sgl.descricao
	,sgl.direcao
	,sgl.pontos_geolocalizacao
	from slv_geo_linhas sgl



