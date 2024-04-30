with schedule as (
	SELECT
	"data" as date_execution
	, linha
	, carro
	, re
	, nome
	FROM {{source('hp', 'raw_escalas_programadas')}} AS rep

)
select
	date_execution
	, linha
	, carro
	, re
	, nome
	from schedule