with made_trip as (
	SELECT
	id
	, registration
	, description
	, geometry_type
	, coordinates
	, execution_date
	, hash
	FROM {{source('hp','raw_geodata')}}
)
select
registration
,description as carro
, coordinates as made_trip_coordinates
,execution_date
from made_trip