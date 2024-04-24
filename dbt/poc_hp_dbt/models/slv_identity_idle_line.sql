 WITH identity_idle_line AS (
  SELECT
    rep.data,
    rep.linha as num_linha,
    rep.carro,
    rep.re,
    rep.nome,
    COALESCE(rep.dthr_saida, rep.dthr_retorno) as dthr_saida_efetiva,
    COALESCE(rep.dthr_chegada, rep.dthr_retorno) as dthr_chegada_efetiva
  FROM hp.raw_escalas_programadas rep
),
ordening_schedule AS (
  SELECT
    z.data,
    z.carro,
    z.num_linha,
    FIRST_VALUE(z.num_linha) OVER (PARTITION BY z.data, z.carro ORDER BY z.dthr_saida_efetiva ASC) as first_travel,
    LAST_VALUE(z.num_linha) OVER (PARTITION BY z.data, z.carro ORDER BY z.dthr_saida_efetiva ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_travel
  FROM identity_idle_line z
)

SELECT DISTINCT
  x.data,
  x.carro,
  x.first_travel,
  x.last_travel
FROM ordening_schedule x