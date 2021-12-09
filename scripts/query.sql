--------------------------------------------------
-- PRODUÇÃO MÉDIA DAS FÁBRICAS
--------------------------------------------------
CREATE TEMP VIEW fabrica.media_producao AS
SELECT fabrica_id, maquina_id, ROUND(AVG(unidades_por_dia),0) AS media_unidade_produzida
FROM fabrica.maq_producao
GROUP BY fabrica_id, maquina_id
ORDER BY fabrica_id, maquina_id ASC;
SELECT * FROM fabrica.media_producao;


--------------------------------------------------
-- RECEITA DAS FÁBRICAS
--------------------------------------------------
CREATE TEMP VIEW fabrica.receita AS
SELECT fabrica_id, SUM(receita) AS total_receita
FROM (SELECT fabrica_id, (media_unidade_produzida * receita_por_unidade) AS receita
       FROM media_producao mp JOIN fabrica.maq_receita mr
       ON mp.maquina_id = mr.maquina_id)
GROUP BY fabrica_id
ORDER BY fabrica_id ASC;
SELECT * FROM fabrica.receitas;


--------------------------------------------------
-- HORAS MAQUINAS OPERANDO FÁBRICAS 2
--------------------------------------------------
SELECT maquina_id, ROUND(AVG(horas_operando),0) AS media_horas_operando
FROM fabrica.maq_temp_ativa
WHERE fabrica_id = 2
GROUP BY maquina_id
ORDER BY maquina_id ASC;


--------------------------------------------------
-- HORAS MAQUINAS OPERANDO FÁBRICAS 1
--------------------------------------------------
SELECT maquina_id, ROUND(AVG(horas_operando),0) AS media_horas_operando
FROM fabrica.maq_temp_ativa
WHERE fabrica_id = 1
GROUP BY maquina_id
ORDER BY maquina_id ASC;


--------------------------------------------------
-- HORAS FUNIONARIOS TRABALHANDO CADA FÁBRICA
--------------------------------------------------
SELECT fabrica_id, AVG(tempo_trabalhado) AS media_horas_trabalhadas
FROM rh.tempo_trabalhado
GROUP BY fabrica_id
ORDER BY fabrica_id ASC;


--------------------------------------------------
-- LICENÇA FUNIONARIOS CADA FÁBRICA
--------------------------------------------------
DROP VIEW IF EXISTS rh.v_tempo_licenca_funcionario;

CREATE VIEW rh.v_tempo_licenca_funcionario AS
SELECT fabrica_id, tipo_licenca, COUNT(tipo_licenca) / COUNT(DISTINCT funcionario_id) AS media_tempo
FROM rh.tempo_licenca
GROUP BY fabrica_id, tipo_licenca
ORDER BY fabrica_id;

SELECT * FROM rh.v_tempo_licenca_funcionario;
