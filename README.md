# Exploração de Dados e Relatórios

## Descrição
Nesse projeto estaremos analisando um conjunto de dados sintéticos para uma empresa fictícia que possui várias fábricas de alta tecnologia em todo o mundo. Recentemente foi notado que houve uma queda na receita no ano atual. Recebemos a tarefa de pesquisar os dados para tentar encontrar quaisquer potenciais causas que possam explicar o porquê disso.
 
## Iniciando

Vamos criar nosso DataBase fictício e fazer a incerção dos dados. Aqui está o [script em python para a inserção dos dados](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/scripts/insercao.py). Começamos importando bibliotecas e definindo Spark Session.
~~~python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Ingestao").getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
~~~

Criando os Data Frames
```python
func_dados = spark.read.csv("hdfs:///tmp/exploracaodados/funcionario_dados.csv", header=True, inferSchema=True)
func_doente = spark.read.csv("hdfs:///tmp/exploracaodados/funcionario_doente.csv", header=True, inferSchema=True)
func_ferias = spark.read.csv("hdfs:///tmp/exploracaodados/funcionario_ferias.csv", header=True, inferSchema=True)
func_logs = spark.read.csv("hdfs:///tmp/exploracaodados/funcionario_logs.csv", header=True, inferSchema=True)
maq_producao = spark.read.csv("hdfs:///tmp/exploracaodados/maquina_producao.csv", header=True, inferSchema=True)
maq_tempo_ativa = spark.read.csv("hdfs:///tmp/exploracaodados/maquina_tempo_ativa.csv", header=True, inferSchema=True)
fabrica_receita = spark.read.csv("hdfs:///tmp/exploracaodados/fabrica_receita.csv", header=True, inferSchema=True)
fabrica_ambiente = spark.read.csv("hdfs:///tmp/exploracaodados/fabrica_ambiente.csv", header=True, inferSchema=True)
```

Criando os databases rh e fabrica. Abaixo está descrito quais tabelas cada databases possuirá.
```
DATABASE: RH
TABELAS: FUNCIONARIO
         FABRICA
         TEMPO_LICENCA
         TEMPO_TRABALHO
         
DATABASE: FABRICA
TABELAS: MAQ_PRODUCAO
         MAQ_TEMPO_ATIVA
         MAQ_RECEITA
         DADOS_AMBIENTE
```

```python
spark.sql("DROP DATABASE IF EXISTS rh CASCADE")
spark.sql("DROP DATABASE IF EXISTS fabrica CASCADE")

spark.sql("CREATE DATABASE rh")
spark.sql("CREATE DATABASE fabrica")
```

## Criando tabelas e inserindo dados.

Não vamos fazer nenhum filtro na crianção da tabela "funcionario".
```python
func_dados.write.mode("overwrite").saveAsTable("rh.funcionario", format="parquet")
```

Entretanto, para criar a tabela "fabrica", vamos usar o DataFrame "func_dados" e pegar apenas a coluna "fabria_id". A tabela conterá somente isso.
```python
fabrica_id = func_dados.select("fabrica_id").distinct().sort("fabrica_id")
fabrica_id.write.mode("overwrite").saveAsTable("rh.fabrica", format="parquet")
```

A criação da tabela "tempo_licenca" será uma pouco mais complexa. Vamos aos passo:
1 - criar um dataframe (dias_doente) que contenha os funcionários que estiveram doentes. Usar o dataframe "func_doente".
2 - criar a coluna "tipo_licenca" com valores "doente".
3 - criar outro dataframe (dias_ferias) que contenha os funcionários que tiraram férias. Usar o dataframe "func_ferias".
4 - criar a coluna "tipo_licenca" com valores "ferias".
5 - fazer a união dos dataframes "dias_doente" e "dias_ferias".
6 - fazer um select somente com as colunas que temos interesse ("fabrica_id", "funcionario_id", "data", "tipo_licenca").
7 - finalmente criar a tabela no hive e inserir os dados nela.
```python
dias_doente = func_doente.filter(col("doente") == 1)
dias_doente = dias_doente.withColumn("tipo_licenca", lit("doente"))
dias_ferias = func_ferias.filter(col("ferias") == 1)
dias_ferias = dias_ferias.withColumn("tipo_licenca", lit("ferias"))
tempo_licenca = dias_doente.union(dias_ferias)
tempo_licenca = tempo_licenca.select("fabrica_id", "funcionario_id", "data", "tipo_licenca")
tempo_licenca.write.mode("overwrite").saveAsTable("rh.tempo_licenca", format="parquet")
```

Criar a tabela "tempo_trabalho" a partir do dataframe "tempo_trabalho" renomeando a coluna "data" para "dia_trabalhado".
```python
tempo_trabalhado = func_logs.withColumnRenamed("data", "dia_trabalhado")
tempo_trabalhado.write.mode("overwrite").saveAsTable("tempo_trabalhado", format="parquet")
```

Criar a tabela "maq_producao", "maq_temp_ativa", "maq_receita" e "dados_ambiente". 
```python
maq_producao.write.mode("overwrite").saveAsTable("fabrica.maq_producao", format="parquet")
maq_tempo_ativa.write.mode("overwrite").saveAsTable("fabrica.maq_temp_ativa", format="parquet")
fabrica_receita.write.mode("overwrite").saveAsTable("fabrica.maq_receita", format="parquet")
fabrica_ambiente.write.mode("overwrite").saveAsTable("fabrica.dados_ambiente", format="parquet")
```

Com isso, a ingestão dos dados está completa e o datawarehouse está pronto.

## Realizando Consultas

Agora vamos fazer algumas querys parra descobrir o motivo da queda de receita na empresa. Vamos começar dando uma olhada nas tabelas que estão no database "fabrica" e na descrição da tabela "maq_producao". [A query em SQL está aqui](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/scripts/query.sql).

![fabrica_desc_maq_producao](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/Imagens/DB%20fabrica%20DESC%20maq_producao.png)


Podemos medir a produção de cada fábrica e ver se tem alguma coisa anormal e criar uma tabela temporária com O resultado para consultas posteriores.
```python
media_producao = spark.sql(" \
SELECT fabrica_id, maquina_id, ROUND(AVG(unidades_por_dia),0) AS media_unidade_produzida \
FROM fabrica.maq_producao \
GROUP BY fabrica_id, maquina_id \
ORDER BY fabrica_id, maquina_id ASC")

media_producao.createOrReplaceTempView("media_Producao")
```

Com a tabela "media_producao" pronta, podemos fazer uma query para descobrir qual a média de receita de cada fábrica.
```python
receita = spark.sql(" \
SELECT fabrica_id, SUM(receita) AS total_receita \
FROM (SELECT fabrica_id, (media_unidade_produzida * receita_por_unidade) AS receita \
       FROM media_producao mp JOIN fabrica.maq_receita mr \
       ON mp.maquina_id = mr.maquina_id) \
GROUP BY fabrica_id \
ORDER BY fabrica_id ASC")

receita.createOrReplaceTempView("receita")
```

Muito bem, abaixo temos a imagem que mostra a receita de cada fábrica e descobrimos que tem algo errado com a fábrica 2. Muito provável que ela seja a culpada da queda de faturamento do ano.

![RECEITA](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/Imagens/receita.png)


Vamos olhar com mais cuidado o que aconteceu com essa fábrica.
```python
fabrica_2 = spark.sql(" \
SELECT maquina_id, ROUND(AVG(horas_operando),0) AS media_horas_operando \
FROM fabrica.maq_temp_ativa \
WHERE fabrica_id = 2 \
GROUP BY maquina_id \
ORDER BY maquina_id ASC")

fabrica_2.show()
```

![fabrica_2](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/Imagens/fabrica_2.png)


Vamos comparar com fabrica 1 para termos uma referência sobre a quantidade de horas trabalhadas.
```python
fabrica_1 = spark.sql(" \
SELECT maquina_id, ROUND(AVG(horas_operando),0) AS media_horas_operando \
FROM fabrica.maq_temp_ativa \
WHERE fabrica_id = 1 \
GROUP BY maquina_id \
ORDER BY maquina_id ASC")

fabrica_1.show()
```

![fabrica_1](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/Imagens/fabrica_2.png)


A diferenças de horas é mínima. Provavelmente o tempo de atividade das máquinas não deve ser o motivo principal para a queda de produção. 

Como temos os dados de RH, podemos ver se há alguma correlação nos dados dos funcionários que possa explicar a redução da receita na fábrica 2.
```python
horas_trabalhada = spark.sql(" \
SELECT fabrica_id, AVG(tempo_trabalhado) AS media_horas_trabalhadas \
FROM rh.tempo_trabalhado \
GROUP BY fabrica_id \
ORDER BY fabrica_id ASC")

horas_trabalhada.show()
```

![horas_trabalhada](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/Imagens/horas_trabalhada.png)


A fábrica 2 apresenta o menor número de horas trabalhadas. Vamos descobrir o porquê.
```python
tempo_licenca_funcionario = spark.sql(" \
SELECT fabrica_id, tipo_licenca, COUNT(tipo_licenca) / COUNT(DISTINCT funcionario_id) AS media_tempo \
FROM rh.tempo_licenca \
GROUP BY fabrica_id, tipo_licenca \
ORDER BY fabrica_id")

tempo_licenca_funcionario.show()
```

![tempo_licenca_funcionario](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/Imagens/tempo_funcionario_licenca.png)


Os funcionários da fábrica 2 têm um maior número de dias de baixa por doença. Parece que, ao contrário das outras fábricas, os funcionários da fábrica 2 possuem uma quantidade desproporcional de licença médica e menos férias. Como isso é exclusivo da fábrica 2, parece digno de ser relatado como uma potencial causa na queda da receita.
