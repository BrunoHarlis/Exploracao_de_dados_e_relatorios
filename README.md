# Exploração de Dados e Relatórios

## Descrição
Nesse projeto estaremos analisando um conjunto de dados sintéticos para uma empresa fictícia que possui várias fábricas de alta tecnologia em todo o mundo. Recentemente foi notado que houve uma queda na receita no ano atual. Recebemos a tarefa de pesquisar os dados para tentar encontrar quaisquer potenciais causas que possam explicar o porquê disso.
 
## Iniciando

Vamos criar nosso DataBase fictício e fazer a incerção dos dados. Começamos importando bibliotecas e definindo Spark Session.
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Ingestao").getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
```

Criando os Data Frames
```
func_dados = spark.read.csv("hdfs:///tmp/data/exploracaodados/funcionario_dados.csv", header=True, inferSchema=True)
func_doente = spark.read.csv("hdfs:///tmp/data/exploracaodados/funcionario_doente.csv", header=True, inferSchema=True)
func_ferias = spark.read.csv("hdfs:///tmp/data/exploracaodados/funcionario_ferias.csv", header=True, inferSchema=True)
func_logs = spark.read.csv("hdfs:///tmp/data/exploracaodados/funcionario_logs.csv", header=True, inferSchema=True)
maq_producao = spark.read.csv("hdfs:///tmp/data/exploracaodados/maquina_producao.csv", header=True, inferSchema=True)
maq_tempo_ativa = spark.read.csv("hdfs:///tmp/data/exploracaodados/maquina_tempo_ativa.csv", header=True, inferSchema=True)
fabrica_receita = spark.read.csv("hdfs:///tmp/data/exploracaodados/fabrica_receita.csv", header=True, inferSchema=True)
fabrica_ambiente = spark.read.csv("hdfs:///tmp/data/exploracaodados/fabrica_ambiente.csv", header=True, inferSchema=True)
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

```
spark.sql("DROP DATABASE IF EXISTS rh CASCADE")
spark.sql("DROP DATABASE IF EXISTS fabrica CASCADE")

spark.sql("CREATE DATABASE rh")
spark.sql("CREATE DATABASE fabrica")
```

## Criando tabelas e inserindo dados.

Não vamos fazer nenhum filtro na crianção da tabela "funcionario".
```
func_dados.write.mode("overwrite").saveAsTable("rh.funcionario", format="parquet")
```

Entretanto, para criar a tabela "fabrica", vamos usar o DataFrame "func_dados" e pegar apenas a coluna "fabria_id". A tabela conterá somente isso.
```
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
```
dias_doente = func_doente.filter(col("doente") == 1)
dias_doente = dias_doente.withColumn("tipo_licenca", lit("doente"))
dias_ferias = func_ferias.filter(col("ferias") == 1)
dias_ferias = dias_ferias.withColumn("tipo_licenca", lit("ferias"))
tempo_licenca = dias_doente.union(dias_ferias)
tempo_licenca = tempo_licenca.select("fabrica_id", "funcionario_id", "data", "tipo_licenca")
tempo_licenca.write.mode("overwrite").saveAsTable("rh.tempo_licenca", format="parquet")
```

Criar a tabela "tempo_trabalho" a partir do dataframe "tempo_trabalho" renomeando a coluna "data" para "dia_trabalhado".
```
tempo_trabalhado = func_logs.withColumnRenamed("data", "dia_trabalhado")
tempo_trabalhado.write.mode("overwrite").saveAsTable("tempo_trabalhado", format="parquet")
```

Criar a tabela "maq_producao", "maq_temp_ativa", "maq_receita" e "dados_ambiente". 
```
maq_producao.write.mode("overwrite").saveAsTable("fabrica.maq_producao", format="parquet")
maq_tempo_atica.write.mode("overwrite").saveAsTable("fabrica.maq_temp_ativa", format="parquet")
fabrica_receita.write.mode("overwrite").saveAsTable("fabrica.maq_receita", format="parquet")
fabrica_ambiente.write.mode("overwrite")saveAsTable("fabrica.dados_ambiente", format="parquet")
```

Com isso, a ingestão dos dados está completa e o datawarehouse está pronto.

Agora vamos fazer algumas querys para descobrir o motivo da queda de receita na empresa. Vamos começar dando uma olhada nas tabelas que estão no database "fabrica" e na descrição da tabela "maq_producao".

![fabrica_desc_maq_producao](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/Imagens/DB%20fabrica%20DESC%20maq_producao.png)


Podemos medir a produção de cada fábrica e ver se tem alguma coisa anormal e criar uma tabela temporária com resultado para consultas posteriores.
```
media_producao = spark.sql(" \
SELECT fabrica_id, maquina_id, ROUND(AVG(unidades_por_dia),0) AS media_unidade_produzida \
FROM maq_producao \
GROUP BY fabrica_id, maquina_id \
ORDER BY fabrica_id, maquina_id ASC")

media_producao.createOrReplaceTempView("media_Producao")
```

Com a tabela "media_producao" pronta, podemos fazer uma query para descobrir qual a média de receita de cada fábrica.
```
receita = spark.sql(" \
SELECT fabrica_id, SUM(receita) AS total_receita \
FROM (SELECT fabrica_id, (media_unidade_produzida * receita_por_unidade) AS receita \
       FROM media_producao mp JOIN maq_receita mr \
       ON mp.maquina_id = mr.maquina_id) \
GROUP BY fabrica_id \
ORDER BY fabrica_id ASC")
```

Muito bem, abaixo temos a imagem que mostra a receita de cada fábrica e descobrimos que tem algo errado com a fábrica 2. Muito provável que ela seja a culpada da queda de faturamento do ano.

![RECEITA](https://github.com/BrunoHarlis/Exploracao_de_dados_e_relatorios/blob/main/Imagens/receita.png)


Vamos olhar com mais cuidado o que aconteceu com essa fábrica.
```
fabrica_2 = spark.sql(" \
SELECT maquina_id, ROUND(AVG(horas_operando),0) AS media_horas_operando \
FROM maq_temp_ativa \
WHERE fabrica_id = 2 \
GROUP BY maquina_id \
ORDER BY maquina_id ASC")


