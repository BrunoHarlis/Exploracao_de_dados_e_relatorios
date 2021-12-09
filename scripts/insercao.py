# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Ingestao").getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")



#*************************************************
# CRIAR DATAFAMES
#*************************************************
print("INICIANDO OS TRABALHOS\n\n")
print("CARREGANDO ARQUIVOS...")
func_dados = spark.read.csv("hdfs:///tmp/exploracaodados/funcionario_dados.csv", header=True, inferSchema=True)
func_doente = spark.read.csv("hdfs:///tmp/exploracaodados/funcionario_doente.csv", header=True, inferSchema=True)
func_ferias = spark.read.csv("hdfs:///tmp/exploracaodados/funcionario_ferias.csv", header=True, inferSchema=True)
func_logs = spark.read.csv("hdfs:///tmp/exploracaodados/funcionario_logs.csv", header=True, inferSchema=True)
maq_producao = spark.read.csv("hdfs:///tmp/exploracaodados/maquina_producao.csv", header=True, inferSchema=True)
maq_tempo_ativa = spark.read.csv("hdfs:///tmp/exploracaodados/maquina_tempo_ativa.csv", header=True, inferSchema=True)
fabrica_receita = spark.read.csv("hdfs:///tmp/exploracaodados/fabrica_receita.csv", header=True, inferSchema=True)
fabrica_ambiente = spark.read.csv("hdfs:///tmp/exploracaodados/fabrica_ambiente.csv", header=True, inferSchema=True)
print("ARQUIVOS PRONTOS\n\n")
 

#*************************************************
# DROP DATABASES
#*************************************************
print("CRIANDO DATABASE...")
spark.sql("DROP DATABASE IF EXISTS rh CASCADE")
spark.sql("DROP DATABASE IF EXISTS fabrica CASCADE")


#*************************************************
# CRIAR DATABASES
#*************************************************
spark.sql("CREATE DATABASE rh")
spark.sql("CREATE DATABASE fabrica")
print("DATABASE PRONTO\n\n")



#*************************************************
# CRIAR E INSERIR TABELA RH.FUNCIONARIO
#*************************************************
print("CRIANDO TABELAS...")
func_dados.write.mode("overwrite").saveAsTable("rh.funcionario", format="parquet")



#*************************************************
# CRIAR E INSERIR TABELA RH.FABRICA
#*************************************************
fabrica_id = func_dados.select("fabrica_id").distinct().sort("fabrica_id")
fabrica_id.write.mode("overwrite").saveAsTable("rh.fabrica", format="parquet")



#*************************************************
# CRIAR E INSERIR TABELA RH.TEMPO_LICENCA
#*************************************************
dias_doente = func_doente.filter(col("doente") == 1)
dias_doente = dias_doente.withColumn("tipo_licenca", lit("doente"))
dias_ferias = func_ferias.filter(col("ferias") == 1)
dias_ferias = dias_ferias.withColumn("tipo_licenca", lit("ferias"))
tempo_licenca = dias_doente.union(dias_ferias)
tempo_licenca = tempo_licenca.select("fabrica_id", "funcionario_id", "data", "tipo_licenca")
tempo_licenca.write.mode("overwrite").saveAsTable("rh.tempo_licenca", format="parquet")



#*************************************************
# CRIAR E INSERIR TABELA RH.TEMPO_TRABALHADO
#*************************************************
tempo_trabalhado = func_logs.withColumnRenamed("data", "dia_trabalhado")
tempo_trabalhado.write.mode("overwrite").saveAsTable("tempo_trabalhado", format="parquet")


#*************************************************
# CRIAR E INSERIR TABELAS: FABRICA.MAQ_PRODUCAO
#                          FABRICA.MAQ_TEMP_ATIVA
#                          FABRICA.MAQ_RECEITA
#                          FABRICA.DADOS_AMBIENTE
#*************************************************
maq_producao.write.mode("overwrite").saveAsTable("fabrica.maq_producao", format="parquet")
maq_tempo_ativa.write.mode("overwrite").saveAsTable("fabrica.maq_temp_ativa", format="parquet")
fabrica_receita.write.mode("overwrite").saveAsTable("fabrica.maq_receita", format="parquet")
fabrica_ambiente.write.mode("overwrite").saveAsTable("fabrica.dados_ambiente", format="parquet")
print("TABELAS PRONTAS\n\n")


print("\nFIM DO PROGRAMA\n")
spark.stop()
