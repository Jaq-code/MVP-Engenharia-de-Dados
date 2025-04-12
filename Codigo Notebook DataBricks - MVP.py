# Databricks notebook source
# MAGIC %md
# MAGIC **Análise de Faltas e Comportamento de Pacientes para Otimização do Agendamento em uma Clínica de Saúde**
# MAGIC
# MAGIC **Aluna:** Jaqueline Paciêlo Dantas 
# MAGIC
# MAGIC A clínica enfrenta um alto índice de faltas em consultas agendadas, impactando diretamente o faturamento. Este MVP tem como objetivo principal entender os padrões de comportamento dos pacientes e da agenda médica para reduzir as ausências, otimizar o agendamento e aumentar a receita.
# MAGIC Objetivos específicos:
# MAGIC ·  Identificar médicos com maior índice de retorno: reconhecer os profissionais com maior engajamento e confiança dos pacientes.
# MAGIC ·  Avaliar a base de contatos dos pacientes: quantificar quantos têm telefone preenchido, possibilitando ações de lembrete para reduzir faltas.
# MAGIC ·  Analisar a demanda por especialidades: identificar as especialidades mais procuradas e o volume de atendimentos nos últimos 3 meses, ajustando a oferta conforme a demanda.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd 
import io
import requests

# COMMAND ----------

# DBTITLE 1,Importação de especialidade
df_especialidade = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").option("delimiter", ";").option("encoding", "ISO-8859-1").csv("/FileStore/tables/Especialidade-6.csv") 
                 
df_especialidade.head()

# COMMAND ----------

# DBTITLE 1,Importação de Marcacao
df_marcacoes = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").option("delimiter", ";").option("encoding", "ISO-8859-1").csv("/FileStore/tables/Marcacoes-4.csv") 
                 
df_marcacoes.head()

# COMMAND ----------

# DBTITLE 1,Importação de Medicos
df_medico = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").option("delimiter", ";").option("encoding", "ISO-8859-1").csv("/FileStore/tables/Medicos-5.csv") 
                 
df_medico.head()

# COMMAND ----------

# DBTITLE 1,Importação de pacientes
df_paciente = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").option("delimiter", ";").option("encoding", "ISO-8859-1").csv("/FileStore/tables/Pacientes-7.csv") 
                 
df_paciente.head()



# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE bronze CASCADE

# COMMAND ----------

# DBTITLE 1,Criando a tabela "bronze"
# MAGIC %sql
# MAGIC CREATE DATABASE bronze;

# COMMAND ----------

# DBTITLE 1,Criação das tabelas partir do dataframe
import re

def limpar_nomes_colunas(df):
    colunas_limpas = [re.sub(r'[ ,;{}()\n\t=]', '_', col.strip().lower()) for col in df.columns]
    return df.toDF(*colunas_limpas)

#limpeza dos nomes das colunas, removendo o "_"
df_paciente = limpar_nomes_colunas(df_paciente) 
df_especialidade = limpar_nomes_colunas(df_especialidade) 
df_medico = limpar_nomes_colunas(df_medico) 
df_marcacoes = limpar_nomes_colunas(df_marcacoes) 


df_paciente.write.format("delta").mode("overwrite").saveAsTable("bronze.paciente")
df_especialidade.write.format("delta").mode("overwrite").saveAsTable("bronze.especialidade")
df_medico.write.format("delta").mode("overwrite").saveAsTable("bronze.medico")
df_marcacoes.write.format("delta").mode("overwrite").saveAsTable("bronze.marcacoes")

# COMMAND ----------

# DBTITLE 1,Consulta na tabela paciente
# MAGIC %sql
# MAGIC select * from bronze.paciente
# MAGIC limit 15

# COMMAND ----------

# DBTITLE 1,Data Quality
# MAGIC %sql
# MAGIC --consultando os medicos com CRM Válido no Rio de Janeiro e listando todas as suas especialidades
# MAGIC select idmedico, medico, crm, concat_ws(', ', collect_list(especialidade)) as especialidades
# MAGIC from bronze.medico as medico
# MAGIC join bronze.especialidade as especialidade
# MAGIC   on especialidade.idespecialidade = medico.idespecialidade
# MAGIC where CRM is not null
# MAGIC and especialidade not like '%<Indeterminada>%' 
# MAGIC and especialidade not like  '%padrão%'
# MAGIC group by medico.idmedico, medico.medico, medico.crm

# COMMAND ----------

# DBTITLE 1,Data Quality Pacientes
# MAGIC %sql select  idpessoa as IDPaciente, codpessoa as codpaciente, pessoa as Paciente, endereco, cep, pais, nascimento, sexo, estadocivil, identidade, cpf, telefones
# MAGIC from bronze.paciente
# MAGIC where nascimento is not NULL
# MAGIC and sexo is not null 
# MAGIC and identidade is not null 
# MAGIC and cpf is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE silver CASCADE

# COMMAND ----------

# DBTITLE 1,silver
# MAGIC %sql
# MAGIC CREATE DATABASE silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silver.paciente
# MAGIC as select  idpessoa as IDPaciente, codpessoa as codpaciente, pessoa as Paciente, endereco, cep, pais, nascimento, sexo, estadocivil, identidade, cpf, telefones
# MAGIC from bronze.paciente
# MAGIC where nascimento is not NULL
# MAGIC and sexo is not null 
# MAGIC and identidade is not null 
# MAGIC and cpf is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.paciente

# COMMAND ----------

# DBTITLE 1,Consultando aos medicos com maior número de agendamentos
# MAGIC %sql
# MAGIC select  medico, count (marcacoes.idmedico) AS total_marcacoes
# MAGIC from bronze.marcacoes as marcacoes
# MAGIC join bronze.medico as medico 
# MAGIC   on medico.idmedico = marcacoes.idmedico
# MAGIC join bronze.especialidade as especialidade 
# MAGIC   on especialidade.idespecialidade = marcacoes.idespecialidade
# MAGIC group by medico.idmedico, medico
# MAGIC order by total_marcacoes desc 
# MAGIC limit 20

# COMMAND ----------

# DBTITLE 1,Quantidade de pacientes com telefone
# MAGIC %sql
# MAGIC
# MAGIC select count (idpessoa) as Pac_ComTelefone
# MAGIC from bronze.paciente
# MAGIC where telefones is not null
# MAGIC

# COMMAND ----------

# DBTITLE 1,Quantidade de pacientes sem telefone
# MAGIC
# MAGIC %sql
# MAGIC --O tratamento dos dados foi feito no momento da extração
# MAGIC
# MAGIC select count (idpessoa) as Pac_SemTelefone
# MAGIC from bronze.paciente
# MAGIC where telefones is null

# COMMAND ----------

# DBTITLE 1,Demanda por especialidades
# MAGIC %sql
# MAGIC select especialidade, count(idatendimento) as Total_Especialidade
# MAGIC from bronze.marcacoes m
# MAGIC join bronze.especialidade e on e.idespecialidade = m.idespecialidade
# MAGIC group by m.idespecialidade, especialidade
# MAGIC order by Total_Especialidade desc

# COMMAND ----------

# DBTITLE 1,Total de marcações nos ultimos 3 meses
# MAGIC %sql
# MAGIC select count (m.idmarcacao) TotalMarcacoes
# MAGIC from bronze.marcacoes m
# MAGIC where datamarcada < '20250113'