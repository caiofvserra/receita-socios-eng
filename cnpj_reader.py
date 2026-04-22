from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


EMPRESAS_SCHEMA = T.StructType(
    [
        T.StructField("cnpj_basico", T.StringType(), True),
        T.StructField("razao_social", T.StringType(), True),
        T.StructField("natureza_juridica", T.StringType(), True),
        T.StructField("qualificacao_responsavel", T.StringType(), True),
        T.StructField("capital_social", T.StringType(), True),
        T.StructField("porte_empresa", T.StringType(), True),
        T.StructField("ente_federativo_responsavel", T.StringType(), True),
    ]
)

ESTABELECIMENTOS_SCHEMA = T.StructType(
    [
        T.StructField("cnpj_basico", T.StringType(), True),
        T.StructField("cnpj_ordem", T.StringType(), True),
        T.StructField("cnpj_dv", T.StringType(), True),
        T.StructField("identificador_matriz_filial", T.StringType(), True),
        T.StructField("nome_fantasia", T.StringType(), True),
        T.StructField("situacao_cadastral", T.StringType(), True),
        T.StructField("data_situacao_cadastral", T.StringType(), True),
        T.StructField("motivo_situacao_cadastral", T.StringType(), True),
        T.StructField("nome_cidade_exterior", T.StringType(), True),
        T.StructField("pais", T.StringType(), True),
        T.StructField("data_inicio_atividade", T.StringType(), True),
        T.StructField("cnae_fiscal_principal", T.StringType(), True),
        T.StructField("cnae_fiscal_secundaria", T.StringType(), True),
        T.StructField("tipo_logradouro", T.StringType(), True),
        T.StructField("logradouro", T.StringType(), True),
        T.StructField("numero", T.StringType(), True),
        T.StructField("complemento", T.StringType(), True),
        T.StructField("bairro", T.StringType(), True),
        T.StructField("cep", T.StringType(), True),
        T.StructField("uf", T.StringType(), True),
        T.StructField("municipio", T.StringType(), True),
        T.StructField("ddd_1", T.StringType(), True),
        T.StructField("telefone_1", T.StringType(), True),
        T.StructField("ddd_2", T.StringType(), True),
        T.StructField("telefone_2", T.StringType(), True),
        T.StructField("ddd_fax", T.StringType(), True),
        T.StructField("fax", T.StringType(), True),
        T.StructField("correio_eletronico", T.StringType(), True),
        T.StructField("situacao_especial", T.StringType(), True),
        T.StructField("data_situacao_especial", T.StringType(), True),
    ]
)

SOCIOS_SCHEMA = T.StructType(
    [
        T.StructField("cnpj_basico", T.StringType(), True),
        T.StructField("identificador_socio", T.StringType(), True),
        T.StructField("nome_socio_razao_social", T.StringType(), True),
        T.StructField("cpf_cnpj_socio", T.StringType(), True),
        T.StructField("qualificacao_socio", T.StringType(), True),
        T.StructField("data_entrada_sociedade", T.StringType(), True),
        T.StructField("pais", T.StringType(), True),
        T.StructField("representante_legal", T.StringType(), True),
        T.StructField("nome_representante", T.StringType(), True),
        T.StructField("qualificacao_representante_legal", T.StringType(), True),
        T.StructField("faixa_etaria", T.StringType(), True),
    ]
)


def _read_official_csv(spark: SparkSession, path: str, schema: T.StructType) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "false")
        .option("sep", ";")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "ISO-8859-1")
        .schema(schema)
        .load(path)
    )


def read_empresas(spark: SparkSession, path: str) -> DataFrame:
    return _read_official_csv(spark, path, EMPRESAS_SCHEMA)


def read_estabelecimentos(spark: SparkSession, path: str) -> DataFrame:
    return _read_official_csv(spark, path, ESTABELECIMENTOS_SCHEMA).withColumn(
        "cnpj", F.concat_ws("", F.col("cnpj_basico"), F.col("cnpj_ordem"), F.col("cnpj_dv"))
    )


def read_socios(spark: SparkSession, path: str) -> DataFrame:
    return _read_official_csv(spark, path, SOCIOS_SCHEMA)
