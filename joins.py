from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _tipo_socio_col() -> F.Column:
    return (
        F.when(F.col("identificador_socio") == "1", F.lit("PJ"))
        .when(F.col("identificador_socio") == "2", F.lit("PF"))
        .otherwise(F.lit("OUTROS"))
    )


def build_final_dataset(empresas_df: DataFrame, empresas_filtradas_df: DataFrame, socios_df: DataFrame) -> DataFrame:
    base = (
        empresas_filtradas_df.join(empresas_df.select("cnpj_basico", "razao_social"), on="cnpj_basico", how="left")
        .join(socios_df, on="cnpj_basico", how="left")
        .withColumn("tipo_socio", _tipo_socio_col())
    )

    return base.select(
        F.col("cnpj").alias("cnpj"),
        F.col("razao_social").alias("razao_social"),
        F.concat_ws(",", F.col("cnaes_relevantes")).alias("cnaes_relevantes"),
        F.col("nome_socio_razao_social").alias("nome_socio"),
        F.col("tipo_socio").alias("tipo_socio"),
        F.col("qualificacao_socio").alias("qualificacao_socio"),
    )
