from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_establishments_by_cnae(estabelecimentos_df: DataFrame, cnaes_validos_df: DataFrame) -> DataFrame:
    valid_cnae_df = cnaes_validos_df.select(F.col("cnae_subclasse").alias("cnae_valido"))

    normalized = (
        estabelecimentos_df.withColumn("cnae_principal_limpo", F.regexp_replace(F.col("cnae_fiscal_principal"), r"[^0-9]", ""))
        .withColumn(
            "cnaes_secundarios",
            F.expr(
                "filter(transform(split(coalesce(cnae_fiscal_secundaria, ''), ','), x -> regexp_replace(trim(x), '[^0-9]', '')), x -> x <> '')"
            ),
        )
        .repartition("cnpj_basico")
    )

    principal_matches = (
        normalized.join(F.broadcast(valid_cnae_df), normalized.cnae_principal_limpo == valid_cnae_df.cnae_valido, "inner")
        .select("cnpj_basico", "cnpj", F.col("cnae_principal_limpo").alias("cnae_relevante"))
        .dropDuplicates()
    )

    secondary_matches = (
        normalized.withColumn("cnae_secundario", F.explode_outer(F.col("cnaes_secundarios")))
        .join(F.broadcast(valid_cnae_df), F.col("cnae_secundario") == valid_cnae_df.cnae_valido, "inner")
        .select("cnpj_basico", "cnpj", F.col("cnae_secundario").alias("cnae_relevante"))
        .dropDuplicates()
    )

    return (
        principal_matches.unionByName(secondary_matches)
        .groupBy("cnpj_basico", "cnpj")
        .agg(F.array_sort(F.collect_set("cnae_relevante")).alias("cnaes_relevantes"))
    )
