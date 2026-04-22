from __future__ import annotations

import unicodedata
from typing import Iterable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ELIGIBLE_MARKERS = {"sim", "passivel de obrigacao"}


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).strip().lower()


def _to_digits(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _filter_eligible_rows(rows: Iterable[dict]) -> list[dict]:
    filtered = []
    for row in rows:
        marker = _normalize_text(row["indicador_obrigacao"])
        cnae = _to_digits(row["cnae_subclasse"])
        if marker in ELIGIBLE_MARKERS and cnae:
            filtered.append(
                {
                    "cnae_subclasse": cnae,
                    "descricao_cnae": str(row["descricao_cnae"] or "").strip(),
                    "indicador_obrigacao": str(row["indicador_obrigacao"] or "").strip(),
                }
            )
    return filtered


def load_eligible_cnaes(spark: SparkSession, excel_path: str) -> DataFrame:
    xlsx = pd.read_excel(excel_path, dtype=str)
    if xlsx.shape[1] < 3:
        raise ValueError("The Excel file must have at least 3 columns: CNAE, description, and indicator.")

    normalized = xlsx.iloc[:, :3].copy()
    normalized.columns = ["cnae_subclasse", "descricao_cnae", "indicador_obrigacao"]
    eligible_rows = _filter_eligible_rows(normalized.to_dict(orient="records"))

    if not eligible_rows:
        raise ValueError("No eligible CNAE found in Excel (expected values: Sim or Passível de obrigação).")

    cnaes_df = spark.createDataFrame(eligible_rows)
    return (
        cnaes_df.withColumn("cnae_subclasse", F.regexp_replace(F.col("cnae_subclasse"), r"[^0-9]", ""))
        .filter(F.col("cnae_subclasse") != "")
        .dropDuplicates(["cnae_subclasse"])
    )
