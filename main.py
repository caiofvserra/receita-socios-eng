from __future__ import annotations

import argparse
import json

from cnaes_loader import load_eligible_cnaes
from cnpj_reader import read_empresas, read_estabelecimentos, read_socios
from filters import filter_establishments_by_cnae
from joins import build_final_dataset
from spark_session import build_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline Spark para CNPJ + CNAEs elegíveis")
    parser.add_argument("--excel-cnaes", required=True, help="Caminho do arquivo Excel (.xlsx) com CNAEs válidos")
    parser.add_argument("--empresas-path", required=True, help="Path (arquivo, pasta ou glob) dos CSVs de Empresas")
    parser.add_argument(
        "--estabelecimentos-path", required=True, help="Path (arquivo, pasta ou glob) dos CSVs de Estabelecimentos"
    )
    parser.add_argument("--socios-path", required=True, help="Path (arquivo, pasta ou glob) dos CSVs de Sócios")
    parser.add_argument("--output-path", required=True, help="Diretório de saída")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "csv"], help="Formato de saída")
    parser.add_argument("--app-name", default="receita-socios-eng", help="Nome da aplicação Spark")
    parser.add_argument("--output-partitions", type=int, default=200, help="Número de partições da saída")
    parser.add_argument(
        "--spark-config-json",
        default="{}",
        help='Configurações extras Spark em JSON, ex: {"spark.sql.shuffle.partitions":"400"}',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark_configs = json.loads(args.spark_config_json)
    spark = build_spark_session(args.app_name, spark_configs)

    cnaes_validos_df = load_eligible_cnaes(spark, args.excel_cnaes).cache()

    empresas_df = read_empresas(spark, args.empresas_path).repartition("cnpj_basico").cache()
    estabelecimentos_df = read_estabelecimentos(spark, args.estabelecimentos_path)
    socios_df = read_socios(spark, args.socios_path).repartition("cnpj_basico")

    empresas_filtradas_df = filter_establishments_by_cnae(estabelecimentos_df, cnaes_validos_df)

    final_df = (
        build_final_dataset(empresas_df, empresas_filtradas_df, socios_df)
        .repartition(args.output_partitions, "cnpj")
        .cache()
    )

    writer = final_df.write.mode("overwrite")
    if args.output_format == "csv":
        writer.option("header", "true").csv(args.output_path)
    else:
        writer.parquet(args.output_path)

    spark.stop()


if __name__ == "__main__":
    main()
