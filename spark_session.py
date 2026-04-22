from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import SparkSession

BROADCAST_TIMEOUT_SECONDS = 900


def build_spark_session(app_name: str, configs: Optional[Dict[str, str]] = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.broadcastTimeout", str(BROADCAST_TIMEOUT_SECONDS))
    return spark
