# receita-socios-eng

Pipeline de engenharia de dados em **Apache Spark (PySpark)** para processar dados públicos do **CNPJ da Receita Federal** e gerar uma lista de empresas com CNAEs elegíveis (engenharia ou passível de engenharia), relacionando-as com seus sócios/administradores.

## Visão geral da solução

A solução usa como única fonte de verdade para elegibilidade de CNAE um arquivo Excel (`.xlsx`) fornecido pelo usuário:

- **Coluna 1**: CNAE (subclasse)
- **Coluna 2**: descrição do CNAE
- **Coluna 3**: indicador de obrigação (`Sim` ou `Passível de obrigação`)

Com base nesse Excel, o pipeline:

1. Carrega CNAEs elegíveis.
2. Lê os arquivos oficiais da base CNPJ (Empresas, Estabelecimentos e Sócios) com schema explícito.
3. Filtra estabelecimentos cujo CNAE principal ou secundário esteja no conjunto elegível.
4. Faz join distribuído com empresas e sócios.
5. Escreve dataset analítico em Parquet (ou CSV).

## Arquitetura do pipeline Spark

```text
Excel CNAEs (.xlsx)
        |
        v
[cnaes_loader.py] -> cnaes_validos_df (broadcast)

Empresas CSV  -> [cnpj_reader.py] -> empresas_df
Estabs CSV    -> [cnpj_reader.py] -> estabelecimentos_df -> [filters.py] -> empresas_filtradas_df
Sócios CSV    -> [cnpj_reader.py] -> socios_df

empresas_filtradas_df + empresas_df + socios_df
                |
                v
            [joins.py]
                |
                v
           dataset final
                |
                v
     Parquet/CSV (output-path)
```

## Estrutura de pastas

```text
.
├── README.md
├── spark_session.py
├── cnaes_loader.py
├── cnpj_reader.py
├── filters.py
├── joins.py
└── main.py
```

## Como fornecer o Excel de CNAEs

- Formato: `.xlsx`
- O pipeline usa as **3 primeiras colunas** da planilha.
- Valores aceitos para elegibilidade (coluna 3):
  - `Sim`
  - `Passível de obrigação`
- O processo normaliza acentuação e pontuação para comparação.

## Formato esperado dos dados CNPJ

Arquivos CSV oficiais da Receita Federal (layout público), separados por `;` e codificação `ISO-8859-1`:

- **Empresas**
- **Estabelecimentos**
- **Sócios**

> Recomendação: disponibilizar os CSVs em storage acessível ao cluster Spark (S3/HDFS/ADLS/GCS ou filesystem distribuído), preferencialmente já descompactados para melhor paralelismo.

## Execução

### 1) Ambiente local

Pré-requisitos:

- Python 3.10+
- Spark 3.x
- Dependências Python: `pyspark`, `pandas`, `openpyxl`

Exemplo:

```bash
python main.py \
  --excel-cnaes /data/cnaes_engenharia.xlsx \
  --empresas-path '/data/cnpj/Empresas*.csv' \
  --estabelecimentos-path '/data/cnpj/Estabelecimentos*.csv' \
  --socios-path '/data/cnpj/Socios*.csv' \
  --output-path /data/output/empresas_engenharia \
  --output-format parquet \
  --output-partitions 200 \
  --spark-config-json '{"spark.sql.shuffle.partitions":"400"}'
```

### 2) Execução em cluster (spark-submit)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  main.py \
  --excel-cnaes /mnt/input/cnaes_engenharia.xlsx \
  --empresas-path 's3://bucket/cnpj/Empresas*.csv' \
  --estabelecimentos-path 's3://bucket/cnpj/Estabelecimentos*.csv' \
  --socios-path 's3://bucket/cnpj/Socios*.csv' \
  --output-path s3://bucket/output/empresas_engenharia \
  --output-format parquet
```

## Decisões de performance e escalabilidade

- **DataFrames Spark** (sem RDD) para otimização com Catalyst.
- **Broadcast join** dos CNAEs elegíveis (dataset pequeno) para reduzir shuffle no filtro principal/secundário.
- **Repartition por `cnpj_basico`** antes de joins grandes para melhorar distribuição.
- **Persist/cache** em DataFrames intermediários críticos (`cnaes_validos_df`, `empresas_df`, resultados finais) para evitar recomputação.
- Escrita particionada por número de partições configurável (`--output-partitions`).

## Esquema de saída mínimo

O dataset final contém, no mínimo:

- `cnpj`
- `razao_social`
- `cnaes_relevantes`
- `nome_socio`
- `tipo_socio` (PF/PJ/OUTROS)
- `qualificacao_socio`

## Uso responsável dos dados públicos (LGPD)

Apesar de serem dados públicos oficiais, o uso deve obedecer princípios de finalidade, necessidade e segurança, em conformidade com a LGPD e normas aplicáveis. Evite exposição indevida de dados pessoais e implemente controles de acesso, retenção e auditoria adequados ao contexto do tratamento.
