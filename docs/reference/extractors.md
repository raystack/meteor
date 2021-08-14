# Extractors

Metor currently support metadata extraction on these data sources.
To perform extraction on any of these you need to create a recipe file with instructions as mentioned [here](../concepts/recipe.md).
In the sample-recipe.yaml add `source` information such as `type` from the table below and `config` for that particular extractor can be found by visiting the link in **Name of Extractor** field.

## List of Extractors

| Name of Extractor | `type` | Data Model used |
| :---- | :---- | :---- |
| [BigQuery](../../plugins/extractors/bigquery/README.md) | `bigquery` | Table |
| [BigTable](../../plugins/extractors/bigtable/README.md) | `bigtable` | Table |
| [Clickhouse](../../plugins/extractors/clickhouse/README.md) | `clickhouse` | Table|
| [CSV](../../plugins/extractors/csv/README.md)  | `csv`| Table |
| [Github](../../plugins/extractors/github/README.md) | `github` | User |
| [Google Cloud Storage](../../plugins/extractors/gcs/README.md) | `gcs` | Bucket |
| [Grafana](../../plugins/extractors/grafana/README.md) | `grafana` | Dashboard |
| [Kafka](../../plugins/extractors/kafka/README.md) | `kafka` | Topic |
| [Metabase](../../plugins/extractors/metabase/README.md) | `metabase` | Dashboard |
| [Mongo DB](../../plugins/extractors/mongodb/README.md) | `mongodb` | Table |
| [MS SQL](../../plugins/extractors/mssql/README.md) | `mssql` | Table |
| [MySQL](../../plugins/extractors/mysql/README.md) | `mysql` | Table |
| [PostgresDB](../../plugins/extractors/postgres/README.md) | `postgres` | Table |
