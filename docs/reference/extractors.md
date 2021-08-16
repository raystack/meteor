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

## Feature Matrix

### Tables

| Database | Urn | Name | Source | Description | Profile | Schema | Ownership | Lineage | Tags | Custom | Timestamps | Event |
| :--- | :-- | :-- | :-- | :-- | :-- | :-- |:-- |:-- | :-- | :-- | :-- | :-- |
| Clickhouse |  &#9745; | &#9745; | &#9745; | &#9745; | &#9744; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |
| MongoDB |  &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9744; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |
| MSSQL |  &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |
| MySQL |  &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |
| Postgres |  &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |

### Dashboard

|Dashboard |Urn |Name |Source |Description |Url | Chart |Lineage |Tags |Custom |Timestamps |Event |
| :-- |  :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| Grafana |  &#9745; | &#9745; | &#9745; | &#9744; | &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; |
| Metabase |  &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; |

### Topic

| Topic |Urn  |Name |Source | Description| Profile| Schema |Ownership | Lineage |Tags |Custom |Timestamps | Event |
| :--- | :-- | :-- | :-- | :-- | :-- | :-- |:-- |:-- | :-- | :-- | :-- | :-- |
| Kafka |  &#9744; | &#9745; | &#9745; | &#9744; | &#9744; | &#9744; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |

### User

| User | Urn  |Source |Email |Username |FirstName |LastName |FullName | DisplayName | Title | IsActive | ManagerEmail | Profiles | Memberships | Tags | Custom | Timestamps | Event |
| :--- | :-- | :-- | :-- | :-- | :-- | :-- |:-- |:-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| GitHub |  &#9745; | &#9744; | &#9745; | &#9745; | &#9744; | &#9744; | &#9745; | &#9744; |&#9744; | &#9745; | &#9744; | &#9744; | &#9744; | &#9744; | &#9744; | &#9744; | &#9744; |

### Bucket

|Bucket | Urn | Name | Source | Description | Location | StorageType| Blobs  | Ownership | Tags | Custom | Timestamps | Event |
| :--- | :-- | :-- | :-- | :-- | :-- | :-- |:-- |:-- | :-- | :-- | :-- | :-- |
| GCS |  &#9745; | &#9745; | &#9745; | &#9744; | &#9745; | &#9745; | &#9744; | &#9745; |&#9745; | &#9744; | &#9745; | &#9744; |
