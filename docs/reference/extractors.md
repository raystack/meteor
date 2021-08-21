# Extractors

Metor currently support metadata extraction on these data sources.
To perform extraction on any of these you need to create a recipe file with instructions as mentioned [here](../concepts/recipe.md).
In the sample-recipe.yaml add `source` information such as `type` from the table below and `config` for that particular extractor can be found by visiting the link in `type` field.

## Extractors Feature Matrix

### Table

| `type` | Profile | Schema | Ownership | Lineage | Tags | Custom | Timestamps | Event |
| :--- | :-- | :-- |:-- |:-- | :-- | :-- | :-- | :-- |
| [`clickhouse`](../../plugins/extractors/clickhouse/README.md) | &#9744; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |
| [`mongodb`](../../plugins/extractors/mongodb/README.md) |  &#9745; | &#9744; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |
| [`mssql`](../../plugins/extractors/mssql/README.md) |  &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |
| [`mysql`](../../plugins/extractors/mysql/README.md) |  &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |
| [`postgres`](../../plugins/extractors/postgres/README.md) |  &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |

### Dashboard

|type |Url | Chart |Lineage |Tags |Custom |Timestamps |Event |
| :-- |  :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| [`grafana`](../../plugins/extractors/grafana/README.md) |  &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; |
| [`metabase`](../../plugins/extractors/metabase/README.md) | &#9745; | &#9745; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; |

### Topic

| type | Profile| Schema |Ownership | Lineage |Tags |Custom |Timestamps | Event |
| :--- | :-- | :-- |:-- |:-- | :-- | :-- | :-- | :-- |
| [`kafka`](../../plugins/extractors/kafka/README.md) |  &#9744; | &#9744; | &#9744; | &#9744; |&#9744; | &#9744; | &#9744; | &#9744; |

### User

| type | Email |Username |FullName | Title | IsActive | ManagerEmail | Profiles | Memberships | facets | common |
| :--- | :-- | :-- | :-- | :-- | :-- | :-- |:-- |:-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| [`github`](../../plugins/extractors/github/README.md)  | &#9745; | &#9745; | &#9745; |&#9744; | &#9745; | &#9744; | &#9744; | &#9744; | &#9744; | &#9744; |

### Bucket

| type | Location | StorageType| Blobs  | Ownership | Tags | Custom | Timestamps | Event |
| :--- | :-- | :-- |:-- |:-- | :-- | :-- | :-- | :-- |
| [`gcs`](../../plugins/extractors/gcs/README.md) | &#9745; | &#9745; | &#9744; | &#9745; |&#9745; | &#9744; | &#9745; | &#9744; |
