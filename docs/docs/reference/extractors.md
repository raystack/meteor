# Extractors

Meteor currently supports metadata extraction on these data sources. To perform extraction on any of these you need to create a recipe file with instructions as mentioned [here](../concepts/recipe.md). In the `sample-recipe.yaml` add `source` information such as `type` from the table below and `config` for that particular extractor can be found by visiting the link in the `type` field.

## Extractors Feature Matrix

### Table

| Type                                                             | Attributes | Profile | Schema | Lineage | Ownership | Custom |
|:-----------------------------------------------------------------|:-----------|:--------|:-------|:--------|:----------|:-------|
| [`clickhouse`](../../../plugins/extractors/clickhouse/README.md) | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`couchdb`](../../../plugins/extractors/couchdb/README.md)       | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`mongodb`](../../../plugins/extractors/mongodb/README.md)       | ✅          | ✅       | ✗      | ✗       | ✗         | ✗      |
| [`mssql`](../../../plugins/extractors/mssql/README.md)           | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`mysql`](../../../plugins/extractors/mysql/README.md)           | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`postgres`](../../../plugins/extractors/postgres/README.md)     | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`cassandra`](../../../plugins/extractors/cassandra/README.md)   | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`oracle`](../../../plugins/extractors/oracle/README.md)         | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`mariadb`](../../../plugins/extractors/mariadb/README.md)       | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`redshift`](../../../plugins/extractors/redshift/README.md)     | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`presto`](../../../plugins/extractors/presto/README.md)         | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |
| [`snowflake`](../../../plugins/extractors/snowflake/README.md)   | ✅          | ✅       | ✅      | ✗       | ✗         | ✗      |

### Dashboard

| Type                                                         | Url | Chart | Lineage | Tags | Custom |
|:-------------------------------------------------------------|:----|:------|:--------|:-----|:-------|
| [`grafana`](../../../plugins/extractors/grafana/README.md)   | ✅   | ✅     | ✗       | ✗    | ✗      |
| [`metabase`](../../../plugins/extractors/metabase/README.md) | ✅   | ✅     | ✗       | ✗    | ✗      |
| [`superset`](../../../plugins/extractors/superset/README.md) | ✅   | ✅     | ✅       | ✗    | ✗      |
| [`tableau`](../../../plugins/extractors/tableau/README.md)   | ✅   | ✅     | ✅       | ✗    | ✗      |
| [`redash`](../../../plugins/extractors/redash/README.md)     | ✅   | ✗     | ✗       | ✗    | ✗      |


### Topic

| Type                                                   | Profile | Schema | Ownership | Lineage | Tags | Custom |
|:-------------------------------------------------------|:--------|:-------|:----------|:--------|:-----|:-------|
| [`kafka`](../../../plugins/extractors/kafka/README.md) | ✗       | ✗      | ✗         | ✗       | ✗    | ✗      |

### User

| Type                                                     | Email | Username | FullName | Title | IsActive | ManagerEmail | Profiles | Memberships | facets | common |
|:---------------------------------------------------------|:------|:---------|:---------|:------|:---------|:-------------|:---------|:------------|:-------|:-------|
| [`github`](../../../plugins/extractors/github/README.md) | ✅     | ✅        | ✅        | ☐     | ✅        | ☐            | ☐        | ☐           | ☐      | ☐      |
| [`shield`](../../../plugins/extractors/shield/README.md) | ✅     | ✅        | ✅        | ☐     | ✅        | ☐            | ☐        | ✅           | ✅      | ☐      |
| [`gsuite`](../../../plugins/extractors/gsuite/README.md) | ✅     | ☐        | ✅        | ☐     | ✅        | ✅            | ☐        | ☐           | ☐      | ☐      |

### Bucket

| type                                               | Location | StorageType | Blobs | Ownership | Tags | Custom | Timestamps |
|:---------------------------------------------------|:---------|:------------|:------|:----------|:-----|:-------|:-----------|
| [`gcs`](../../../plugins/extractors/gcs/README.md) | ✅        | ✅           | ✗     | ✅         | ✅    | ✗      | ✅          |

### Job

| Type                                                       | Ownership | Upstreams | Downstreams | Custom |
|:-----------------------------------------------------------|:----------|:----------|:------------|:-------|
| [`optimus`](../../../plugins/extractors/optimus/README.md) | ✅         | ✅         | ✅           | ✅      | ✅ |

### Machine Learning Feature Table

| Type                                                               | Ownership | Upstreams | Downstreams | Custom |
|:-------------------------------------------------------------------|:----------|:----------|:------------|:-------|
| [`caramlstore`](../../../plugins/extractors/caramlstore/README.md) | ✗         | ✅         | ✗           | ✅      |

