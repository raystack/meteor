# Extractors

Meteor currently support metadata extraction on these data sources. To perform extraction on any of these you need to create a recipe file with instructions as mentioned [here](../concepts/recipe.md). In the `sample-recipe.yaml` add `source` information such as `type` from the table below and `config` for that particular extractor can be found by visiting the link in `type` field.

## Extractors Feature Matrix

### Table

| Type | Attributes | Profile | Schema | Lineage | Ownership | Custom |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| [`clickhouse`](https://github.com/odpf/meteor/tree/main/plugins/extractors/clickhouse/README.md) | ✅  | ✅  | ✅  |  ✗ | ✗ | ✗ |
| [`couchdb`](https://github.com/odpf/meteor/tree/main/plugins/extractors/couchdb/README.md) | ✅  | ✅  | ✅  |  ✗ | ✗ | ✗ |
| [`mongodb`](https://github.com/odpf/meteor/tree/main/plugins/extractors/mongodb/README.md) | ✅  | ✅  |  ✗ | ✗ | ✗ | ✗ |
| [`mssql`](https://github.com/odpf/meteor/tree/main/plugins/extractors/mssql/README.md) | ✅  | ✅  | ✅  | ✗ | ✗ | ✗ |
| [`mysql`](https://github.com/odpf/meteor/tree/main/plugins/extractors/mysql/README.md) | ✅  | ✅  | ✅  | ✗ | ✗ | ✗ |
| [`postgres`](https://github.com/odpf/meteor/tree/main/plugins/extractors/postgres/README.md) | ✅  | ✅  | ✅  | ✗ | ✗ | ✗ |
| [`cassandra`](https://github.com/odpf/meteor/tree/main/plugins/extractors/cassandra/README.md) | ✅  | ✅  | ✅  | ✗ | ✗ | ✗ |

### Dashboard

| Type | Url | Chart | Lineage | Tags | Custom |
| :--- | :--- | :--- | :--- | :--- | :--- |
| [`grafana`](https://github.com/odpf/meteor/tree/main/plugins/extractors/grafana/README.md) | ✅  | ✅  | ✗ | ✗ | ✗ |
| [`metabase`](https://github.com/odpf/meteor/tree/main/plugins/extractors/metabase/README.md) | ✅  | ✅  | ✗ | ✗ | ✗ |
| [`superset`](https://github.com/odpf/meteor/tree/main/plugins/extractors/superset/README.md) | ✅  | ✅  | ✅ | ✗ | ✗ |
| [`tableau`](https://github.com/odpf/meteor/tree/main/plugins/extractors/tableau/README.md) | ✅  | ✅  | ✅ | ✗ | ✗ |

### Topic

| Type | Profile | Schema | Ownership | Lineage | Tags | Custom |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| [`kafka`](https://github.com/odpf/meteor/tree/main/plugins/extractors/kafka/README.md) | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |

### User

| Type | Email | Username | FullName | Title | IsActive | ManagerEmail | Profiles | Memberships | facets | common |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| [`github`](https://github.com/odpf/meteor/tree/main/plugins/extractors/github/README.md) | ✅  | ✅  | ✅  | ☐ | ✅  | ☐ | ☐ | ☐ | ☐ | ☐ |

### Bucket

| type | Location | StorageType | Blobs | Ownership | Tags | Custom | Timestamps |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| [`gcs`](https://github.com/odpf/meteor/tree/main/plugins/extractors/gcs/README.md) | ✅  | ✅  | ✗ | ✅  | ✅  | ✗ | ✅  |

### Job

| Type | Ownership | Upstreams | Downstreams | Custom |
| :--- | :-------- | :-------- | :---------- | :----- |
| [`optimus`](https://github.com/odpf/meteor/tree/main/plugins/extractors/optimus/README.md) | ✅ | ✅ | ✅ | ✅ | ✅ |

