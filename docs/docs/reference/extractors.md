# Extractors

Meteor currently supports metadata extraction on these data sources. To perform
extraction on any of these you need to create a recipe file with instructions as
mentioned [here](../concepts/recipe.md). In the `sample-recipe.yaml`
add `source` information such as `type` from the table below and `config` for
that particular extractor can be found by visiting the link in the `type` field.

## Extractors Feature Matrix

### Table

| Type                              | Attributes | Profile | Schema | Lineage | Ownership | Custom |
| :-------------------------------- | :--------- | :------ | :----- | :------ | :-------- | :----- |
| [`bigquery`][bigquery-readme]     | ✅         | ✅      | ✅     | ✅      | ✗         | ✗      |
| [`bigtable`][bigtable-readme]     | ✅         | ✗       | ✅     | ✗       | ✗         | ✗      |
| [`cassandra`][cassandra-readme]   | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`clickhouse`][clickhouse-readme] | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`couchdb`][couchdb-readme]       | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`csv`][csv-readme]               | ✅         | ✗       | ✅     | ✗       | ✗         | ✗      |
| [`elastic`][elastic-readme]       | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`mariadb`][mariadb-readme]       | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`mongodb`][mongodb-readme]       | ✅         | ✅      | ✗      | ✗       | ✗         | ✗      |
| [`mssql`][mssql-readme]           | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`mysql`][mysql-readme]           | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`oracle`][oracle-readme]         | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`postgres`][postgres-readme]     | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`presto`][presto-readme]         | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`redshift`][redshift-readme]     | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |
| [`snowflake`][snowflake-readme]   | ✅         | ✅      | ✅     | ✗       | ✗         | ✗      |

### Dashboard

| Type                          | Url | Chart | Lineage | Tags | Custom |
| :---------------------------- | :-- | :---- | :------ | :--- | :----- |
| [`grafana`][grafana-readme]   | ✅  | ✅    | ✗       | ✗    | ✗      |
| [`metabase`][metabase-readme] | ✅  | ✅    | ✗       | ✗    | ✗      |
| [`redash`][redash-readme]     | ✅  | ✗     | ✗       | ✗    | ✗      |
| [`superset`][superset-readme] | ✅  | ✅    | ✅      | ✗    | ✗      |
| [`tableau`][tableau-readme]   | ✅  | ✅    | ✅      | ✗    | ✗      |

### Topic

| Type                    | Profile | Schema | Ownership | Lineage | Tags | Custom |
| :---------------------- | :------ | :----- | :-------- | :------ | :--- | :----- |
| [`kafka`][kafka-readme] | ✗       | ✗      | ✗         | ✗       | ✗    | ✗      |

### User

| Type                      | Email | Username | FullName | Title | IsActive | ManagerEmail | Profiles | Memberships | facets | common |
| :------------------------ | :---- | :------- | :------- | :---- | :------- | :----------- | :------- | :---------- | :----- | :----- |
| [`github`][github-readme] | ✅    | ✅       | ✅       | ✗     | ✅       | ✗            | ✗        | ✗           | ✗      | ✗      |
| [`gsuite`][gsuite-readme] | ✅    | ✗        | ✅       | ✗     | ✅       | ✅           | ✗        | ✗           | ✗      | ✗      |

### Bucket

| Type                | Location | StorageType | Blobs | Ownership | Tags | Custom | Timestamps |
| :------------------ | :------- | :---------- | :---- | :-------- | :--- | :----- | :--------- |
| [`gcs`][gcs-readme] | ✅       | ✅          | ✗     | ✅        | ✅   | ✗      | ✅         |

### Job

| Type                        | Ownership | Upstreams | Downstreams | Custom |
| :-------------------------- | :-------- | :-------- | :---------- | :----- | --- |
| [`optimus`][optimus-readme] | ✅        | ✅        | ✅          | ✅     | ✅  |

### Application

| Type                                          | Ownership | Upstreams | Downstreams | Custom |
| :-------------------------------------------- | :-------- | :-------- | :---------- | :----- | --- |
| [`application_yaml`][application-yaml-readme] | ✅        | ✅        | ✅          | ✅     | ✅  |

### Generic

These are special type of extractors that are capable of extracting _any_ type
of asset.

| Type                  | Ownership | Upstreams | Downstreams | Custom |
| :-------------------- | :-------- | :-------- | :---------- | :----- | --- |
| [`http`][http-readme] | ✅        | ✅        | ✅          | ✅     | ✅  |

<!--- Not using relative links because that breaks the docs build -->

[bigquery-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/bigquery/README.md
[bigtable-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/bigtable/README.md
[cassandra-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/cassandra/README.md
[clickhouse-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/clickhouse/README.md
[couchdb-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/couchdb/README.md
[csv-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/csv/README.md
[elastic-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/elastic/README.md
[mariadb-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/mariadb/README.md
[mongodb-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/mongodb/README.md
[mssql-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/mssql/README.md
[mysql-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/mysql/README.md
[oracle-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/oracle/README.md
[postgres-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/postgres/README.md
[presto-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/presto/README.md
[redshift-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/redshift/README.md
[snowflake-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/snowflake/README.md
[grafana-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/grafana/README.md
[metabase-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/metabase/README.md
[redash-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/redash/README.md
[superset-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/superset/README.md
[tableau-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/tableau/README.md
[kafka-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/kafka/README.md
[github-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/github/README.md
[gsuite-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/gsuite/README.md
[gcs-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/gcs/README.md
[optimus-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/optimus/README.md
[application-yaml-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/application_yaml/README.md
[http-readme]: https://github.com/raystack/meteor/tree/main/plugins/extractors/http/README.md
