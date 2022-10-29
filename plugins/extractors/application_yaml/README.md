# application_yaml

Extractor for Application from YAML file.

## Usage

```yaml
source:
  name: application_yaml
  scope: applications-stg
  config:
    file: "./path/to/meteor.app.yaml"
    env_prefix: CI
```

## Inputs

| Key          | Value    | Example           | Description                                                                                                            | Required? |
|:-------------|:---------|:------------------|:-----------------------------------------------------------------------------------------------------------------------|:----------|
| `file`       | `string` | `meteor.app.yaml` | File path of `application.yaml`                                                                                        | ✅         |
| `env_prefix` | `string` | `CI`              | Prefix for environment variables. These are made available as variables in `application.yaml` with the prefix trimmed. | ❌         |

### `application.yaml` format

```yaml
name: "string" # REQUIRED
id: "string" # REQUIRED
team:
  id: "string"
  name: "string"
  email: "string"
description: "string"
url: "string"
version: "string"
inputs: # OPTIONAL
  - type: "batch" # ONE OF {batch, kafka}
    platform: "bigquery" # If type is batch then bigquery 
    options:
      table_ref: "{project_id}:{dataset_id}.{table_id}" # REQUIRED
  - type: "stream"
    platform: "kafka" # If type is stream then kafka
    options:
      topic: "estimate-log" # REQUIRED
      bootstrap_servers: "host:port" # REQUIRED
outputs: # OPTIONAL
  - type: "stream"
    platform: "kafka"
    options:
      topic: "string"
      bootstrap_servers: "host:port"
create_time: "2006-01-02T15:04:05Z"
update_time: "2006-01-02T15:04:05Z"
labels:
  x: "y"
```

For an example,
see [`application.detailed.yaml`](./testdata/application.detailed.yaml)

Environment variables that are commonly available in CI can be used for
populating the fields. The default prefix for environment variables is `CI` but
this can be overridden using `env_prefix` config.

For an example,
see [`application.envvars.yaml`](./testdata/application.envvars.yaml). The
following env vars are utilised for it:

- `CI_PROJECT_NAME`
- `CI_PROJECT_URL`
- `CI_COMMIT_SHORT_SHA`

## Outputs

The application is mapped to an [`Asset`][proton-asset] with model specific
metadata stored using [`Application`][proton-application]. Please refer the
proto definitions for more information.

| Field                           | Value                                                         | Sample Value                                                                   |
|:--------------------------------|:--------------------------------------------------------------|:-------------------------------------------------------------------------------|
| `resource.urn`                  | `urn:application_yaml:{scope}:application:{application.name}` | `urn:application_yaml:integration:application:order-manager`                   |
| `resource.name`                 | `{application.name}`                                          | `order-manager`                                                                |
| `resource.service`              | `application_yaml`                                            | `application_yaml`                                                             |
| `resource.type`                 | `application`                                                 | `application`                                                                  |
| `resource.url`                  | `{application.url}`                                           | `https://github.com/mycompany/order-manager`                                   |
| `resource.description`          | `{application.description`                                    | `Order-Manager is the order management system for MyCompany`                   |
| `application_id`                | `application.id`                                              | `0adf3214-676c-4a74-ab37-9d4a4b8ade0e`                                         |
| `version`                       | `application.version`                                         | `d6ec883`                                                                      |
| `create_time`                   | `{application.create_time}`                                   | `2022-08-08T03:17:54Z`                                                         |
| `update_time`                   | `{application.update_time}`                                   | `2022-08-08T03:57:54Z`                                                         |
| `ownership.owners[0].urn`       | `{application.team.id}`                                       | `9ebcc2f8-5894-47c6-83a9-160b7eaa3f6b`                                         |
| `ownership.owners[0].name`      | `{application.team.name}`                                     | `Search`                                                                       |
| `ownership.owners[0].email`     | `{application.team.email}`                                    | `search@mycompany.com`                                                         |
| `lineage.upstreams[].urn`       | `urn:{service}:{scope}:{type}:{name}`                         | `urn:kafka:int-kafka.yonkou.io:topic:staging_30min_demand`                     |
| `lineage.upstreams[].type`      | `{type}`                                                      | `topic`                                                                        |
| `lineage.upstreams[].service`   | `{service}`                                                   | `kafka`                                                                        |
| `lineage.downstreams[].urn`     | `urn:{service}:{scope}:{type}:{name}`                         | `urn:bigquery:bq-internal:table:bq-internal:dagstream.production_30min_demand` |
| `lineage.downstreams[].type`    | `{type}`                                                      | `table`                                                                        |
| `lineage.downstreams[].service` | `{service}`                                                   | `bigquery`                                                                     |
| `resource.labels`               | `map[string]string`                                           | `{"team": "Booking Experience"}`                                               |

## Contributing

Refer to
the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor)
for information on contributing to this module.

[proton-asset]: https://github.com/odpf/proton/blob/2d2177a/odpf/assets/v1beta2/asset.proto#L14

[proton-application]: https://github.com/odpf/proton/blob/2d2177a/odpf/assets/v1beta2/application.proto#L11
