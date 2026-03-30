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
| :----------- | :------- | :---------------- | :--------------------------------------------------------------------------------------------------------------------- | :-------- |
| `file`       | `string` | `meteor.app.yaml` | File path of `application.yaml`                                                                                        | ✅        |
| `env_prefix` | `string` | `CI`              | Prefix for environment variables. These are made available as variables in `application.yaml` with the prefix trimmed. | ✘         |

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
  # Format: "urn:{source}:{scope}:{type}:{name}"
  - urn:bigquery:bq-raw-internal:table:bq-raw-internal:dagstream.production_feast09_s2id13_30min_demand
  - urn:kafka:int-dagstream-kafka.yonkou.io:topic:staging_feast09_s2id13_30min_demand
outputs: # OPTIONAL
  # Format: "urn:{source}:{scope}:{type}:{name}"
  - urn:kafka:1-my-kafka.com:topic:staging_feast09_mixed_granularity_demand_forecast_3es
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

The extractor emits a Record containing an Entity and Edges.

### Entity

| Field               | Value                                                         | Sample Value                                                 |
| :------------------ | :------------------------------------------------------------ | :----------------------------------------------------------- |
| `urn`               | `urn:application_yaml:{scope}:application:{application.name}` | `urn:application_yaml:integration:application:order-manager` |
| `name`              | `{application.name}`                                          | `order-manager`                                              |
| `source`            | `application_yaml`                                            | `application_yaml`                                           |
| `type`              | `application`                                                 | `application`                                                |
| `description`       | `{application.description}`                                   | `Order-Manager is the order management system for MyCompany` |
| `properties.url`    | `{application.url}`                                           | `https://github.com/mycompany/order-manager`                 |
| `properties.id`     | `{application.id}`                                            | `0adf3214-676c-4a74-ab37-9d4a4b8ade0e`                      |
| `properties.version`| `{application.version}`                                       | `d6ec883`                                                    |
| `properties.create_time` | `{application.create_time}`                              | `2022-08-08T03:17:54Z`                                       |
| `properties.update_time` | `{application.update_time}`                              | `2022-08-08T03:57:54Z`                                       |
| `properties.labels` | `map[string]string`                                           | `{"team": "Booking Experience"}`                             |

### Edges

| Edge Type   | Description                             | Example                                                                            |
|:------------|:----------------------------------------|:-----------------------------------------------------------------------------------|
| `owned_by`  | Team ownership from `application.team`  | `source_urn: <app_urn>`, `target_urn: {team.id}`, `properties: {name, email}`      |
| `lineage`   | Upstream from `application.inputs[]`    | `source_urn: {input_urn}`, `target_urn: <app_urn>`, `type: lineage`                |
| `lineage`   | Downstream from `application.outputs[]` | `source_urn: <app_urn>`, `target_urn: {output_urn}`, `type: lineage`               |

## Contributing

Refer to
the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor)
for information on contributing to this module.
