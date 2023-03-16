# caramlstore

Extractor for Machine Learning(ML) Features from [caraml-store][caraml-store].

The extractor uses the [`CoreService.proto`][coreservice.proto] service exposed by 
[caraml-store][caraml-store] to extract feature tables.

## Usage

```yaml
source:
  name: caramlstore
  scope: caramlstore-stg
  config:
    url: staging.caraml-store.com:80
    max_size_in_mb: 10
    request_timeout: 30s
```

## Inputs

| Key               | Value    | Example               | Description                                                | Required? |
|:------------------|:---------|:----------------------|:-----------------------------------------------------------|:----------|
| `url`             | `string` | `caraml-store.com:80` | caraml-store's host URL                                    | ✅         |
| `max_size_in_mb`  | `int`    | `10`                  | Max MB for gRPC client to receive message. Default is 45.  | ✘          |
| `request_timeout` | `string` | `10s`                 | Timeout for gRPC requests to caraml-store. Default is 10s. | ✘         |

## Outputs

The feature tables are mapped to an [`Asset`][proton-asset] with model specific
metadata stored using [`FeatureTable`][proton-featuretable]. Please refer 
the proto definitions for more information.

| Field              | Value                                                                                | Sample Value                                                                         |
|:-------------------|:-------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------|
| `resource.urn`     | `urn:caramlstore:{scope}:feature_table:{feature_table.project}.{feature_table.name}` | `urn:caramlstore:caramlstore-stg:feature_table:my_project.merchant_uuid_t2_discovery` |
| `resource.name`    | `{feature_table.name}`                                                               | `merchant_uuid_t2_discovery`                                                         |
| `resource.service` | `caramlstore`                                                                        | `caramlstore`                                                                        |
| `resource.type`    | `feature_table`                                                                      | `feature_table`                                                                      |
| `namespace`        | `{feature_table.project}`                                                            | `my_project`                                                                         |
| `entities`         | [`[]Entity`](#entity)                                                                |                                                                                      |
| `features`         | [`[]Feature`](#feature)                                                              |                                                                                      |
| `create_time`      | `{feature_table.created_timestamp}`                                                  | `2022-08-08T03:17:54Z`                                                               |
| `update_time`      | `{feature_table.updated_timestamp}`                                                  | `2022-08-08T03:57:54Z`                                                               |

### Entity

| Field    | Value               | Sample Value                                            |
|:---------|:--------------------|:--------------------------------------------------------|
| `name`   | `{entity.name}`     | `service_type`                                          |
| `labels` | `map[string]string` | `{"description":"merchant uuid","value_type":"STRING"}` |

### Feature

| Field       | Value                   | Sample Value          |
|:------------|:------------------------|:----------------------|
| `name`      | `{feature.name}`        | `avg_gmv_merchant_1d` |
| `data_type` | `{features.value_type}` | `INT64`               |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) 
for information on contributing to this module.

[caraml-store]: https://github.com/caraml-dev/caraml-store
[coreservice.proto]: https://github.com/caraml-dev/caraml-store/blob/v0.1.1/caraml-store-protobuf/src/main/proto/feast/core/CoreService.proto#L12
[proton-asset]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/asset.proto#L14
[proton-featuretable]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/feature_table.proto#L32
