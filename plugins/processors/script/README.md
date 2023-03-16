# script

`script` processor will run the user specified script to transform each asset
that is emitted by the extractor. Currently, [Tengo][tengo] is the only
supported script engine.

Refer Tengo documentation for script language syntax and supported functionality
\- https://github.com/d5/tengo/tree/v2.13.0#references
. [Tengo standard library modules][tengo-stdlib] can also be imported and used
if required.

## Usage

```yaml
processors:
  - name: script
    config:
      engine: tengo
      script: |
        asset.owners = append(asset.owners || [], { name: "Big Mom", email: "big.mom@wholecakeisland.com" })
```

## Inputs

| Key      | Value    | Example                                                        | Description                                          | Required? |
|:---------|:---------|:---------------------------------------------------------------|:-----------------------------------------------------|:----------|
| `engine` | `string` | `"tengo"`                                                      | Script engine. Only `"tengo"` is supported currently | ✅         |
| `script` | `string` | `asset.labels = merge({script_engine: "tengo"}, asset.labels)` | [Tengo][tengo] script.                               | ✅         |

### Notes

- Tengo is the only supported script engine.
- Tengo's `os` stdlib module cannot be imported and used in the script.

### Script Globals

#### `asset`

The asset record emitted by the extractor is made available in the script
environment as `asset`. Any changes made to the asset will be reflected in the
record that will be output from the script processor. The field names will be as
per the [`Asset` proto definition][proton-asset]. Furthermore, the data
structure for `asset.data` will be one of the following:

- [`Bucket`][proton-bucket]
- [`Dashboard`][proton-dashboard]
- [`Experiment`][proton-experiment]
- [`FeatureTable`][proton-featuretable]
- [`Group`][proton-group]
- [`Job`][proton-job]
- [`Metric`][proton-metric]
- [`Model`][proton-model]
- [`Application`][proton-application]
- [`Table`][proton-table]
- [`Topic`][proton-topic]
- [`User`][proton-user]

The data type for `asset.data` depends on the specific type of extractor.

## Worked Example

Consider a [`FeatureTable`][proton-featuretable] asset with the following data:

```json
{
  "urn": "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
  "name": "avg_dispatch_arrival_time_10_mins",
  "service": "caramlstore",
  "type": "feature_table",
  "data": {
    "@type": "type.googleapis.com/gotocompany.assets.v1beta2.FeatureTable",
    "namespace": "sauron",
    "entities": [
      {
        "name": "merchant_uuid",
        "labels": {"description": "merchant uuid", "value_type": "STRING"}
      }
    ],
    "features": [
      {
        "name": "ongoing_placed_and_waiting_acceptance_orders",
        "data_type": "INT64"
      },
      {"name": "ongoing_orders", "data_type": "INT64"},
      {"name": "merchant_avg_dispatch_arrival_time_10m", "data_type": "FLOAT"},
      {"name": "ongoing_accepted_orders", "data_type": "INT64"}
    ],
    "create_time": "2022-09-19T22:42:04Z",
    "update_time": "2022-09-21T13:23:02Z"
  },
  "lineage": {
    "upstreams": [
      {
        "urn": "urn:kafka:int-dagstream-kafka.yonkou.io:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
        "service": "kafka",
        "type": "topic"
      }
    ]
  }
}
```

With the following contrived requirements to transform the asset:

- Add a label to the asset - `"script_engine": "tengo`.
- Add a label to each entity. Ex: `"catch_phrase": "You talkin' to me?"`.
- Set an EntityName for each feature based on the following mapping:
  - `ongoing_placed_and_waiting_acceptance_orders: customer_orders`
  - `ongoing_orders: customer_orders`
  - `merchant_avg_dispatch_arrival_time_10m: merchant_driver`
  - `ongoing_accepted_orders: merchant_orders`
- Set the owner as `{Name: Big Mom, Email: big.mom@wholecakeisland.com}`.
- For each lineage upstream, if the service is Kafka, apply a string replace op
  on the URN - `{.yonkou.io => }`.
- Add 1 day to the `update_time` timestamp present under `asset.data`.

The script to apply the transformations above:

[//]: # (@formatter:off)

```go
text := import("text")
times := import("times")

merge := func(m1, m2) {
    for k, v in m2 {
        m1[k] = v
    }
    return m1
}

asset.labels = merge({script_engine: "tengo"}, asset.labels)

for e in asset.data.entities {
    e.labels = merge({catch_phrase: "You talkin' to me?"}, e.labels)
}

for f in asset.data.features {
    if f.name == "ongoing_placed_and_waiting_acceptance_orders" || f.name == "ongoing_orders" {
        f.entity_name = "customer_orders"
    } else if f.name == "merchant_avg_dispatch_arrival_time_10m" {
        f.entity_name = "merchant_driver"
    } else if f.name == "ongoing_accepted_orders" {
        f.entity_name = "merchant_orders"
    }
}

asset.owners = append(asset.owners || [], { name: "Big Mom", email: "big.mom@wholecakeisland.com" })

for u in asset.lineage.upstreams {
    u.urn = u.service != "kafka" ? u.urn : text.replace(u.urn, ".yonkou.io", "", -1)
}

update_time := times.parse("2006-01-02T15:04:05Z07:00", asset.data.update_time)
asset.data.update_time = times.add_date(update_time, 0, 0, 1)
```

[//]: # (@formatter:on)

With this script, the output from the processor would have the following asset:

```json
{
  "urn": "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
  "name": "avg_dispatch_arrival_time_10_mins",
  "service": "caramlstore",
  "type": "feature_table",
  "data": {
    "@type": "type.googleapis.com/gotocompany.assets.v1beta2.FeatureTable",
    "namespace": "sauron",
    "entities": [
      {
        "name": "merchant_uuid",
        "labels": {
          "catch_phrase": "You talkin' to me?",
          "description": "merchant uuid",
          "value_type": "STRING"
        }
      }
    ],
    "features": [
      {
        "name": "ongoing_placed_and_waiting_acceptance_orders",
        "data_type": "INT64",
        "entity_name": "customer_orders"
      },
      {
        "name": "ongoing_orders",
        "data_type": "INT64",
        "entity_name": "customer_orders"
      },
      {
        "name": "merchant_avg_dispatch_arrival_time_10m",
        "data_type": "FLOAT",
        "entity_name": "merchant_driver"
      },
      {
        "name": "ongoing_accepted_orders",
        "data_type": "INT64",
        "entity_name": "merchant_orders"
      }
    ],
    "create_time": "2022-09-19T22:42:04Z",
    "update_time": "2022-09-22T13:23:02Z"
  },
  "owners": [
    {"name": "Big Mom", "email": "big.mom@wholecakeisland.com"}
  ],
  "lineage": {
    "upstreams": [
      {
        "urn": "urn:kafka:int-dagstream-kafka:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
        "service": "kafka",
        "type": "topic"
      }
    ]
  },
  "labels": {"script_engine": "tengo"}
}
```

## Contributing

Refer to
the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-processor)
for information on contributing to this module.

[tengo]: https://github.com/d5/tengo

[tengo-stdlib]: https://github.com/d5/tengo/blob/v2.13.0/docs/stdlib.md

[proton-asset]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/asset.proto#L14

[proton-bucket]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/bucket.proto#L13

[proton-dashboard]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/dashboard.proto#L14

[proton-experiment]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/experiment.proto#L15

[proton-featuretable]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/feature_table.proto#L32

[proton-group]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/group.proto#L12

[proton-job]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/job.proto#L13

[proton-metric]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/metric.proto#L13

[proton-model]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/model.proto#L73

[proton-application]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/application.proto#L11

[proton-table]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/table.proto#L14

[proton-topic]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/topic.proto#L14

[proton-user]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/user.proto#L15

