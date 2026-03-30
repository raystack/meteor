# script

`script` processor will run the user specified script to transform each record
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
        asset.properties.custom_flag = "reviewed"
```

## Inputs

| Key      | Value    | Example                                                                         | Description                                          | Required? |
| :------- | :------- | :------------------------------------------------------------------------------ | :--------------------------------------------------- | :-------- |
| `engine` | `string` | `"tengo"`                                                                       | Script engine. Only `"tengo"` is supported currently | ✅        |
| `script` | `string` | `asset.properties.labels = merge({script_engine: "tengo"}, asset.properties.labels)` | [Tengo][tengo] script.                               | ✅        |

### Notes

- Tengo is the only supported script engine.
- Tengo's `os` stdlib module cannot be imported and used in the script.

### Script Globals

#### `asset`

The entity record emitted by the extractor is made available in the script
environment as `asset`. Note: the variable is still called `asset` in tengo
scripts for backward compatibility, but it represents an Entity. Any changes
made to the entity will be reflected in the record that will be output from the
script processor.

The `asset` object has the following structure:

| Field         | Type     | Description                                    |
|:--------------|:---------|:-----------------------------------------------|
| `urn`         | `string` | Unique resource name                           |
| `name`        | `string` | Human-readable name                            |
| `source`      | `string` | Source system (replaces old `service` field)    |
| `type`        | `string` | Entity type (`table`, `dashboard`, `job`, etc.) |
| `description` | `string` | Description                                    |
| `properties`  | `map`    | Flat key-value map with all type-specific data |

All type-specific data (schema, columns, features, config, labels, etc.) lives
under `asset.properties`. There are no separate typed schemas.

## Worked Example

Consider a feature table entity with the following data:

```json
{
  "urn": "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
  "name": "avg_dispatch_arrival_time_10_mins",
  "source": "caramlstore",
  "type": "feature_table",
  "properties": {
    "namespace": "sauron",
    "entities": [
      {
        "name": "merchant_uuid",
        "labels": { "description": "merchant uuid", "value_type": "STRING" }
      }
    ],
    "features": [
      { "name": "ongoing_placed_and_waiting_acceptance_orders", "data_type": "INT64" },
      { "name": "ongoing_orders", "data_type": "INT64" },
      { "name": "merchant_avg_dispatch_arrival_time_10m", "data_type": "FLOAT" },
      { "name": "ongoing_accepted_orders", "data_type": "INT64" }
    ],
    "create_time": "2022-09-19T22:42:04Z",
    "update_time": "2022-09-21T13:23:02Z"
  }
}
```

With the following contrived requirements to transform the entity:

- Add a label under properties - `"script_engine": "tengo"`.
- Add a label to each entity in the features. Ex: `"catch_phrase": "You talkin' to me?"`.
- Set an EntityName for each feature based on a mapping.
- Add 1 day to the `update_time` timestamp present under `asset.properties`.

The script to apply the transformations above:

[//]: # "@formatter:off"

```go
text := import("text")
times := import("times")

merge := func(m1, m2) {
    for k, v in m2 {
        m1[k] = v
    }
    return m1
}

asset.properties.labels = merge({script_engine: "tengo"}, asset.properties.labels)

for e in asset.properties.entities {
    e.labels = merge({catch_phrase: "You talkin' to me?"}, e.labels)
}

for f in asset.properties.features {
    if f.name == "ongoing_placed_and_waiting_acceptance_orders" || f.name == "ongoing_orders" {
        f.entity_name = "customer_orders"
    } else if f.name == "merchant_avg_dispatch_arrival_time_10m" {
        f.entity_name = "merchant_driver"
    } else if f.name == "ongoing_accepted_orders" {
        f.entity_name = "merchant_orders"
    }
}

update_time := times.parse("2006-01-02T15:04:05Z07:00", asset.properties.update_time)
asset.properties.update_time = times.add_date(update_time, 0, 0, 1)
```

[//]: # "@formatter:on"

With this script, the output from the processor would have the following entity:

```json
{
  "urn": "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
  "name": "avg_dispatch_arrival_time_10_mins",
  "source": "caramlstore",
  "type": "feature_table",
  "properties": {
    "namespace": "sauron",
    "labels": { "script_engine": "tengo" },
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
      { "name": "ongoing_placed_and_waiting_acceptance_orders", "data_type": "INT64", "entity_name": "customer_orders" },
      { "name": "ongoing_orders", "data_type": "INT64", "entity_name": "customer_orders" },
      { "name": "merchant_avg_dispatch_arrival_time_10m", "data_type": "FLOAT", "entity_name": "merchant_driver" },
      { "name": "ongoing_accepted_orders", "data_type": "INT64", "entity_name": "merchant_orders" }
    ],
    "create_time": "2022-09-19T22:42:04Z",
    "update_time": "2022-09-22T13:23:02Z"
  }
}
```

Note: Ownership and lineage are now represented as Edges on the Record, not as
fields on the entity. Edges have `source_urn`, `target_urn`, `type` (e.g.
`owned_by`, `lineage`), `source`, and `properties`.

## Contributing

Refer to
the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-processor)
for information on contributing to this module.

[tengo]: https://github.com/d5/tengo
[tengo-stdlib]: https://github.com/d5/tengo/blob/v2.13.0/docs/stdlib.md
