# Script

Transform each record by running a user-defined [Tengo](https://github.com/d5/tengo) script.

## Usage

```yaml
processors:
  - name: script
    config:
      engine: tengo
      script: |
        entity.properties.custom_flag = "reviewed"
```

## Configuration

| Key      | Type     | Required | Description                                                    |
| :------- | :------- | :------- | :------------------------------------------------------------- |
| `engine` | `string` | Yes      | Script engine. Only `tengo` is supported.                      |
| `script` | `string` | Yes      | Tengo script to execute for each record.                       |

## Behavior

The entity is exposed in the script as a variable called `entity`. It has the following fields:

| Field         | Type     | Description                                        |
| :------------ | :------- | :------------------------------------------------- |
| `urn`         | `string` | Unique resource name                               |
| `name`        | `string` | Human-readable name                                |
| `source`      | `string` | Source system (e.g. `bigquery`, `postgres`)         |
| `type`        | `string` | Entity type (`table`, `dashboard`, `job`, etc.)    |
| `description` | `string` | Description                                        |
| `properties`  | `map`    | Flat key-value map holding all type-specific data  |
| `create_time` | `string` | Creation timestamp (RFC 3339)                      |
| `update_time` | `string` | Last update timestamp (RFC 3339)                   |

All type-specific data (schema, columns, labels, config, etc.) lives under `entity.properties`. Mutations to `entity` fields inside the script are reflected in the output record.

Edges attached to the record are passed through unchanged.

### Notes

- The `os` stdlib module cannot be imported. All other [Tengo standard library modules](https://github.com/d5/tengo/blob/v2.13.0/docs/stdlib.md) are available.
- The script is compiled once during `Init` and cloned per record for safe concurrent execution.

## Example

Add a label and modify a timestamp:

```go
times := import("times")

entity.properties.labels = merge({script_engine: "tengo"}, entity.properties.labels)

update_time := times.parse("2006-01-02T15:04:05Z07:00", entity.properties.update_time)
entity.properties.update_time = times.add_date(update_time, 0, 0, 1)
```

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-processor) for information on contributing to this module.
