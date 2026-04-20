# Enrich

Append custom key-value pairs to each entity's `properties` map.

## Usage

```yaml
processors:
  - name: enrich
    config:
      attributes:
        team: data-platform
        environment: production
```

## Configuration

| Key          | Type                  | Required | Description                                              |
| :----------- | :-------------------- | :------- | :------------------------------------------------------- |
| `attributes` | `map[string]string`   | Yes      | Key-value pairs to merge into `entity.properties`.       |

## Behavior

- Each key in `attributes` is set directly in the entity's `properties` map. For example, `team: data-platform` results in `entity.properties.team = "data-platform"`.
- If a key already exists in `properties`, the value from the config overwrites it.
- Only string values are written; non-string values in the config are ignored.
- Edges attached to the record are passed through unchanged.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-processor) for information on contributing to this module.
