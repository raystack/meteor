# Labels

Append labels to each entity's `properties.labels` map.

## Usage

```yaml
processors:
  - name: labels
    config:
      labels:
        team: data-platform
        source: meteor
```

## Configuration

| Key      | Type                | Required | Description                                                  |
| :------- | :------------------ | :------- | :----------------------------------------------------------- |
| `labels` | `map[string]string` | Yes      | Key-value pairs to merge into `entity.properties.labels`.    |

## Behavior

- The processor reads the existing `labels` map from `entity.properties.labels` (or creates one if absent).
- Each key from the config is merged into that map. Existing keys with the same name are overwritten.
- The updated `labels` map is written back to `entity.properties.labels`.
- Edges attached to the record are passed through unchanged.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-processor) for information on contributing to this module.
