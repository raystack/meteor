# Enrich

`enrich` processor appends custom attributes to each asset emitted by the extractor. Use it to add metadata that is not available in the source system, such as team ownership tags, environment labels, or domain identifiers.

## Usage

```yaml
processors:
  - name: enrich
    config:
      attributes:
        fieldA: valueA
        fieldB: valueB
```

## Config

| Key | Value | Example | Description | |
| :-- | :---- | :------ | :---------- | :- |
| `attributes` | `map[string]interface{}` | `{team: platform}` | Key-value pairs to append to each asset's attributes | *required* |

## Behavior

- The `attributes` map is merged into the asset's `data.attributes` field.
- If a key already exists in the asset's attributes, the value from the enrich config will overwrite it.
- Values can be strings, numbers, booleans, or nested objects.

## Examples

### Adding team and environment tags

```yaml
processors:
  - name: enrich
    config:
      attributes:
        team: data-platform
        environment: production
        tier: "1"
```

### Combining with other processors

```yaml
processors:
  - name: enrich
    config:
      attributes:
        domain: payments
  - name: labels
    config:
      labels:
        source: meteor
```

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-processor) for information on contributing to this module.
