# Processors

Processors transform or enrich metadata records after extraction and before they reach a sink. They are executed sequentially in the order defined in the recipe — the output of one processor becomes the input of the next.

## Enrich

`enrich`

Append custom attributes to each asset's data. Useful for adding metadata that does not exist in the source system.

### Config

| Key | Value | Example | Description | |
| :-- | :---- | :------ | :---------- | :- |
| `attributes` | `map` | `{team: platform}` | Key-value pairs to merge into the asset's attributes | *required* |

### Sample usage

```yaml
processors:
  - name: enrich
    config:
      attributes:
        team: data-platform
        environment: production
```

[More details][enrich-readme]

## Labels

`labels`

Append key-value labels to each asset's `labels` field. Labels are useful for categorization, filtering, and routing in downstream systems.

### Config

| Key | Value | Example | Description | |
| :-- | :---- | :------ | :---------- | :- |
| `labels` | `map[string]string` | `{source: meteor}` | Key-value pairs to append to the asset's labels | *required* |

### Sample usage

```yaml
processors:
  - name: labels
    config:
      labels:
        source: meteor
        classification: internal
```

[More details][labels-readme]

## Script

`script`

Transform each asset using a user-defined script. Currently, [Tengo][tengo] is the only supported script engine. The script processor gives you full control over asset transformation, including the ability to make HTTP calls to external services.

### Config

| Key | Value | Example | Description | |
| :-- | :---- | :------ | :---------- | :- |
| `engine` | `string` | `tengo` | Script engine to use (currently only `tengo`) | *required* |
| `script` | `string` | _see below_ | Inline Tengo script | *required* |

### Sample usage

```yaml
processors:
  - name: script
    config:
      engine: tengo
      script: |
        asset.labels["processed"] = "true"
```

[More details][script-readme]

## Chaining Processors

Processors execute sequentially in the order they appear in the recipe. Each processor receives the output of the previous one. This allows you to build transformation pipelines:

```yaml
processors:
  - name: enrich          # Step 1: add attributes
    config:
      attributes:
        domain: payments
  - name: labels          # Step 2: add labels
    config:
      labels:
        source: meteor
  - name: script          # Step 3: custom transform
    config:
      engine: tengo
      script: |
        asset.name = asset.name + " (processed)"
```

If a processor fails, the entire recipe execution fails. There is no skip-on-error behavior — fix the processor configuration to resolve errors.

[enrich-readme]: https://github.com/raystack/meteor/blob/main/plugins/processors/enrich/README.md
[labels-readme]: https://github.com/raystack/meteor/blob/main/plugins/processors/labels/README.md
[script-readme]: https://github.com/raystack/meteor/blob/main/plugins/processors/script/README.md
[tengo]: https://github.com/d5/tengo
