# File

Write metadata records to a local file in NDJSON or YAML format.

## Usage

```yaml
sinks:
  - name: file
    config:
      path: "./output/metadata.ndjson"
      format: "ndjson"
      overwrite: false
```

## Configuration

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `path` | `string` | `./dir/sample.ndjson` | Absolute or relative path to the output file. The parent directory must exist. | *required* |
| `format` | `string` | `ndjson` | Output format: `ndjson` or `yaml` | *required* |
| `overwrite` | `bool` | `true` | When `true` (default), the file is truncated on init. When `false`, data is appended to an existing file. | *optional* |

## Behavior

Each Record (Entity + Edges) is serialized as JSON. In `ndjson` mode, one JSON object per line is written. In `yaml` mode, all records in a batch are written as a YAML list.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-sink) for information on contributing to this module.
