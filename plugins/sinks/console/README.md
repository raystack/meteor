# Console

Print metadata records to standard output as JSON.

## Usage

```yaml
sinks:
  - name: console
```

## Configuration

No configuration is required.

## Behavior

Each Record (Entity + Edges) is serialized as JSON and printed to stdout, one JSON object per line. Useful for debugging recipes and inspecting extractor output.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
