# labels

`labels` processor will append Asset's Labels with value from given config.

## Usage

```yaml
processors:
  - name: labels
    config:
      labels:
        foo: bar
        myLabel: myValue       
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `labels` | `object` | `{"foo": "bar"}` | Map of string | *required* |


## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
