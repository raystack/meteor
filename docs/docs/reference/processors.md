# Processors

## Enrich

`enrich`

Enrich extra fields to metadata.

### Configs

| Key            | Value    | Example  | Description     |                         |            |
| :------------- | :------- | :------- | :-------------- | :---------------------- | :--------- |
| `{field_name}` | \`string | number\` | `{field_value}` | Dynamic field and value | _required_ |

### Sample usage

```yaml
processors:
  - name: enrich
    config:
      fieldA: valueA
      fieldB: valueB
```

## Labels

`labels`

This processor will append Asset's Labels with value from given config.

[More details][labels-readme]

## Script

Script processor uses the user specified script to transform each asset emitted
from the extractor. Currently, [Tengo][tengo] is the only supported script
engine.

[More details][script-readme]

[labels-readme]: https://github.com/goto/meteor/blob/main/plugins/processors/labels/README.md

[script-readme]: https://github.com/goto/meteor/blob/main/plugins/processors/script/README.md

[tengo]: https://github.com/d5/tengo
