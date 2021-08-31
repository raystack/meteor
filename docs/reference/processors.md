# Processors

## Enrich

`enrich`

Enrich extra fields to metadata.

### Configs

| Key | Value | Example | Description |  |  |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `{field_name}` | \`string | number\` | `{field_value}` | Dynamic field and value | _required_ |

### Sample usage

```yaml
processors:
 - name: enrich
   config:
     fieldA: valueA
     fieldB: valueB
```
