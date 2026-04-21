# Console

Print metadata records to standard output.

## Usage

```yaml
sinks:
  - name: console
    config:
      format: json
```

## Configuration

| Key | Description | Default | Required |
|---|---|---|---|
| `format` | Output format: `json` or `markdown` | `json` | No |

## Formats

### JSON (default)

Each Record is serialized as a single JSON object per line:

```json
{"entity":{"urn":"urn:postgres:prod:table:public.users","type":"table","name":"users","source":"postgres"},"edges":[...]}
```

### Markdown

Each Record is rendered as a readable markdown document with tables for metadata, properties, and edges. Useful for piping into local AI tools like Claude Code:

```markdown
## users

| Field | Value |
|---|---|
| URN | `urn:postgres:prod:table:public.users` |
| Type | table |
| Source | postgres |

### Properties

- **database**: mydb

### Columns

| Data Type | Name |
|---|---|
| integer | id |
| varchar | email |

### Edges

| Type | Source URN | Target URN |
|---|---|---|
| owned_by | `urn:postgres:prod:table:public.users` | `urn:org:team:data-eng` |
```

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-sink) for information on contributing to this module.
