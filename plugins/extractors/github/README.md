# Github

## Usage
```yaml
source:
  type: github
  config:
    org: odpf
    token: github_token
```
## Inputs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `org` | `string` | `odpf` | Name of github organisation | *required* |
| `token` | `string` | `kdfljdfljoijj` | Github API access token | *required* |

## Outputs
| Field | Sample Value |
| :---- | :---- |
| `urn` | `https://github.com/ravisuhag` |
| `email` | `suhag.ravi@gmail.com` |
| `username` | `ravisuhag` |
| `full_name` | `Ravi Suhag` |
| `is_active` | `true` |
~

## Contributing
Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.