# Github

## Usage

```yaml
source:
  name: github
  config:
    org: raystack
    token: github_token
```

## Inputs

| Key     | Value    | Example         | Description                 |            |
| :------ | :------- | :-------------- | :-------------------------- | :--------- |
| `org`   | `string` | `raystack`      | Name of github organisation | _required_ |
| `token` | `string` | `kdfljdfljoijj` | Github API access token     | _required_ |

## Outputs

| Field          | Sample Value                   |
| :------------- | :----------------------------- |
| `resource.urn` | `https://github.com/ravisuhag` |
| `email`        | `suhag.ravi@gmail.com`         |
| `username`     | `ravisuhag`                    |
| `full_name`    | `Ravi Suhag`                   |
| `status`       | `active`                       |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
