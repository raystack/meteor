# shield

## Usage

```yaml
source:
  type: shield
  config:
    host: shield.com:80
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `shield.com:80` | Shield' GRPC host | *required* |

## Outputs

| Field                   | Sample Value                              |
|:------------------------|:------------------------------------------|
| `resource.urn`          | `shield::https://shield-host.com/jonsnow` |
| `resource.name`         | `Jon Snow`                                |
| `resource.service`      | `shield`                                  |
| `resource.type`         | `user`                                    |
| `resource.description`  | `sample user description`                 |
| `email`                 | `snow.jon@gmail.com`                      |
| `username`              | `jonsnow`                                 |
| `full_name`             | `Jon Snow`                                |
| `status`                | `active`                                  |
| `memberships.group_urn` | `grpname:grpId`                           |
| `memberships.role`      | `rolename`                                |
| `timestamp.create_time` | `12432`                                   |
| `timestamp.updatetime`  | `90242`                                   |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
