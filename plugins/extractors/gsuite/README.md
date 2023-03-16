# G-Suite

## Usage

```yaml
source:
  scope: my-scope
  type: gsuite
  config:     
    service_account_json: "XXX"
    user_email: meteor@gotocompany.com
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `user_email` | `string` | `meteor@gotocompany.com` | User email authorized to access the APIs | *required* |
| `service_account_json` | `string` | `{"type": "service_account","project_id": "XXXXXX","private_key_id": "XXXXXX","private_key": "XXXXXX","client_email": "XXXXXX","client_id": "XXXXXX","auth_uri": "https://accounts.google.com/o/oauth2/auth","token_uri": "https://oauth2.googleapis.com/token","auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url": "XXXXXX"}` | Service Account JSON object | *required* |

## Outputs

| Field                   | Sample Value                                         |
|:------------------------|:-----------------------------------------------------|
| `email`                 | `doe.john@gmail.com`                                 |
| `full_name`             | `Jon Doe`                                            |
| `status`                | `suspended`                                          |
| `attributes`            | `{"aliases":"john.doe@gotocompany.com","custom_schemas":{},`|
|                         |  `"org_unit_path":"/","organizations":`              |
|                         | `[{"costCenter": "gotocompany"}],`                          |
|                         |  `"relations":[{"type":"manager",`                   |
|                         |  `"value":"john.lee@gotocompany.com"}]}`                    |

### Notes
 - The service account must have a [delegated domain wide authority](https://developers.google.com/admin-sdk/directory/v1/guides/delegation#delegate_domain-wide_authority_to_your_service_account)
 - User Email : Only users with access to the Admin APIs can access the Admin SDK Directory API, therefore your service account needs to impersonate one of those users to access the Admin SDK Directory API.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
