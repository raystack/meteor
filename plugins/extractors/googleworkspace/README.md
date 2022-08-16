# Google Workspace

## Usage

```yaml
source:
  scope: my-scope
  type: googleworkspace
  config:     
    service_account_json: "XXX"
    user_email: meteor@odpf.com
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `user_email` | `string` | `meteor@odpf.com` | User email authorized to access the APIs | *required* |
| `service_account_json` | `string` | `{"type": "service_account","project_id": "odpf-project","private_key_id": "XXXXXXXXXXXXXXXX","private_key": "-----BEGIN PRIVATE KEY-----\nXXXXXXXX\n-----END PRIVATE KEY-----\n","client_email": "meteor-sa@odpf-project.iam.gserviceaccount.com","client_id": "XXXXXXXXXXXXXXXX","auth_uri": "https://accounts.google.com/o/oauth2/auth","token_uri": "https://oauth2.googleapis.com/token","auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40odpf-project.iam.gserviceaccount.com"}` | Service Account JSON object | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `john.doe@gmail.com` |
| `resource.name` | `John Doe` |
| `email` | `john.doe@gmail.com` |
| `full_name` | `John Doe` |
| `status` | `not suspended` |
| `properties` | `{"attributes":{"aliases":"doe.john@gmail.com,john.doe0@gmail.com","manager":"christian.aristika@gmail.com","org_unit_path":"/"}}`

### Notes
 - The service account must have a [delegated domain wide authority](https://developers.google.com/admin-sdk/directory/v1/guides/delegation#delegate_domain-wide_authority_to_your_service_account)
 - User Email : Only users with access to the Admin APIs can access the Admin SDK Directory API, therefore your service account needs to impersonate one of those users to access the Admin SDK Directory API.
 
## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
