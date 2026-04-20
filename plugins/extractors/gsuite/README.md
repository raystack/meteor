# Google Workspace

Extract user metadata from Google Workspace (G Suite).

## Usage

```yaml
source:
  name: gsuite
  scope: my-org
  config:
    service_account_json: '{"type":"service_account","project_id":"...","private_key":"..."}'
    user_email: admin@example.com
```

## Configuration

| Key | Type | Required | Description |
|:----|:-----|:---------|:------------|
| `service_account_json` | `string` | Yes | Service Account JSON credential object. |
| `user_email` | `string` | Yes | Email of a user with Admin SDK Directory API access to impersonate. |

### Notes

- The service account must have [domain-wide delegation](https://developers.google.com/admin-sdk/directory/v1/guides/delegation#delegate_domain-wide_authority_to_your_service_account) enabled.
- The `user_email` must belong to a user with access to the Admin APIs.

## Entities

- **Type:** `user`
- **URN format:** `urn:gsuite:{scope}:user:{primary_email}`

### Properties

| Property | Type | Description |
|:---------|:-----|:------------|
| `properties.email` | `string` | Primary email address. |
| `properties.full_name` | `string` | Full name of the user. |
| `properties.status` | `string` | `"suspended"` if the user is suspended; omitted otherwise. |
| `properties.aliases` | `string` | Comma-separated list of email aliases. |
| `properties.org_unit_path` | `string` | Organizational unit path. |
| `properties.organizations` | `[]any` | List of organization records from Google Workspace. |
| `properties.relations` | `[]any` | List of relation records (e.g. manager). |
| `properties.custom_schemas` | `map[string]any` | Custom schema data from Google Workspace. |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-extractor) for information on contributing to this module.
