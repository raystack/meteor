# User

| `keys` | Data type | json |
| :--- | :---- | :-- |
|Urn  | string | `urn` |
|Source| string | `source` |
|Email | string | `email` |
|Username | string | `username` |
|FirstName | string | `first_name` |
|LastName | string | `last_name` |
|FullName | string | `full_name` |
|DisplayName | string | `display_name`|
|Title |string | `title` |
|IsActive | bool | `is_active` |
| ManagerEmail |string| `manager_email` |
| Profiles | []*Profile | `profiles` |
| Memberships |[]*Membership | `memberships` |
|Tags | *facets.Tags | `tags` |
|Custom | *facets.Custom | `custom` |
|Timestamps | *common.Timestamp | `timestamps` |
|Event | *common.Event | `event` |
