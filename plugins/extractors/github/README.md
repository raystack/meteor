# GitHub

Extract metadata from a GitHub organisation including users, repositories, and teams.

## Usage

```yaml
source:
  name: github
  scope: my-github-org
  config:
    org: raystack
    token: github_token
    extract:
      - users
      - repositories
      - teams
```

## Inputs

| Key       | Value      | Example      | Description                                                        |            |
| :-------- | :--------- | :----------- | :----------------------------------------------------------------- | :--------- |
| `org`     | `string`   | `raystack`   | Name of the GitHub organisation                                    | _required_ |
| `token`   | `string`   | `ghp_xxx`    | GitHub API access token                                            | _required_ |
| `extract` | `[]string` | `["users"]`  | Entity types to extract. Defaults to all: users, repositories, teams | _optional_ |

## Outputs

The extractor emits three entity types and their relationships as edges.

### Entity: `user`

| Field                | Sample Value                              |
| :------------------- | :---------------------------------------- |
| `urn`                | `urn:github:my-org:user:MDQ6VXNl...`      |
| `name`               | `Ravi Suhag`                              |
| `properties.email`   | `suhag.ravi@gmail.com`                    |
| `properties.username`| `ravisuhag`                               |
| `properties.full_name` | `Ravi Suhag`                            |
| `properties.company` | `Raystack`                                |
| `properties.location`| `Bangalore`                               |
| `properties.bio`     | `Engineer`                                |
| `properties.status`  | `active`                                  |

### Entity: `repository`

| Field                        | Sample Value                              |
| :--------------------------- | :---------------------------------------- |
| `urn`                        | `urn:github:my-org:repository:R_kgDO...`  |
| `name`                       | `meteor`                                  |
| `properties.full_name`       | `raystack/meteor`                         |
| `properties.language`        | `Go`                                      |
| `properties.visibility`      | `public`                                  |
| `properties.default_branch`  | `main`                                    |
| `properties.archived`        | `false`                                   |
| `properties.stargazers`      | `42`                                      |
| `properties.forks`           | `5`                                       |
| `properties.topics`          | `["metadata", "golang"]`                  |

### Entity: `team`

| Field                    | Sample Value                              |
| :----------------------- | :---------------------------------------- |
| `urn`                    | `urn:github:my-org:team:T_lADO...`        |
| `name`                   | `Backend`                                 |
| `properties.slug`        | `backend`                                 |
| `properties.description` | `Backend engineering team`                |
| `properties.privacy`     | `closed`                                  |
| `properties.permission`  | `push`                                    |

### Edges

| Type        | Source       | Target     | Description                          |
| :---------- | :----------- | :--------- | :----------------------------------- |
| `member_of` | `user`       | `org`      | User is a member of the organisation |
| `owned_by`  | `repository` | `user`     | Repository is owned by a user        |
| `member_of` | `user`       | `team`     | User is a member of a team           |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
