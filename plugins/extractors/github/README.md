# GitHub

Extract metadata from a GitHub organisation including users, repositories, teams, and documents.

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
      - documents
    # docs configures document extraction (only used when "documents" is in extract).
    docs:
      repos: ["meteor"]
      paths: ["docs"]
      pattern: "*.md"
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `org` | `string` | Yes | Name of the GitHub organisation. |
| `token` | `string` | Yes | GitHub API access token. |
| `extract` | `[]string` | No | Entity types to extract. Defaults to all: `users`, `repositories`, `teams`, `documents`. |
| `docs.repos` | `[]string` | No | Repositories to scan for documents. Defaults to all org repos. |
| `docs.paths` | `[]string` | No | Directory paths to scan within each repo. Defaults to `["docs"]`. |
| `docs.pattern` | `string` | No | Glob pattern to match files. Defaults to `"*.md"`. |

## Entities

The extractor emits four entity types and their relationships as edges.

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

### Entity: `document`

| Field                    | Sample Value                                          |
| :----------------------- | :---------------------------------------------------- |
| `urn`                    | `urn:github:my-org:document:abc123...`                |
| `name`                   | `getting-started`                                     |
| `properties.path`        | `docs/getting-started.md`                             |
| `properties.file_name`   | `getting-started.md`                                  |
| `properties.content`     | `# Getting Started\n...`                              |
| `properties.html_url`    | `https://github.com/raystack/meteor/blob/main/docs/...` |
| `properties.repo`        | `raystack/meteor`                                     |
| `properties.size`        | `2048`                                                |
| `properties.sha`         | `abc123def456...`                                     |

### Edges

| Type         | Source       | Target       | Description                            |
| :----------- | :----------- | :----------- | :------------------------------------- |
| `member_of`  | `user`       | `org`        | User is a member of the organisation   |
| `owned_by`   | `repository` | `user`       | Repository is owned by a user          |
| `member_of`  | `user`       | `team`       | User is a member of a team             |
| `belongs_to` | `document`   | `repository` | Document belongs to a repository       |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
