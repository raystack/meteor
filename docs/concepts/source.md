# Source

When the source field is defined, Meteor will extract data from a metadata source using details defined in the field. Extractor can be defined using the `type` field.

## Sample usage in a recipe
```yaml
name: sample-recipe
source:
 - name: kafka
   config:
     broker: broker:9092
```

## Extractors
### BigQuery Dataset
```yaml
source:
  type: bigquery-dataset
  config:
    project_id: "sample-project" # required
    credentials: "./path-to-credentials.json" # required
```
### BigQuery Table
```yaml
source:
  type: bigquery-table
  config:
    project_id: "sample-project" # required
    credentials: "./path-to-credentials.json" # required
```
### Kafka
```yaml
source:
  type: kafka
  config:
    broker: broker:9092 # required
```
### HTTP
```yaml
source:
  type: http
  config:
    method: GET # optional - default to GET
    url: https://custom-metadata.io/sample # required
    headers: # optional
      authorization: "Bearer oauth-token"
```
