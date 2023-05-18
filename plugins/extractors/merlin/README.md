# merlin

Extractor for Machine Learning(ML) Models from [Merlin][merlin].

The extractor uses the REST API exposed by Merlin to extract models. The REST
API has been documented with Swagger and can be seen [here][merlin-swagger].

## Usage

```yaml
source:
  name: merlin
  scope: staging
  config:
    url: my-company.com/api/merlin/
    service_account_base64: |
      ____base64_encoded_service_account_credentials____
```

## Inputs

| Key                      | Value    | Example                                  | Description                                                               | Required? |
|:-------------------------|:---------|:-----------------------------------------|:--------------------------------------------------------------------------|:----------|
| `url`                    | `string` | `my-company.com/api/merlin/`             | Merlin's API base URL                                                     | ✅         |
| `service_account_base64` | `string` | `____BASE64_ENCODED_SERVICE_ACCOUNT____` | Service Account credentials in base64 encoded string.                     | ❌         |
| `request_timeout`        | `string` | `10s`                                    | Timeout for HTTP requests to Merlin API                                   | ❌         |
| `worker_count`           | `int`    | `5`                                      | Number of workers to spawn for extracting projects parallely from Merlin. | ❌         |

### Notes

- Leaving `service_account_base64` blank will default
  to [Google's default authentication][google-default-auth]. It is recommended
  if Meteor instance runs inside the same Google Cloud environment as the
  BigQuery project.

## Outputs

The models are mapped to an [`Asset`][proton-asset] with model specific metadata
stored using [`Model`][proton-model]. Please refer the proto definitions for
more information.

A single model asset includes all the active model versions. A model version is
considered active if it has an endpoint.

| Field                              | Value                                                                  | Sample Value                                                  |
|:-----------------------------------|:-----------------------------------------------------------------------|:--------------------------------------------------------------|
| `resource.urn`                     | `urn:merlin:{scope}:model:{model.project_name}.{model.name}`           | `urn:merlin:staging:model:food.restaurant-image`              |
| `resource.name`                    | `{model.name}`                                                         | `tensorflow-sample`                                           |
| `resource.service`                 | `merlin`                                                               | `merlin`                                                      |
| `resource.type`                    | `model`                                                                | `model`                                                       |
| `resource.url`                     | `{model.endpoints[0].url}`                                             | `tensorflow-sample.integration-test.models.mycompany.com`     |
| `namespace`                        | `{project.name}`                                                       | `integration-test`                                            |
| `flavor`                           | `model.type`                                                           | `pyfunc`                                                      |
| `versions`                         | [`[]ModelVersion`](#modelversion)                                      |                                                               |
| `attributes.merlin_project_id`     | `project.id`                                                           | `23`                                                          |
| `attributes.mlflow_experiment_id`  | `model.mlflow_experiment_id`                                           | `721`                                                         |
| `attributes.mlflow_experiment_url` | `model.mlflow_url`                                                     | `http://mlflow.mycompany.com/#/experiments/721`               |
| `attributes.endpoint_urls[]`       | `model.endpoints[].url`                                                | `["tensorflow-sample.integration-test.models.mycompany.com"]` |
| `create_time`                      | `model.created_at`                                                     | `2021-03-01T18:42:50.564685Z`                                 |
| `update_time`                      | `model.updated_at`                                                     | `2022-01-27T10:21:26.121941Z`                                 |
| `resource.owners[].urn`            | `{project.administrators[]}`                                           | `giga.chad@knowyourmeme.com`                                  |
| `resource.owners[].email`          | `{project.administrators[]}`                                           | `giga.chad@knowyourmeme.com`                                  |
| `lineage.upstreams`                | [`[]Resource` upstreams](#resource-upstreams)                          |                                                               |
| `resource.labels`                  | `{"team": {project.team}, "stream": {project.stream} + project.labels` | `{"stream": "relevance","team": "search"}`                    |

### `ModelVersion`

A [`ModelVersion`][proton-modelversion] is used to represent each combination of
Merlin model's version and it's 'endpoint' destination. A single model version
will have an 'endpoint' for each environment it is deployed in. Please refer the
proto definitions for more information.

| Field                             | Value                                  | Sample Value                                                                                       |
|:----------------------------------|:---------------------------------------|:---------------------------------------------------------------------------------------------------|
| `status`                          | `model_version.status`                 | `running`                                                                                          |
| `version`                         | `model_version.id`                     | `11`                                                                                               |
| `attributes.endpoint_id`          | `endpoint.id`                          | `187`                                                                                              |
| `attributes.mlflow_run_id`        | `model_version.mlflow_run_id`          | `3c7067f3770441ebbd66a0dce91b8724`                                                                 |
| `attributes.mlflow_run_url`       | `model_version.mlflow_url`             | `http://mlflow.mycompany.com/#/experiments/721/runs/3c7067f3770441ebbd66a0dce91b8724`              |
| `attributes.endpoint_url`         | `endpoint.url`                         | `tensorflow-sample.integration-test.models.mycompany.com`                                          |
| `attributes.version_endpoint_url` | `version_endpoint.url`                 | `http://tensorflow-sample-11.integration-test.models.mycompany.com/v1/models/tensorflow-sample-11` |
| `attributes.monitoring_url`       | `version_endpoint.monitoring_url`      | `https://grafana.mycompany.com/graph/d/z9MBKR1Az/model-version-dashboard?params`                   |
| `attributes.message`              | `version_endpoint.message`             | `timeout creating inference service`                                                               |
| `attributes.environment_name`     | `endpoint.environment_name`            | `aws-staging`                                                                                      |
| `attributes.deployment_mode`      | `version_endpoint.deployment_mode`     | `serverless`                                                                                       |
| `attributes.service_name`         | `version_endpoint.service_name`        | `tensorflow-sample-11-predictor-default.integration-test.models.mycompany.com`                     |
| `attributes.env_vars`             | `version_endpoint.env_vars`            | `{"INIT_HEAP_SIZE_IN_MB": "2250","WORKERS": "1"}`                                                  |
| `attributes.transformer`          | `version_endpoint.transformer`         | Attributes including `transformer.{enabled, type, image, command, args, env_vars}`                 |
| `attributes.weight`               | `endpoint.rule.destinationsp[].weight` | `100`                                                                                              |
| `labels`                          | `model_version.labels`                 |                                                                                                    |
| `create_time`                     | `model_version.created_at`             | `2022-11-13T07:21:07.888150Z`                                                                      |
| `update_time`                     | `model_version.updated_at`             | `2022-11-13T07:21:07.888150Z`                                                                      |

### `Resource` upstreams

The extractor currently has limited support for constructing the upstreams for
Model that utilises the env vars for `standard` transformer. It parses the
feature table specs that specify the project name and feature table name of the
[CaraML Store][caraml-store] Feature Table from the env vars. This information
is used to construct the upstreams for the model.

| Field     | Value                                                          | Sample Value                                               |
|:----------|:---------------------------------------------------------------|:-----------------------------------------------------------|
| `urn`     | `urn:caramlstore:{scope}:feature_table:{ft.project}.{ft.name}` | `urn:kafka:int-kafka.yonkou.io:topic:staging_30min_demand` |
| `type`    | `feature_table`                                                | `topic`                                                    |
| `service` | `caramlstore`                                                  | `kafka`                                                    |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor)
for information on contributing to this module.

[merlin]: https://github.com/gojek/merlin

[merlin-swagger]: https://github.com/gojek/merlin/blob/v0.24.0/swagger.yaml

[google-default-auth]: https://cloud.google.com/docs/authentication/production#automatically

[proton-asset]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/asset.proto#L14

[proton-model]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/model.proto#L73

[proton-modelversion]: https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/model.proto#L31

[caraml-store]: https://github.com/caraml-dev/caraml-store
