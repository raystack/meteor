# Meteor
Metadata Collector API

## Version: 0.1.0

### /v1/recipes

#### POST
##### Summary

create a recipe

##### Description

API to create a new recipe

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| payload | body |  | Yes | [CreateRecipeRequest](#createreciperequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 201 | OK | string |
| 400 | validation error | string |
| 409 | duplicate recipe name | string |
| 500 | internal server error | string |

### /v1/run

#### POST
##### Summary

run a recipe

##### Description

API to run an existing recipe

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| payload | body |  | Yes | [RunRecipeRequest](#runreciperequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [Run](#run) |
| 400 | validation error | string |
| 404 | recipe not found | string |
| 500 | internal server error | string |

### /v1/secrets

#### PUT
##### Summary

upsert a secret

##### Description

API to upsert a secret

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| payload | body |  | Yes | [UpsertSecretRequest](#upsertsecretrequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | string |
| 400 | validation error | string |
| 500 | internal server error | string |

### Models

#### CreateRecipeRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |
| source | object |  | Yes |
| processors | [ object ] |  | No |
| sinks | [ object ] |  | Yes |

#### RunRecipeRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| recipe_name | string |_Example:_ `"kafka-production"` | Yes |

#### UpsertSecretRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string | _Example:_ `"mysql-cred"` | Yes |
| data | object |  | Yes |

#### Recipe

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | No |
| source | [Source](#source) |  | No |
| processors | [ [Processor](#processor) ] |  | No |
| sinks | [ [Sink](#sink) ] |  | No |

#### Source

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| type | string |  | Yes |
| config | object |  | No |

#### Sink

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |
| config | object |  | No |

#### Processor

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| name | string |  | Yes |
| config | object |  | No |

#### Run

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| recipe | [Recipe](#recipe) |  | No |
| tasks | object |  | No |
