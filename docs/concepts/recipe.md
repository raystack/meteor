# Recipe

A recipe is an instruction on how to fetch, process and sink metadata.

Recipe is basically a yaml file with the information below.
```yaml
name: main-kafka-production # unique recipe name as an ID
source: # required - for fetching input from sources
 type: kafka # required - collector to use (e.g. bigquery, kafka)
 config:
   broker: "http://localhost:9092"
sinks: # required - at least 1 sink defined
 - name: http
   config:
     method: POST
     url: "https://example.com/metadata"
processors: # optional - metadata processors
 - name: topic-proto-populator
```

## Recipe Management
There are two ways to register recipe to Meteor.

### via .yaml files
This is the simplest one with just by creating a `*_recipe.yaml` file and inside `/recipes` folder.

On startup, Meteor will load all those recipes inside `/recipes` folder and upsert them. Be noted that this process will replace all the changes made to the recipe if it were updated via Meteor API.

Deleting the `.yaml` file alone will not delete the recipe, you also need to delete it via Meteor API.

### via Meteor REST API
Another approach is by using Meteor API that is exposed via HTTP.

| Actions | Path | Payload | 
| ------- | ---- | ------- |
| Create a Recipe | [POST] /v1/recipes | [Recipe Payload](#recipe-payload) |
| Update a Recipe | [PUT] /v1/recipes/{name} | [Recipe Payload](#recipe-payload) |
| Delete a Recipe | [DELETE] /v1/recipes/{name}  | -        |

#### Recipe Payload
```json
{
 "name": "new-recipe",
 "source": {},
 "processors": [
   {"name": "proc-name"}
 ],
 "sinks": [
   {"name": "kafka", "config": {}}
 ]
}
```

## Recipe Storage
By default, recipes will be persisted locally inside `/_recipes` folder as .yaml files. But it can be configurable to external storage such as GCS or AWS S3.

## Running a recipe

A recipe itself will not do anything, it needs to be run to actually do something. To run a recipe, you can hit Meteor API with the recipe name and optionally provide an input to be processed with the recipe.

Running a recipe requires an initial data to work with before processing anything. There are two ways to get the initial data, using Meteor’s built in extractor (via `source` field) or by passing it when creating a job as an input.

`[POST] /v1/run`
```json
{
    "name": "{RECIPE_NAME}",
    "input": []
}
```
