# Recipe

A recipe is an instruction on how to fetch, process and sink metadata.

Recipe is a yaml file with the below information.

```yaml
name: main-kafka-production # unique recipe name as an ID
source: # required - for fetching input from sources
 type: kafka # required - collector to use (e.g. bigquery, kafka)
 config:
   broker: "localhost:9092"
sinks: # required - at least 1 sink defined
 - name: http
   config:
     method: POST
     url: "https://example.com/metadata"
processors: # optional - metadata processors
 - name: metadata
   config:
     foo: bar
     bar: foo
```

`name` contains recipe name.

`source` is a required field to tell Meteor how and where to extract the data from. Learn more [here](./source.md).

`source.type` defines what source and extractor to use to fetch the metadata. ([Extractor list](../reference/extractors.md))

`source.config` is an optional field. Each extractor may require different configuration. More info can be found [here](../reference/extractors.md).

`sinks` is a required field containing list of sinks to send the metadata to. Learn more [here](./sink.md).

`sinks[].name` defines which sink you want to use. ([Sink list](../reference/sinks.md))

`sinks[].config` is an optional field. Each sink may require different configuration. More info can be found [here](../reference/sinks.md).

`processors` is an optional field containing list of processors. Learn more about processor [here](./processor.md).

`processors[].name` defines what processor to use. ([Processor list](../reference/processors.md))

`processors[].config` is an optional field. Each processor may require different configuration. More info can be found [here](../reference/processors.md).

## Dynamic recipe value

Meteor reads recipe using [go template](https://golang.org/pkg/text/template/), which means you can put a variable instead of static value in a recipe.
Environment variables with prefix `METEOR_` will be used as the template data for the recipe.

*recipe-with-variable.yaml*

```yaml
name: sample-recipe
source:
  type: mongodb
  config:
    user_id: {{ .mongodb_user }}
    password: "{{ .mongodb_pass }}" # wrap it with double quotes to make sure value is read as a string
```

sample usage

```shell
#setup environment variables
> export METEOR_MONGODB_USER=admin
> export METEOR_MONGODB_PASS=1234
#run the recipe
> meteor run recipe-with-variable.yaml
```
