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

`source.type` defines what source and extractor to use to fetch the metadata. ([Extractor list](../guides/extractors.md))

`source.config` is an optional field. Each extractor may require different configuration. More info can be found [here](../guides/extractors.md).

`sinks` is a required field containing list of sinks to send the metadata to. Learn more [here](./sink.md).

`sinks[].name` defines which sink you want to use.

`sinks[].config` is an optional field. Each sink may require different configuration.

`processors` is an optional field containing list of processors. Learn more about processor [here](./processor.md).

`processors[].name` defines what processor to use.

`processors[].config` is an optional field. Each processor may require different configuration.
