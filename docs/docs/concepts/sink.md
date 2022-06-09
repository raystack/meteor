# Sink

`sinks` are used to define the medium of consuming the metadata being extracted. You need to specify **atleast one** sink or can specify multiple sinks in a recipe, this will prevent you from having to create duplicate recipes for the same job. The given examples shows you its correct usage if your sink is `http` and `kafka`.

## Writing `sinks` part of your recipe

```yaml
sinks: # required - at least 1 sink defined
  - name: http
    config:
      method: POST
      url: "https://example.com/metadata"
  - name: kafka
    config:
      broker: localhost:9092
      topic: "target-topic"
      serializer:
        type: proto
        key: SampleLogKey
        value: SampleLogMessage
```

| key | Description | requirement |
| :--- | :--- | :--- |
| `name` | contains the name of sink | required |
| `config` | different sinks will require different configuration | optional, depends on sink |

## Available Sinks

* **Console**

```yaml
name: sample-recipe
sinks:
  - name: console
```

Print metadata to stdout.

* **File**

```yaml
sinks:
    name: file
    config:
        format: "yaml/json"
        filename: "postgres_server"
        output-dir: path/to/folder
```

Sinks metadata to a file in `json/yaml` format as per the config defined.

## Upcoming sinks

* HTTP
* Kafka

## Serializer

By default, metadata would be serialized into JSON format before sinking. To send it using other formats, a serializer needs to be defined in the sink config.

## Custom Sink

Meteor has built-in sinks like Kafka and HTTP which users can just utilise directly. We will also allow creating custom sinks for DRY purposes.

It will be useful if you can find yourself sinking multiple metadata source to one place.

### Sample Custom Sink

* _central\_metadata\_store\_sink.yaml_

```yaml
name: central-metadata-store # unique sink name as an ID
sink:
  - name: http
    config:
      method: PUT
      url: "https://metadata-store.com/metadata"
```

More info about available sinks can be found [here](../reference/sinks.md).
