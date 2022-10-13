# Sink

`sinks` are used to define the medium of consuming the metadata being extracted. You need to specify **at least one** sink or can specify multiple sinks in a recipe, this will prevent you from having to create duplicate recipes for the same job. The given examples show you its correct usage if your sink is `http` and `kafka`.

## Writing `sinks` part of your recipe

```yaml
sinks: # required - at least 1 sink defined
  - name: http
    config:
      method: POST
      url: "https://example.com/metadata"
  - name: kafka
    config:
      brokers: localhost:9092
      topic: "target-topic"
      key_path:
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
        path: "./dir/sample.yaml"
        format: "yaml"
```

Sinks metadata to a file in `json/yaml` format as per the config defined.

* **http**

```yaml
sinks:
  name: http
  config:
    method: POST
    success_code: 200
    url: https://compass.com/v1beta1/asset
    headers:
      Header-1: value11,value12
```

Sinks metadata to a http destination as per the config defined.

* **Stencil**

```yaml
sinks:
  name: stencil
  config:
    host: https://stencil.com
    namespace_id: myNamespace
    schema_id: mySchema
    format: json
    send_format_header: false
```

Upload metadata of a given schema `format` in the existing `namespace_id` present in Stencil. Request will be sent via HTTP to a given host.

## Upcoming sinks

* HTTP
* Kafka

## Serializer

By default, metadata would be serialized into JSON format before sinking. To send it using other formats, a serializer needs to be defined in the sink config.

## Custom Sink

Meteor has built-in sinks like Kafka and HTTP which users can just use directly. We will also allow creating custom sinks for DRY purposes.

It will be useful if you can find yourself sinking multiple metadata sources to one place.

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
