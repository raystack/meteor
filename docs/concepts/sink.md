# Sink

You can specify multiple sinks in a recipe, this will prevent you from having to create duplicate recipes for the same metadata extraction.

## Sample usage in a recipe
```yaml
name: sample-recipe
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

## Available Sinks
### Console
```yaml
name: sample-recipe
sinks:
  - name: console
```
Print metadata to stdout.

## Upcoming sinks
- HTTP
- Kafka

## Serializer
By default, metadata would be serialized into JSON format before sinking. To send it using other formats, a serializer needs to be defined in the sink config.

## Custom Sink
Meteor will have built-in sinks like Kafka and HTTP where users can just utilise directly. We will also allow creating custom sinks for DRY purposes.

This will be useful if you find yourself sinking multiple metadata source to one place.

### Sample Custom Sink
*central_metadata_store_sink.yaml*
```yaml
name: central-metadata-store # unique sink name as an ID
sink:
  - name: http
    config:
      method: PUT
      url: "https://metadata-store.com/metadata"
```
