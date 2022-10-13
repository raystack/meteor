# Example

## Running recipe with dynamic variable

```cli
export METEOR_KAFKA_BROKER=localhost:9092
meteor run ./kafka-console.yaml
```

This recipe tells meteor to fetch kafka metadata from broker defined by `METEOR_KAFKA_BROKER` envvar which will be translated to `kafka_broker` in recipe. ([learn more about dynamic recipe](../concepts/recipe.md#dynamic-recipe-value))

[enrich](../reference/processors.md#enrich) processor is also added in the `processors` list in the recipe which will enrich metadata fetched with keyvalues defined in the config.

At last, Meteor will output the results to the `sinks` given in the recipe which in this case is to [console](../reference/sinks.md#console) only.
