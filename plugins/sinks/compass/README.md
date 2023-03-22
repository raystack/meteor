# Compass

Compass is a search and discovery engine built for querying application deployments, datasets and meta resources. It can also optionally track data flow relationships between these resources and allow the user to view a representation of the data flow graph.

## Usage

```yaml
sinks:
  name: compass
  config:
    host: https://compass.com
    headers:
      compass-User-Email: meteor@odpf.io
      Header-1: value11,value12
    labels:
      myCustom: $attributes.myCustomField
      sampleLabel: $labels.sampleLabelField
```

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
