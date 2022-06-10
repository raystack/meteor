# Sinks

## Console

`console`

Print data to stdout.

### Sample usage of console sink

```yaml
sinks:
 - name: console
```

## Compass

`compass`

Upload metadata to a given `type` in [Compass](https://github.com/odpf/meteor/tree/cb12c3ecf8904cf3f4ce365ca8981ccd132f35d0/docs/reference/github.com/odpf/compass/README.md). Request will be send via HTTP to given host.

### Sample usage of compass sink

```yaml
sinks:
 - name: compass
   config:
     host: https://compass.com
     type: sample-compass-type
     mapping:
       new_fieldname: "json_field_name"
       id: "resource.urn"
       displayName: "resource.name"
```

## File

`file`

Sinks metadata to a file in `json/yaml` format as per the config defined.

```yaml
sinks:
    name: file
    config:
        path: "./dir/sample.yaml"
```

_**Notes**_

Compass' Type requires certain fields to be sent, hence why `mapping` config is needed to map value from any of our metadata models to any field name when sending to Compass. Supports getting value from nested fields.
