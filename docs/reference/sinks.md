# Sinks

## Console

`console`

Print data to stdout.

### Sample usage

```yaml
sinks:
 - name: console
```

## Columbus

`columbus`

Upload metadata to a given `type` in [Columbus](github.com/odpf/columbus). Request will be send via HTTP to given host.

### Sample usage

```yaml
sinks:
 - name: columbus
   config:
     host: https://columbus.com
     type: sample-columbus-type
     mapping:
       ModelFieldName: "new_fieldname"
       Urn: "id"
       Name: "displayName"
```
***Notes***

Columbus' Type requires certain fields to be sent, hence why `mapping` config is needed to map value from any of our metadata models to any field name when sending to Columbus.
