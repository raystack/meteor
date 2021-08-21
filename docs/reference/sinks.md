# Sinks

## Console

`console`

Print data to stdout.

### Sample usage of console sink

```yaml
sinks:
 - name: console
```

## Columbus

`columbus`

Upload metadata to a given `type` in [Columbus](github.com/odpf/columbus). Request will be send via HTTP to given host.

### Sample usage of columbus sink

```yaml
sinks:
 - name: columbus
   config:
     host: https://columbus.com
     type: sample-columbus-type
```
