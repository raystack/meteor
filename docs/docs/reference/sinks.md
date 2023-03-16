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

Upload metadata to a given `type` in [Compass](https://github.com/goto/meteor/tree/cb12c3ecf8904cf3f4ce365ca8981ccd132f35d0/docs/reference/github.com/goto/compass/README.md). Request will be sent via HTTP to a given host.

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
        format: "yaml"
```
## GCS

`Google Cloud Storage`

Sinks json data to a file as ndjson format in Google Cloud Storage bucket

```yaml
sinks:
  - name: gcs
    config:
     project_id: google-project-id
     url:  gcs://bucket_name/target_folder
     object_prefix : github-users
     service_account_base64: <base64 encoded service account key>
     service_account_json:
      {
        "type": "service_account",
        "private_key_id": "xxxxxxx",
        "private_key": "xxxxxxx",
        "client_email": "xxxxxxx",
        "client_id": "xxxxxxx",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "xxxxxxx",
        "client_x509_cert_url": "xxxxxxx",
      }
```

## http

`http`

Sinks metadata to a http destination as per the config defined.

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

## Stencil

`stencil`

Upload metadata of a given schema `format` in the existing `namespace_id` present in [Stencil](https://github.com/goto/meteor/tree/cb12c3ecf8904cf3f4ce365ca8981ccd132f35d0/docs/reference/github.com/goto/stencil/README.md). Request will be sent via HTTP to a given host.

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

## Shield

`shield`

Upsert users to shield service running at a given 'host'. Request will be sent via GRPC.

```yaml
sinks:
  name: shield
  config:
    host: shield.com
    headers:
      X-Shield-Email: meteor@gotocompany.com
      X-Other-Header: value1, value2
```

_**Notes**_

Compass' Type requires certain fields to be sent, hence why `mapping` config is needed to map value from any of our metadata models to any field name when sending to Compass. Supports getting value from nested fields.
