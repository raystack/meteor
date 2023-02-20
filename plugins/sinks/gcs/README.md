# GCS

Sinks json data to a file as ndjson format in Google Cloud Storage bucket

## Usage
```yaml
sinks:
  - name: gcs
    config:
     project_id: google-project-id
     url: gcs://bucket_name/target_folder
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

## Config Definition

| Key | Value | Example | Description |  |
| :-- | :---- | :------ | :---------- | :-- |
|`project_id` | `string` | `google-project-id` | Google Cloud Storage Project ID  | *required*|
| `url` | `string` | `gcs://bucket_name/target_folder` | the URL with bucket name and path of the folder with format `gcs://<bucket_name>/<optional_folder_path>` | *required* |
| `object_prefix` | `string` | `github-users` | the .ndjson file name prefix where json data will be inserted with timestamp </b></b> Note: If prefix is not provided, the output data will be put in a `timestamp.ndjson` file in the provided path. Otherwise in the given example the output file will be `github-users-timestamp.ndjson`| *optional* |
| `service_account_base64` | `string` | `ewog....fQo=` |  Service Account Key in base64 encoded string. Takes precedence over `service_account_json` value | *optional* |
| `service_account_json` | `string` | `{"private_key": .., "private_id": ...}` |   Service Account Key in JSON string | *optional* |


## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
