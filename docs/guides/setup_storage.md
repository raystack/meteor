# Setup Storage

Meteor uses object storage to persist created recipes which can be specified via `RECIPE_STORAGE_URL` config value ([config details](../reference/configuration.md#-recipe_storage_url)).

Following table represents required variables to authenticate for different backend stores

| Backend store | URL scheme | ENV variables needed to authenticate     | Description                |
| :-------- | :------- | :---------- | :------------------------- |
| Google cloud storage | `gs://` | `GOOGLE_APPLICATION_CREDENTIALS` | Value should point to service account key file. Refer [here](https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication) to generate key file |
| Azure cloud storage | `azblob://` | `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `AZURE_STORAGE_SAS_TOKEN` | `AZURE_STORAGE_ACCOUNT` is required, along with one of the other two. refer [here](https://gocloud.dev/howto/blob/#azure) for more details |
| AWS cloud storage | `s3://` | refer [here](https://docs.aws.amazon.com/sdk-for-go/api/aws/session/) for list of envs needed | [reference](https://gocloud.dev/howto/blob/#s3) |
| Local storage | `file://` |none | No extra envs required |
| In memory storage | `mem://` | none | No Extra envs required |