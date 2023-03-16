# Configuration

This page contains references for all the application configurations for Meteor.

## Table of Contents

* [Generic](configuration.md#generic)

## Generic

Meteor's required variables to start using it.

### `PORT`

* Example value: `8080`
* Type: `optional`
* Default: `3000`
* Port to listen on.

### `RECIPE_STORAGE_URL`

* Example value: `s3://my-bucket?region=us-west-1`
* Type: `optional`
* Default: `mem://`
* Object storage URL to persist recipes. Can be a gcs, an aws bucket or even a local folder. Check this [guide](https://github.com/goto/meteor/tree/27f39fe2f83b657d4ecb9eb2c2a8794c6c0671b6/docs/guides/setup_storage.md) for url format and how to setup each available storage.

