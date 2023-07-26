# Metadata Models

Metadata models are structs in which metadata of a certain kind will be
extracted in order to mainatain the integrity across similar data sources. For
e.g, MySQL and Postgres are supposed to provide similar struct for metadata
since both are SQL based databases. Currently meteor provides the extracted
metadata as one of the following metadata models:

* [`Bucket`](bucket.pb.gp)
* [`Chart`](chart.pb.go)
* [`Dashboard`](dashboard.pb.go)
* [`Group`](group.pb.go)
* [`Job`](job.pb.go)
* [`Table`](table.pb.go)
* [`Topic`](topic.pb.go)
* [`User`](user.pb.go)
* [`FeatureTable`](feature_table.pb.go)
* [`Application`](application.pb.go)
* [`Model`](model.pb.go)

While adding an extractor one needs to provide metadata supported by these
models. If you want some other data model added to the list feel free to raise a
issue. Please refer [docs](../../../docs/docs/reference/metadata_models.md) for
easier reference of how data models are being used.
