# Metadata Models

Metadata models are structs in which metadata of a certain kind will be extracted in order to mainatain the integrity across similar data sources.
For e.g, MySQL and Postgres are supposed to provide similar struct for metadata since both are SQL based databases.
Currently meteor provides the extracted metadata as one of the following metadata models:

* [Bucket](Bucket.pb.gp)
* [Chart](Chart.pb.go)
* [Dashboard](Dashboard.pb.go)
* [Group](Group.pb.go)
* [Job](Job.pb.go)
* [Table](Table.pb.go)
* [Topic](Topic.pb.go)
* [User](User.pb.go)

While adding an extractor one needs to provide metadata supported by these models.
If you want some other data model added to the list feel free to raise a issue.
Please refer [docs](../../../docs/data%20models/README.md) for easier reference of how data models are being used.
