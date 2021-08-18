# Meteor Metadata Model

We have a set of defined metadata models which define the structure of metadata that meteor will yield.
To visit the metadata models being used by different extractors please visit [here](../reference/extractors.md).
We are currently using the following metadata models:

* [Bucket](../../proto/odpf/meta/Bucket.pb.go)
* [Chart](../../proto/odpf/meta/Chart.pb.go)
* [Dashboard](../../proto/odpf/meta/Dashboard.pb.go)
* [Group](../../proto/odpf/meta/Group.pb.go)
* [Job](../../proto/odpf/meta/Job.pb.go)
* [Table](../../proto/odpf/meta/Table.pb.go)
* [Topic](../../proto/odpf/meta/Topic.pb.go)
* [User](../../proto/odpf/meta/User.pb.go)

`Proto` has been used to define these metadata models.
To check their implementation please refer [here](../../proto/odpf/meta/README.md).
