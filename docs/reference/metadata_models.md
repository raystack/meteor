# Meteor Metadata Model

We have a set of defined metadata models which define the structure of metadata that meteor will yield.
To visit the metadata models being used by different extractors please visit [here](../reference/extractors.md).
We are currently using the following metadata models:

* [Bucket](../../proto/odpf/meta/Bucket.pb.go):
Used for metadata being extracted from buckets. Buckets are the basic containers in google cloud services, or Amazon S3, etc that are used fot data storage, and quite popular because of their features of access management, aggregation of usage and services and ease of configurations.
Currently, Meteor provides a metadata extractor for the buckets mentioned [here](../reference/extractors.md)

* [Dashboard](../../proto/odpf/meta/Dashboard.pb.go):
Dashboards are essential part of data analysis and are used to track, analyse and visualization.
These Dashboard metadata model includes some basic fields like `urn` and `source`, etc and a list of `Chart`.
There are multiple dashboards that are essential for Data Analysis such as metabase, grafana, tableau, etc.
Please refer the list of Dashboards meteor currently supports [here](../reference/extractors.md).

* [Chart](../../proto/odpf/meta/Chart.pb.go):
Charts are included in all the Dashboard and are result of certain queries in a Dashboard.
Information about them includes the information of the query and few similar details.

* [User](../../proto/odpf/meta/User.pb.go):
This metadata model is used for defining the output of extraction on Users accounts.
Some of these source can be GitHub, Workday, Google Suite, LDAP.
Please refer the list of user meteor currently supports [here](../reference/extractors.md).

* [Table](../../proto/odpf/meta/Table.pb.go):
This metadata model is being used by extractors based around `databases` or for the ones that store data in tabular format.
It contains various fields that includes `schema` of the table and other access related information.

* [Job](../../proto/odpf/meta/Job.pb.go):
Most of the data is being streamed as queues by kafka or other stack in DE pipeline.
And hence Job is a metadata model build for this purpose.

`Proto` has been used to define these metadata models.
To check their implementation please refer [here](../../proto/odpf/meta/README.md).

## Usage

```golang
import(
"github.com/odpf/meteor/proto/odpf/assets"
"github.com/odpf/meteor/proto/odpf/assets/facets"
)

func main(){
    // result is a var of data type of assets.Table one of our metadata model
    result := &assets.Table{
        // assigining value to metadata model
        Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
        Name: tableName,
    }

    // using column facet to add metadata info of schema

    var columns []*facets.Column
    columns = append(columns, &facets.Column{
            Name:       "column_name",
            DataType:   "varchar",
            IsNullable: true,
            Length:     256,
        })
    result.Schema = &facets.Columns{
        Columns: columns,
    }
}
```
