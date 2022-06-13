# Meteor Metadata Model

We have a set of defined metadata models which define the structure of metadata that meteor will yield.
To visit the metadata models being used by different extractors please visit [here](../reference/extractors.md).
We are currently using the following metadata models:

- [Bucket](https://github.com/odpf/proton/blob/main/odpf/assets/bucket.proto):
  Used for metadata being extracted from buckets. Buckets are the basic containers in google cloud services, or Amazon S3, etc that are used fot data storage, and quite popular because of their features of access management, aggregation of usage and services and ease of configurations.
  Currently, Meteor provides a metadata extractor for the buckets mentioned [here](../reference/extractors.md)

- [Dashboard](https://github.com/odpf/proton/blob/main/odpf/assets/dashboard.proto):
  Dashboards are an essential part of data analysis and are used to track, analyze and visualize.
  These Dashboard metadata model includes some basic fields like `urn` and `source`, etc and a list of `Chart`.
  There are multiple dashboards that are essential for Data Analysis such as metabase, grafana, tableau, etc.
  Please refer to the list of Dashboards meteor currently supports [here](../reference/extractors.md).

- [Chart](https://github.com/odpf/proton/blob/main/odpf/assets/chart.proto):
  Charts are included in all the Dashboard and are the result of certain queries in a Dashboard.
  Information about them includes the information of the query and few similar details.

- [User](https://github.com/odpf/proton/blob/main/odpf/assets/user.proto):
  This metadata model is used for defining the output of extraction on Users accounts.
  Some of these sources can be GitHub, Workday, Google Suite, LDAP.
  Please refer to the list of user meteor currently supports [here](../reference/extractors.md).

- [Table](https://github.com/odpf/proton/blob/main/odpf/assets/table.proto):
  This metadata model is being used by extractors based around `databases` or for the ones that store data in tabular format.
  It contains various fields that include `schema` of the table and other access related information.

- [Job](https://github.com/odpf/proton/blob/main/odpf/assets/job.proto):
  Most of the data is being streamed as queues by kafka or other stack in DE pipeline.
  And hence Job is a metadata model built for this purpose.

`Proto` has been used to define these metadata models.
To check their implementation please refer [here](https://github.com/odpf/proton/tree/main/odpf/assets).

## Usage

```golang
import(
"github.com/odpf/meteor/models/odpf/assets/v1beta1"
"github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
)

func main(){
    // result is a var of data type of assetsv1beta1.Table one of our metadata model
    result := &assetsv1beta1.Table{
        // assigining value to metadata model
        Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
        Name: tableName,
    }

    // using column facet to add metadata info of schema

    var columns []*facetsv1beta1.Column
    columns = append(columns, &facetsv1beta1.Column{
            Name:       "column_name",
            DataType:   "varchar",
            IsNullable: true,
            Length:     256,
        })
    result.Schema = &facetsv1beta1.Columns{
        Columns: columns,
    }
}
```
