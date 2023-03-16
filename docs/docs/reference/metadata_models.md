# Meteor Metadata Model

We have a set of defined metadata models which define the structure of metadata
that meteor will yield. To visit the metadata models being used by different
extractors please visit [here](extractors.md). We are currently using the
following metadata models:

- [Bucket][proton-bucket]: Used for metadata being extracted from buckets.
  Buckets are the basic containers in Google cloud services, or Amazon S3, etc.,
  that are used for data storage, and quite popular because of their features of
  access management, aggregation of usage and services and ease of
  configurations. Currently, Meteor provides a metadata extractor for the
  buckets mentioned [here](extractors.md#bucket)

- [Dashboard][proton-dashboard]: Dashboards are an essential part of data
  analysis and are used to track, analyze, and visualize. These Dashboard
  metadata model includes some basic fields like `urn` and `source`, etc., and a
  list of `Chart`. There are multiple dashboards that are essential for Data
  Analysis such as metabase, grafana, tableau, etc. Please refer to the list of
  'Dashboard' extractors meteor currently
  supports [here](extractors.md#dashboard).

  - [Chart][proton-dashboard]: Charts are included in all the Dashboard and are
    the result of certain queries in a Dashboard. Information about them
    includes the information of the query and few similar details.

- [User][proton-user]: This metadata model is used for defining the output of
  extraction on User accounts. Some of these sources can be GitHub, Workday,
  Google Suite, LDAP. Please refer to the list of 'User' extractors meteor
  currently supports [here](extractors.md#user).

- [Table][proton-table]: This metadata model is being used by extractors based
  around databases, typically for the ones that store data in tabular format. It
  contains various fields that include `schema` of the table and other access
  related information. Please refer to the list of 'Table' extractors meteor
  currently supports [here](extractors.md#table).

- [Job][proton-job]: A job can represent a scheduled or recurring task that
  performs some transformation in the data engineering pipeline. Job is a
  metadata model built for this purpose. Please refer to the list of 'Job'
  extractors meteor currently supports [here](extractors.md#table).

- [Topic][proton-topic]: A topic represents a virtual group for logical group of
  messages in message bus like kafka, pubsub, pulsar etc. Please refer to the
  list of 'Topic' extractors meteor currently
  supports [here](extractors.md#topic).

- [Machine Learning Feature Table][proton-featuretable]: A Feature Table is a
  table or view that represents a logical group of time-series feature data as
  it is found in a data source. Please refer to the list of 'Feature Table'
  extractors meteor currently
  supports [here](extractors.md#machine-learning-feature-table).

- [Application][proton-application]: An application represents a service that
  typically communicates over well-defined APIs. Please refer to the list of '
  Application' extractors meteor currently
  supports [here](extractors.md#application).

- [Machine Learning Model][proton-model]: A Model represents a Data Science
  Model commonly used for Machine Learning(ML). Models are algorithms trained on
  data to find patterns or make predictions. Models typically consume ML
  features to generate a meaningful output. Please refer to the list of 'Model'
  extractors meteor currently
  supports [here](extractors.md#machine-learning-model).

`Proto` has been used to define these metadata models. To check their
implementation please refer [here][proton-assets].

## Usage

[//]: # (@formatter:off)

```golang
import(
    assetsv1beta1 "github.com/goto/meteor/models/gotocompany/assets/v1beta1"
    "github.com/goto/meteor/models/gotocompany/assets/facets/v1beta1"
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

[//]: # (@formatter:on)


[proton-bucket]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/bucket.proto

[proton-dashboard]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/dashboard.proto

[proton-user]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/user.proto

[proton-table]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/table.proto

[proton-job]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/job.proto

[proton-topic]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/topic.proto

[proton-featuretable]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/feature_table.proto

[proton-application]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/application.proto

[proton-model]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2/model.proto

[proton-assets]: https://github.com/goto/proton/tree/main/gotocompany/assets/v1beta2
