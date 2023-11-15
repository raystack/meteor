//go:build plugins
// +build plugins

package plugins_test

import (
	"fmt"
	"testing"

	"github.com/goto/meteor/plugins"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryURN(t *testing.T) {
	t.Run("should create bigquery URN", func(t *testing.T) {
		project := "my-project"
		dataset := "my-dataset"
		table := "my-table"

		actual := plugins.BigQueryURN(project, dataset, table)
		expected := "urn:bigquery:my-project:table:my-project:my-dataset.my-table"

		assert.Equal(t, expected, actual)
	})
}

func TestBigQueryTableFQNToURN(t *testing.T) {
	cases := []struct {
		name        string
		fqn         string
		expected    string
		expectedErr string
	}{
		{
			name:     "Valid",
			fqn:      "bq-raw-internal:dagstream.production_feast09_s2id13_30min_demand",
			expected: "urn:bigquery:bq-raw-internal:table:bq-raw-internal:dagstream.production_feast09_s2id13_30min_demand",
		},
		{
			name:        "Invalid",
			fqn:         "bq-raw-internal:dagstream_production_feast09_s2id13_30min_demand",
			expectedErr: "map URN: unexpected BigQuery table FQN 'bq-raw-internal:dagstream_production_feast09_s2id13_30min_demand', expected in format",
		},
		{
			name:        "Invalid",
			fqn:         ":.",
			expectedErr: "map URN: unexpected BigQuery table FQN ':.', expected in format",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			urn, err := plugins.BigQueryTableFQNToURN(tc.fqn)
			if tc.expectedErr != "" {
				assert.ErrorContains(t, err, tc.expectedErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expected, urn)
		})
	}
}

func TestKafkaURN(t *testing.T) {
	cases := []struct {
		name     string
		servers  string
		topic    string
		expected string
	}{
		{
			name:     "Simple",
			servers:  "celestial-dragons-prodstream.yonkou.io:9999",
			topic:    "staging_feast09_mixed_granularity_demand_forecast_3es",
			expected: "urn:kafka:celestial-dragons-prodstream.yonkou.io:topic:staging_feast09_mixed_granularity_demand_forecast_3es",
		},
		{
			name:     "MultipleBootstrapServers",
			servers:  "2-my-kafka.company.com:9999,1-my-kafka.company.com:9999",
			topic:    "staging_feast09_mixed_granularity_demand_forecast_3es",
			expected: "urn:kafka:1-my-kafka.company.com,2-my-kafka.company.com:topic:staging_feast09_mixed_granularity_demand_forecast_3es",
		},
		{
			name:     "SlugBootstrapServer",
			servers:  "1-my-kafka",
			topic:    "staging_feast09_mixed_granularity_demand_forecast_3es",
			expected: "urn:kafka:1-my-kafka:topic:staging_feast09_mixed_granularity_demand_forecast_3es",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, plugins.KafkaURN(tc.servers, tc.topic))
		})
	}
}

func TestKafkaServersToScope(t *testing.T) {
	cases := map[string]string{
		"int-dagstream-kafka.yonkou.io:9999":                      "int-dagstream-kafka.yonkou.io",
		"int-dagstream-kafka.yonkou.io":                           "int-dagstream-kafka.yonkou.io",
		"2-my-kafka.company.com:1234,1-my-kafka.company.com:1234": "1-my-kafka.company.com,2-my-kafka.company.com",
		"int-dagstream-kafka":                                     "int-dagstream-kafka",
		"1-my-kafka.company.com,2-my-kafka.company.com":           "1-my-kafka.company.com,2-my-kafka.company.com",
	}
	for servers, expected := range cases {
		name := fmt.Sprintf("Servers=%s", servers)
		t.Run(name, func(t *testing.T) {
			scope := plugins.KafkaServersToScope(servers)
			assert.Equal(t, expected, scope)
		})
	}
}

func TestCaraMLStoreURN(t *testing.T) {
	assert.Equal(
		t,
		"urn:caramlstore:my_scope:feature_table:my_project.my_ft",
		plugins.CaraMLStoreURN("my_scope", "my_project", "my_ft"),
	)
}
