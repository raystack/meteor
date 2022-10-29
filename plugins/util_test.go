//go:build plugins
// +build plugins

package plugins_test

import (
	"fmt"
	"testing"

	"github.com/odpf/meteor/plugins"
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

func TestKafkaServersToScope(t *testing.T) {
	cases := map[string]string{
		"int-dagstream-kafka.yonkou.io:9999":                      "int-dagstream-kafka.yonkou.io",
		"int-dagstream-kafka.yonkou.io":                           "int-dagstream-kafka.yonkou.io",
		"2-my-kafka.company.com:1234,1-my-kafka.company.com:1234": "1-my-kafka.company.com,2-my-kafka.company.com",
		"int-dagstream-kafka":                                     "int-dagstream-kafka",
	}
	for servers, expected := range cases {
		name := fmt.Sprintf("Servers=%s", servers)
		t.Run(name, func(t *testing.T) {
			scope := plugins.KafkaServersToScope(servers)
			assert.Equal(t, expected, scope)
		})
	}
}
