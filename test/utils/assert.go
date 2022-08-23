package utils

import (
	"os"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/nsf/jsondiff"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/stretchr/testify/require"
)

func AssertAssetsWithJSON(t *testing.T, expected, actuals []*v1beta2.Asset) {
	expectedBytes := buildJSONFromAssets(t, expected)
	actualBytes := buildJSONFromAssets(t, actuals)

	assertJSON(t, expectedBytes, actualBytes)
}

func AssertProtosWithJSONFile(t *testing.T, expectedFilePath string, actuals []*v1beta2.Asset) {
	expectedBytes, err := os.ReadFile(expectedFilePath)
	require.NoError(t, err)

	actualBytes := buildJSONFromAssets(t, actuals)

	assertJSON(t, expectedBytes, actualBytes)
}

func assertJSON(t *testing.T, expected []byte, actual []byte) {
	options := jsondiff.DefaultConsoleOptions()
	diff, report := jsondiff.Compare(expected, actual, &options)
	if diff != jsondiff.FullMatch {
		t.Errorf("jsons do not match:\n %s", report)
	}
}

func buildJSONFromAssets(t *testing.T, actuals []*v1beta2.Asset) []byte {
	actualJSON := "["
	m := protojson.MarshalOptions{
		UseProtoNames: true,
	}

	for i, actual := range actuals {
		jsonBytes, err := m.Marshal(actual)
		require.NoError(t, err)
		actualJSON += string(jsonBytes)

		if i < (len(actuals) - 1) {
			actualJSON += ","
		}
	}

	actualJSON += "]"

	return []byte(actualJSON)
}
