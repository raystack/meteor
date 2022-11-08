package utils

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nsf/jsondiff"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func AssertEqualProto(t *testing.T, expected, actual proto.Message) {
	t.Helper()

	if diff := cmp.Diff(actual, expected, protocmp.Transform()); diff != "" {
		msg := fmt.Sprintf(
			"Not equal:\n"+
				"expected:\n\t'%s'\n"+
				"actual:\n\t'%s'\n"+
				"diff (-expected +actual):\n%s",
			expected, actual, diff,
		)
		assert.Fail(t, msg)
	}
}

func AssertAssetsWithJSON(t *testing.T, expected, actuals []*v1beta2.Asset) {
	t.Helper()

	expectedBytes := buildJSONFromAssets(t, expected)
	actualBytes := buildJSONFromAssets(t, actuals)

	assertJSON(t, expectedBytes, actualBytes)
}

func AssertProtosWithJSONFile(t *testing.T, expectedFilePath string, actuals []*v1beta2.Asset) {
	t.Helper()

	expectedBytes, err := os.ReadFile(expectedFilePath)
	require.NoError(t, err)

	actualBytes := buildJSONFromAssets(t, actuals)

	assertJSON(t, expectedBytes, actualBytes)
}

func assertJSON(t *testing.T, expected []byte, actual []byte) {
	t.Helper()

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
