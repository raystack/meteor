package utils

import (
	"encoding/json"
	"github.com/golang/protobuf/jsonpb"
	"google.golang.org/protobuf/runtime/protoiface"
	"os"
	"testing"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"
)

func AssertWithJSONFile(t *testing.T, expectedFilePath string, actual interface{}) {
	expectedBytes, err := os.ReadFile(expectedFilePath)
	require.NoError(t, err)

	actualBytes, err := json.Marshal(actual)
	require.NoError(t, err)

	AssertJSON(t, expectedBytes, actualBytes)
}

func AssertProtoWithJSONFile(t *testing.T, expectedFilePath string, actual protoiface.MessageV1) {
	expectedBytes, err := os.ReadFile(expectedFilePath)
	require.NoError(t, err)

	m := jsonpb.Marshaler{OrigName: true}
	jsonString, err := m.MarshalToString(actual)
	require.NoError(t, err)

	AssertJSON(t, expectedBytes, []byte(jsonString))
}

func AssertJSON(t *testing.T, expected []byte, actual []byte) {
	options := jsondiff.DefaultConsoleOptions()
	diff, report := jsondiff.Compare(expected, actual, &options)
	if diff != jsondiff.FullMatch {
		t.Errorf("jsons do not match:\n %s", report)
	}
}
