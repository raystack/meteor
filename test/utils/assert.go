package utils

import (
	"encoding/json"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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

func AssertProtoWithJSONFile(t *testing.T, expectedFilePath string, actual proto.Message) {
	expectedBytes, err := os.ReadFile(expectedFilePath)
	require.NoError(t, err)

	m := protojson.MarshalOptions{
		UseProtoNames: true,
	}
	
	jsonBytes, err := m.Marshal(actual)
	require.NoError(t, err)

	AssertJSON(t, expectedBytes, jsonBytes)
}

func AssertJSON(t *testing.T, expected []byte, actual []byte) {
	options := jsondiff.DefaultConsoleOptions()
	diff, report := jsondiff.Compare(expected, actual, &options)
	if diff != jsondiff.FullMatch {
		t.Errorf("jsons do not match:\n %s", report)
	}
}
