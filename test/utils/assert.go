package utils

import (
	"encoding/json"
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

func AssertJSON(t *testing.T, expected []byte, actual []byte) {
	options := jsondiff.DefaultConsoleOptions()
	diff, report := jsondiff.Compare(expected, actual, &options)
	if diff != jsondiff.FullMatch {
		t.Errorf("jsons do not match:\n %s", report)
	}
}
