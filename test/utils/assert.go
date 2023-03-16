package utils

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/nsf/jsondiff"
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

func AssertEqualProtos(t *testing.T, expected, actual interface{}) {
	t.Helper()

	defer func() {
		if r := recover(); r != nil {
			assert.Fail(t, "assert equal protos: panic recovered", r)
		}
	}()

	if reflect.TypeOf(expected).Kind() != reflect.TypeOf(actual).Kind() {
		msg := fmt.Sprintf(
			"Mismatched kinds:\n"+
				"expected: %s\n"+
				"actual: %s\n",
			reflect.TypeOf(expected).Kind(), reflect.TypeOf(actual).Kind(),
		)
		assert.Fail(t, msg)
		return
	}

	if !assert.Len(t, actual, reflect.ValueOf(expected).Len()) {
		return
	}

	ev := reflect.ValueOf(expected)
	av := reflect.ValueOf(actual)
	switch reflect.TypeOf(expected).Kind() {
	case reflect.Slice:
		for i := 0; i < ev.Len(); i++ {
			AssertEqualProto(
				t, ev.Index(i).Interface().(proto.Message), av.Index(i).Interface().(proto.Message),
			)
		}
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
