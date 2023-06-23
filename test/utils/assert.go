package utils

import (
	"fmt"
	"os"
	"reflect"
	"sort"
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

func AssertProtosWithJSONFile(t *testing.T, expectedFilePath string, actual []*v1beta2.Asset) {
	t.Helper()

	AssertJSONFile(t, expectedFilePath, actual, jsondiff.FullMatch)
}

func AssertJSONFile(t *testing.T, expectedFilePath string, actual []*v1beta2.Asset, expectedDiff jsondiff.Difference) {
	t.Helper()

	expected, err := os.ReadFile(expectedFilePath)
	require.NoError(t, err)

	AssertJSON(t, expected, actual, expectedDiff)
}

func AssertJSONEq(t *testing.T, expected, actual interface{}) {
	t.Helper()

	AssertJSON(t, expected, actual, jsondiff.FullMatch)
}

func AssertJSON(t *testing.T, expected, actual interface{}, expectedDiff jsondiff.Difference) {
	t.Helper()

	asBytes := func(v interface{}) []byte {
		switch v := v.(type) {
		case []byte:
			return v
		case string:
			return ([]byte)(v)
		case []*v1beta2.Asset:
			return buildJSONFromAssets(t, v)
		case proto.Message:
			data, err := protojson.MarshalOptions{
				UseProtoNames:   true,
				EmitUnpopulated: true,
			}.Marshal(v)
			require.NoError(t, err)
			return data
		}
		t.Errorf("unexpected type: %T", v)
		return nil
	}

	options := jsondiff.DefaultConsoleOptions()
	actualDiff, report := jsondiff.Compare(asBytes(expected), asBytes(actual), &options)
	assert.Equal(t, expectedDiff, actualDiff, "expected json is %s, got %s\n %s", expectedDiff, actualDiff, report)
}

func SortedAssets(assets []*v1beta2.Asset) []*v1beta2.Asset {
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].Name < assets[j].Name
	})
	return assets
}

func buildJSONFromAssets(t *testing.T, actuals []*v1beta2.Asset) []byte {
	actualJSON := "["
	m := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
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
