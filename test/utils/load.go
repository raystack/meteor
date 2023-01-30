package utils

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func LoadJSON(t *testing.T, filePath string, v interface{}) {
	t.Helper()

	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	if m, ok := v.(proto.Message); ok {
		err = protojson.Unmarshal(data, m)
	} else {
		err = json.Unmarshal(data, v)
	}
	require.NoError(t, err)
}
