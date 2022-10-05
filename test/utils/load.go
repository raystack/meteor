package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func LoadJSONIntoProto(t *testing.T, filePath string, m proto.Message) {
	t.Helper()

	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	err = protojson.Unmarshal(data, m)
	require.NoError(t, err)
}
