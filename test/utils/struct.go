package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func BuildStruct(t *testing.T, value map[string]interface{}) *structpb.Struct {
	res, err := structpb.NewStruct(value)
	require.NoError(t, err)

	return res
}
