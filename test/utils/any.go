package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

func BuildAny(t *testing.T, protoMessage protoreflect.ProtoMessage) *anypb.Any {
	res, err := anypb.New(protoMessage)
	require.NoError(t, err)

	return res
}
