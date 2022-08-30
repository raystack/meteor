package utils

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type ArgMatcher interface{ Matches(interface{}) bool }

func OfTypeContext() ArgMatcher {
	return mock.MatchedBy(func(ctx context.Context) bool { return ctx != nil })
}
