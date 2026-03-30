package utils

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type ArgMatcher interface{ Matches(any) bool }

func OfTypeContext() ArgMatcher {
	return mock.MatchedBy(func(ctx context.Context) bool { return ctx != nil })
}
