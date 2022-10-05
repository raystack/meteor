package utils

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatusCode(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		expected codes.Code
	}{
		{
			name:     "with status.Error",
			err:      status.Error(codes.NotFound, "Somebody that I used to know"),
			expected: codes.NotFound,
		},
		{
			name:     "with wrapped status.Error",
			err:      fmt.Errorf("%w", status.Error(codes.Unavailable, "I shot the sheriff")),
			expected: codes.Unavailable,
		},
		{
			name:     "with std lib error",
			err:      errors.New("Runnin' down a dream"),
			expected: codes.Unknown,
		},
		{
			name:     "with nil error",
			err:      nil,
			expected: codes.OK,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, StatusCode(tc.err))
		})
	}
}
