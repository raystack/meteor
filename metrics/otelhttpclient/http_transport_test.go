package otelhttpclient_test

import (
	"testing"

	"github.com/raystack/meteor/metrics/otelhttpclient"
	"github.com/stretchr/testify/assert"
)

func TestNewHTTPTransport(t *testing.T) {
	tr := otelhttpclient.NewHTTPTransport(nil)
	assert.NotNil(t, tr)
}
