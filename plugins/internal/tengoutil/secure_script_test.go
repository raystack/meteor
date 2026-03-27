//go:build plugins
// +build plugins

package tengoutil

import (
	"testing"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/stretchr/testify/assert"
)

func TestNewSecureScript(t *testing.T) {
	t.Run("Allows import of builtin modules except os", func(t *testing.T) {
		s, err := NewSecureScript(([]byte)(heredoc.Doc(`
			math := import("math")
			text := import("text")
			times := import("times")
			rand := import("rand")
			fmt := import("fmt")
			json := import("json")
			base64 := import("base64")
			hex := import("hex")
			enum := import("enum")
		`)), nil)
		assert.NoError(t, err)
		_, err = s.Compile()
		assert.NoError(t, err)
	})

	t.Run("os import disallowed", func(t *testing.T) {
		s, err := NewSecureScript(([]byte)(`os := import("os")`), nil)
		assert.NoError(t, err)
		_, err = s.Compile()
		assert.ErrorContains(t, err, "Compile Error: module 'os' not found")
	})

	t.Run("File import disallowed", func(t *testing.T) {
		s, err := NewSecureScript(([]byte)(`sum := import("./testdata/sum")`), nil)
		assert.NoError(t, err)
		_, err = s.Compile()
		assert.ErrorContains(t, err, "Compile Error: module './testdata/sum' not found")
	})

	t.Run("Script globals", func(t *testing.T) {
		s, err := NewSecureScript(([]byte)(`obj.prop = 1`), nil)
		assert.NoError(t, err)
		_, err = s.Compile()
		assert.ErrorContains(t, err, "Compile Error: unresolved reference 'obj'")

		s, err = NewSecureScript(([]byte)(`obj.prop = 1`), map[string]interface{}{
			"obj": map[string]interface{}{},
		})
		assert.NoError(t, err)
		_, err = s.Compile()
		assert.NoError(t, err)
	})

	t.Run("Allows import of custom http module", func(t *testing.T) {
		s, err := NewSecureScript(([]byte)(heredoc.Doc(`
			http := import("http")
			response := http.get("http://example.com")
			response.body
		`)), nil)
		assert.NoError(t, err)
		_, err = s.Compile()
		assert.NoError(t, err)
	})

	t.Run("HTTP GET with headers", func(t *testing.T) {
		s, err := NewSecureScript(([]byte)(heredoc.Doc(`
			http := import("http")
			headers := { "User-Agent": "test-agent", "Accept": "application/json" }
			response := http.get("http://example.com", headers)
			response.body
		`)), nil)
		assert.NoError(t, err)
		_, err = s.Compile()
		assert.NoError(t, err)
	})

	t.Run("HTTP GET with invalid URL argument type", func(t *testing.T) {
		s, err := NewSecureScript(([]byte)(heredoc.Doc(`
			http := import("http")
			http.get(12345)
		`)), nil)
		assert.NoError(t, err)
		_, err = s.Compile()
		assert.NoError(t, err)
		_, err = s.Run()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported protocol scheme")
	})

	t.Run("HTTP GET with timeout", func(t *testing.T) {
		s, err := NewSecureScript(([]byte)(heredoc.Doc(`
			http := import("http")
			response := http.get("http://example.com")
			response.body
		`)), nil)
		assert.NoError(t, err)
		originalTimeout := defaultTimeout
		defaultTimeout = 1 * time.Millisecond
		defer func() { defaultTimeout = originalTimeout }()
		_, err = s.Compile()
		assert.NoError(t, err)
		_, err = s.Run()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})
}
