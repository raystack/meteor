package utils

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func ValueAsJSONReader(t *testing.T, v interface{}) io.ReadCloser {
	t.Helper()

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatalf("ValueAsJSONReader() = %v", err)
	}
	return io.NopCloser(&buf)
}

func Respond(t *testing.T, w http.ResponseWriter, status int, v interface{}) {
	t.Helper()

	if v == nil {
		w.WriteHeader(status)
		return
	}

	switch body := v.(type) {
	case string:
		Respond(t, w, status, ([]byte)(body))
		return

	case []byte:
		if !json.Valid(body) {
			w.WriteHeader(status)
			if _, err := w.Write(body); err != nil {
				t.Fatalf("Respond() = %v", err)
			}
			return
		}
		v = (json.RawMessage)(body)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("Respond() = %v", err)
	}
}
