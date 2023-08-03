package otelhttpclient

import (
	"context"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
)

type labelerContextKeyType int

const lablelerContextKey labelerContextKeyType = 0

// AnnotateRequest adds telemetry related annotations to request context and returns.
// The request context on the returned request should be retained.
// Ensure `route` is a route template and not actual URL to prevent high cardinality
// on the metrics.
func AnnotateRequest(req *http.Request, route string) *http.Request {
	ctx := req.Context()

	l := &otelhttp.Labeler{}
	l.Add(attribute.String(attributeHTTPRoute, route))

	return req.WithContext(context.WithValue(ctx, lablelerContextKey, l))
}
