package otelhttpclient

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Refer OpenTelemetry Semantic Conventions for HTTP Client.
// https://github.com/open-telemetry/semantic-conventions/blob/main/docs/http/http-metrics.md#http-client
const (
	metricClientDuration     = "http.client.duration"
	metricClientRequestSize  = "http.client.request.size"
	metricClientResponseSize = "http.client.response.size"

	attributeNetProtoName    = "network.protocol.name"
	attributeNetProtoVersion = "network.protocol.version"

	attributeServerPort         = "server.port"
	attributeServerAddress      = "server.address"
	attributeHTTPRoute          = "http.route"
	attributeRequestMethod      = "http.request.method"
	attributeResponseStatusCode = "http.response.status_code"
)

type httpTransport struct {
	roundTripper http.RoundTripper

	metricClientDuration     metric.Float64Histogram
	metricClientRequestSize  metric.Int64Counter
	metricClientResponseSize metric.Int64Counter
}

func NewHTTPTransport(baseTransport http.RoundTripper) http.RoundTripper {
	if _, ok := baseTransport.(*httpTransport); ok {
		return baseTransport
	}

	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}

	icl := &httpTransport{roundTripper: baseTransport}
	icl.createMeasures(otel.Meter("github.com/goto/meteor/metrics/otehttpclient"))

	return icl
}

func (tr *httpTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	startAt := time.Now()
	labeler, _ := LabelerFromContext(req.Context())

	var bw bodyWrapper
	if req.Body != nil && req.Body != http.NoBody {
		bw.ReadCloser = req.Body
		req.Body = &bw
	}

	port := req.URL.Port()
	if port == "" {
		port = "80"
		if req.URL.Scheme == "https" {
			port = "443"
		}
	}

	attribs := append(labeler.Get(),
		attribute.String(attributeNetProtoName, "http"),
		attribute.String(attributeRequestMethod, req.Method),
		attribute.String(attributeServerAddress, req.URL.Hostname()),
		attribute.String(attributeServerPort, port),
	)

	resp, err := tr.roundTripper.RoundTrip(req)
	if err != nil {
		attribs = append(attribs,
			attribute.Int(attributeResponseStatusCode, 0),
			attribute.String(attributeNetProtoVersion, fmt.Sprintf("%d.%d", req.ProtoMajor, req.ProtoMinor)),
		)
	} else {
		attribs = append(attribs,
			attribute.Int(attributeResponseStatusCode, resp.StatusCode),
			attribute.String(attributeNetProtoVersion, fmt.Sprintf("%d.%d", resp.ProtoMajor, resp.ProtoMinor)),
		)
	}

	elapsedTime := float64(time.Since(startAt)) / float64(time.Millisecond)
	withAttribs := metric.WithAttributes(attribs...)
	tr.metricClientDuration.Record(ctx, elapsedTime, withAttribs)
	tr.metricClientRequestSize.Add(ctx, int64(bw.read), withAttribs)
	if resp != nil {
		tr.metricClientResponseSize.Add(ctx, resp.ContentLength, withAttribs)
	}

	return resp, err
}

func (tr *httpTransport) createMeasures(meter metric.Meter) {
	var err error

	tr.metricClientRequestSize, err = meter.Int64Counter(metricClientRequestSize)
	handleErr(err)

	tr.metricClientResponseSize, err = meter.Int64Counter(metricClientResponseSize)
	handleErr(err)

	tr.metricClientDuration, err = meter.Float64Histogram(metricClientDuration)
	handleErr(err)
}

func handleErr(err error) {
	if err != nil {
		otel.Handle(err)
	}
}

// bodyWrapper wraps a http.Request.Body (an io.ReadCloser) to track the number
// of bytes read and the last error.
type bodyWrapper struct {
	io.ReadCloser

	read int
	err  error
}

func (w *bodyWrapper) Read(b []byte) (int, error) {
	n, err := w.ReadCloser.Read(b)
	w.read += n
	w.err = err
	return n, err
}

func (w *bodyWrapper) Close() error {
	return w.ReadCloser.Close()
}
