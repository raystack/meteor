package otelgrpc

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/goto/meteor/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type UnaryParams struct {
	Start  time.Time
	Method string
	Req    any
	Res    any
	Err    error
}

type Monitor struct {
	duration     metric.Int64Histogram
	requestSize  metric.Int64Histogram
	responseSize metric.Int64Histogram
	attributes   []attribute.KeyValue
}

func NewOtelGRPCMonitor(hostName string) Monitor {
	meter := otel.Meter("github.com/goto/meteor/metrics/otelgrpc")

	duration, err := meter.Int64Histogram("rpc.client.duration", metric.WithUnit("ms"))
	handleOtelErr(err)

	requestSize, err := meter.Int64Histogram("rpc.client.request.size", metric.WithUnit("By"))
	handleOtelErr(err)

	responseSize, err := meter.Int64Histogram("rpc.client.response.size", metric.WithUnit("By"))
	handleOtelErr(err)

	addr, port := ExtractAddress(hostName)

	return Monitor{
		duration:     duration,
		requestSize:  requestSize,
		responseSize: responseSize,
		attributes: []attribute.KeyValue{
			semconv.RPCSystemGRPC,
			attribute.String("network.transport", "tcp"),
			attribute.String("server.address", addr),
			attribute.String("server.port", port),
		},
	}
}

func GetProtoSize(p any) int {
	if p == nil {
		return 0
	}

	size := proto.Size(p.(proto.Message))
	return size
}

func (m *Monitor) RecordUnary(ctx context.Context, p UnaryParams) {
	reqSize := GetProtoSize(p.Req)
	resSize := GetProtoSize(p.Res)

	attrs := make([]attribute.KeyValue, len(m.attributes))
	copy(attrs, m.attributes)
	attrs = append(attrs, attribute.String("rpc.grpc.status_text", utils.StatusText(p.Err)))
	attrs = append(attrs, attribute.String("network.type", netTypeFromCtx(ctx)))
	attrs = append(attrs, ParseFullMethod(p.Method)...)

	m.duration.Record(ctx,
		time.Since(p.Start).Milliseconds(),
		metric.WithAttributes(attrs...))

	m.requestSize.Record(ctx,
		int64(reqSize),
		metric.WithAttributes(attrs...))

	m.responseSize.Record(ctx,
		int64(resSize),
		metric.WithAttributes(attrs...))
}

func (m *Monitor) RecordStream(ctx context.Context, start time.Time, method string, err error) {
	attrs := make([]attribute.KeyValue, len(m.attributes))
	copy(attrs, m.attributes)
	attrs = append(attrs, attribute.String("rpc.grpc.status_text", utils.StatusText(err)))
	attrs = append(attrs, attribute.String("network.type", netTypeFromCtx(ctx)))
	attrs = append(attrs, ParseFullMethod(method)...)

	m.duration.Record(ctx,
		time.Since(start).Milliseconds(),
		metric.WithAttributes(attrs...))
}

func (m *Monitor) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) (err error) {
		defer func(start time.Time) {
			m.RecordUnary(ctx, UnaryParams{
				Start: start,
				Req:   req,
				Res:   reply,
				Err:   err,
			})
		}(time.Now())

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (m *Monitor) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (s grpc.ClientStream, err error) {
		defer func(start time.Time) {
			m.RecordStream(ctx, start, method, err)
		}(time.Now())

		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (m *Monitor) GetAttributes() []attribute.KeyValue {
	return m.attributes
}

func ParseFullMethod(fullMethod string) []attribute.KeyValue {
	name := strings.TrimLeft(fullMethod, "/")
	service, method, found := strings.Cut(name, "/")
	if !found {
		return nil
	}

	var attrs []attribute.KeyValue
	if service != "" {
		attrs = append(attrs, semconv.RPCService(service))
	}
	if method != "" {
		attrs = append(attrs, semconv.RPCMethod(method))
	}
	return attrs
}

func handleOtelErr(err error) {
	if err != nil {
		otel.Handle(err)
	}
}

func ExtractAddress(addr string) (host, port string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, "80"
	}

	return host, port
}

func netTypeFromCtx(ctx context.Context) (ipType string) {
	ipType = "unknown"
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ipType
	}

	clientIP, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return ipType
	}

	ip := net.ParseIP(clientIP)
	if ip.To4() != nil {
		ipType = "ipv4"
	} else if ip.To16() != nil {
		ipType = "ipv6"
	}

	return ipType
}
