package client

import (
	"context"
	"fmt"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	sh "github.com/raystack/frontier/proto/v1beta1"
	og "github.com/raystack/meteor/metrics/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	GRPCMaxClientSendSize      = 45 << 20 // 45MB
	GRPCMaxClientRecvSize      = 45 << 20 // 45MB
	GRPCMaxRetry          uint = 3
)

type Client interface {
	sh.FrontierServiceClient
	Connect(ctx context.Context, host string) error
	Close() error
}

func New() Client {
	return &client{}
}

type client struct {
	sh.FrontierServiceClient
	conn *grpc.ClientConn
}

func (c *client) Connect(ctx context.Context, host string) error {
	dialTimeoutCtx, dialCancel := context.WithTimeout(ctx, time.Second*2)
	defer dialCancel()

	var err error
	c.conn, err = c.createConnection(dialTimeoutCtx, host)
	if err != nil {
		return fmt.Errorf("create connection: %w", err)
	}

	c.FrontierServiceClient = sh.NewFrontierServiceClient(c.conn)

	return nil
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) createConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
	m := og.NewOtelGRPCMonitor(host)

	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(GRPCMaxRetry),
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(GRPCMaxClientSendSize),
			grpc.MaxCallRecvMsgSize(GRPCMaxClientRecvSize),
		),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			grpc_retry.UnaryClientInterceptor(retryOpts...),
			otelgrpc.UnaryClientInterceptor(),
			grpc_prometheus.UnaryClientInterceptor,
			m.UnaryClientInterceptor(),
		)),
		grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
			otelgrpc.StreamClientInterceptor(),
			grpc_prometheus.StreamClientInterceptor,
			m.StreamClientInterceptor(),
		)),
	)

	return grpc.DialContext(ctx, host, opts...)
}
