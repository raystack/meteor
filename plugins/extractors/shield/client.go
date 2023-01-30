package shield

import (
	"context"
	"fmt"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	sh "github.com/odpf/shield/proto/v1beta1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

const (
	service                    = "shield"
	GRPCMaxClientSendSize      = 45 << 20 // 45MB
	GRPCMaxClientRecvSize      = 45 << 20 // 45MB
	GRPCMaxRetry          uint = 3
)

type Client interface {
	sh.ShieldServiceClient
	Connect(ctx context.Context, host string) error
	Close() error
}

func newClient() Client {
	return &client{}
}

type client struct {
	sh.ShieldServiceClient
	conn *grpc.ClientConn
}

func (c *client) Connect(ctx context.Context, host string) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(ctx, time.Second*2)
	defer dialCancel()

	if c.conn, err = c.createConnection(dialTimeoutCtx, host); err != nil {
		err = fmt.Errorf("error creating connection: %w", err)
		return
	}

	c.ShieldServiceClient = sh.NewShieldServiceClient(c.conn)

	return
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) createConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
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
		)),
		grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
			otelgrpc.StreamClientInterceptor(),
			grpc_prometheus.StreamClientInterceptor,
		)),
	)

	return grpc.DialContext(ctx, host, opts...)
}
