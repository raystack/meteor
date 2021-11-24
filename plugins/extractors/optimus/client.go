package optimus

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

const (
	service                    = "optimus"
	GRPCMaxClientSendSize      = 45 << 20 // 45MB
	GRPCMaxClientRecvSize      = 45 << 20 // 45MB
	GRPCMaxRetry          uint = 3
)

type Client interface {
	pb.RuntimeServiceClient
	Connect(ctx context.Context, host string) error
	// TODO: Remove GetJobTask function and use Optimus' when it is already available.
	GetJobTask(ctx context.Context, in *GetJobTaskRequest, opts ...grpc.CallOption) (*GetJobTaskResponse, error)
}

func newClient() Client {
	return &client{}
}

type client struct {
	pb.RuntimeServiceClient
}

func (c *client) Connect(ctx context.Context, host string) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(ctx, time.Second*2)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = c.createConnection(dialTimeoutCtx, host); err != nil {
		return errors.Wrap(err, "error creating connection")
	}
	defer conn.Close()

	c.RuntimeServiceClient = pb.NewRuntimeServiceClient(conn)

	return
}

func (c *client) GetJobTask(ctx context.Context, in *GetJobTaskRequest, opts ...grpc.CallOption) (*GetJobTaskResponse, error) {
	return nil, nil
}

func (c *client) createConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(GRPCMaxRetry),
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithInsecure(),
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
