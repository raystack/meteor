package optimus

import (
	"context"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

const (
	service                      = "optimus"
	GRPCMaxClientSendSizeMB      = 45
	GRPCMaxClientRecvSizeMB      = 45
	GRPCMaxRetry            uint = 3
)

type Client interface {
	pb.NamespaceServiceClient
	pb.ProjectServiceClient
	pb.JobSpecificationServiceClient
	pb.JobRunServiceClient
	Connect(ctx context.Context, host string, maxSizeInMB int) error
	Close() error
}

func newClient() Client {
	return &client{}
}

type client struct {
	pb.NamespaceServiceClient
	pb.ProjectServiceClient
	pb.JobSpecificationServiceClient
	pb.JobRunServiceClient
	conn *grpc.ClientConn
}

func (c *client) Connect(ctx context.Context, host string, maxSizeInMB int) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(ctx, time.Second*2)
	defer dialCancel()

	if c.conn, err = c.createConnection(dialTimeoutCtx, host, maxSizeInMB); err != nil {
		err = errors.Wrap(err, "error creating connection")
		return
	}

	c.NamespaceServiceClient = pb.NewNamespaceServiceClient(c.conn)
	c.ProjectServiceClient = pb.NewProjectServiceClient(c.conn)
	c.JobSpecificationServiceClient = pb.NewJobSpecificationServiceClient(c.conn)
	c.JobRunServiceClient = pb.NewJobRunServiceClient(c.conn)

	return
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) createConnection(ctx context.Context, host string, maxSizeInMB int) (*grpc.ClientConn, error) {
	if maxSizeInMB <= 0 {
		maxSizeInMB = GRPCMaxClientRecvSizeMB
	}

	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(GRPCMaxRetry),
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(GRPCMaxClientSendSizeMB<<20),
			grpc.MaxCallRecvMsgSize(maxSizeInMB<<20),
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
