package caramlstore

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/meteor/plugins/extractors/caramlstore/internal/core"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	gRPCMaxClientSendSizeMB = 45
	gRPCMaxClientRecvSizeMB = 45
	gRPCMaxRetry            = 3
)

type gRPCClient struct {
	opts []grpc.DialOption
	core.CoreServiceClient

	conn    *grpc.ClientConn
	timeout time.Duration
}

func newGRPCClient(opts ...grpc.DialOption) *gRPCClient {
	return &gRPCClient{opts: opts}
}

func (c *gRPCClient) Connect(ctx context.Context, hostURL string, maxSizeInMB int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := c.createConnection(ctx, hostURL, maxSizeInMB)
	if err != nil {
		return fmt.Errorf("error creating connection: %w", err)
	}

	c.timeout = timeout
	c.conn = conn
	c.CoreServiceClient = core.NewCoreServiceClient(c.conn)

	return nil
}

func (c *gRPCClient) Projects(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	res, err := c.ListProjects(ctx, &core.ListProjectsRequest{})
	if err != nil {
		return nil, fmt.Errorf("caramlstore gRPC client: fetch projects: %w", err)
	}

	return res.Projects, nil
}

func (c *gRPCClient) Entities(ctx context.Context, project string) (map[string]*core.Entity, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	res, err := c.ListEntities(ctx, &core.ListEntitiesRequest{
		Filter: &core.ListEntitiesRequest_Filter{
			Project: project,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("caramlstore gRPC client: fetch entities in project '%s': %w", project, err)
	}

	entities := make(map[string]*core.Entity, len(res.Entities))
	for _, e := range res.Entities {
		entities[e.Spec.Name] = e
	}

	return entities, nil
}

func (c *gRPCClient) FeatureTables(ctx context.Context, project string) ([]*core.FeatureTable, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	res, err := c.ListFeatureTables(ctx, &core.ListFeatureTablesRequest{
		Filter: &core.ListFeatureTablesRequest_Filter{
			Project: project,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("caramlstore gRPC client: fetch feature tables in project '%s': %w", project, err)
	}

	return res.Tables, nil
}

func (c *gRPCClient) Close() error {
	return c.conn.Close()
}

func (c *gRPCClient) createConnection(ctx context.Context, hostURL string, maxSizeInMB int) (*grpc.ClientConn, error) {
	if maxSizeInMB <= 0 {
		maxSizeInMB = gRPCMaxClientRecvSizeMB
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(gRPCMaxClientSendSizeMB<<20),
			grpc.MaxCallRecvMsgSize(maxSizeInMB<<20),
		),
		grpc.WithUnaryInterceptor(grpcmw.ChainUnaryClient(
			grpcretry.UnaryClientInterceptor(
				grpcretry.WithBackoff(grpcretry.BackoffExponential(100*time.Millisecond)),
				grpcretry.WithMax(gRPCMaxRetry),
			),
			otelgrpc.UnaryClientInterceptor(),
			grpcprom.UnaryClientInterceptor,
		)),
		grpc.WithStreamInterceptor(grpcmw.ChainStreamClient(
			otelgrpc.StreamClientInterceptor(),
			grpcprom.StreamClientInterceptor,
		)),
	}
	opts = append(opts, c.opts...)

	return grpc.DialContext(ctx, hostURL, opts...)
}
