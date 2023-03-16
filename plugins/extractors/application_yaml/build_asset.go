package applicationyaml

import (
	"fmt"
	"time"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	service = "application_yaml"
	typ     = "application"
)

func buildAsset(scope string, svc Application) (*v1beta2.Asset, error) {
	data, err := anypb.New(&v1beta2.Application{
		Id:         svc.ID,
		Version:    svc.Version,
		CreateTime: toTimestamp(svc.CreateTime),
		UpdateTime: toTimestamp(svc.UpdateTime),
	})
	if err != nil {
		return nil, fmt.Errorf("build asset metadata: %w", err)
	}

	var owners []*v1beta2.Owner
	if svc.Team.ID != "" {
		owners = append(owners, &v1beta2.Owner{
			Urn:   svc.Team.ID,
			Name:  svc.Team.Name,
			Email: svc.Team.Email,
		})
	}

	return &v1beta2.Asset{
		Urn:         models.NewURN(service, scope, typ, svc.Name),
		Name:        svc.Name,
		Service:     service,
		Type:        typ,
		Url:         svc.URL,
		Description: svc.Description,
		Data:        data,
		Owners:      owners,
		Lineage:     buildLineage(svc),
		Labels:      svc.Labels,
	}, nil
}

func buildLineage(svc Application) *v1beta2.Lineage {
	return &v1beta2.Lineage{
		Upstreams:   buildLineageResources(svc.Inputs),
		Downstreams: buildLineageResources(svc.Outputs),
	}
}

func buildLineageResources(urns []string) []*v1beta2.Resource {
	var res []*v1beta2.Resource
	for _, urn := range urns {
		res = append(res, &v1beta2.Resource{Urn: urn})
	}
	return res
}

func toTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}

	return timestamppb.New(t)
}
