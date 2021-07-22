package meta

import (
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/odpf/meteor/proto/odpf/meta/common"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"google.golang.org/protobuf/runtime/protoimpl"
)

type Bucket struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
	Urn          string
	BucketName   string
	Location     string
	LocationType string
	StorageClass string
	Source string
	Timestamps   *common.Timestamp
	Tags         *facets.Tags
	Event        *common.Event
	Blobs        []Blob
}

type Blob struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
	Urn         string               `protobuf:"bytes,1,opt,name=urn,proto3" json:"urn,omitempty"`
	Name        string               `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Description string               `protobuf:"bytes,4,opt,name=description,proto3" json:"description,omitempty"`
	Size        int64              `protobuf:"bytes,6,opt,name=size,proto3" json:"size,omitempty"`
	DeletedAt   *timestamp.Timestamp `protobuf:"bytes,7,opt,name=deleted_at,proto3" json:"deleted_at,omitempty"`
	ExpiredAt   *timestamp.Timestamp `protobuf:"bytes,8,opt,name=expired_at,proto3" json:"expired_at,omitempty"`
	Ownership   *facets.Ownership    `protobuf:"bytes,9,opt,name=ownership,proto3" json:"ownership,omitempty"`
	Tags        *facets.Tags         `protobuf:"bytes,21,opt,name=tags,proto3" json:"tags,omitempty"`
	Custom      *facets.Custom       `protobuf:"bytes,22,opt,name=custom,proto3" json:"custom,omitempty"`
	Timestamps  *common.Timestamp    `protobuf:"bytes,23,opt,name=timestamps,proto3" json:"timestamps,omitempty"`
	Event       *common.Event        `protobuf:"bytes,100,opt,name=event,proto3" json:"event,omitempty"`
}