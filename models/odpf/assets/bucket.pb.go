// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.18.1
// source: odpf/assets/bucket.proto

package assets

import (
	common "github.com/odpf/meteor/models/odpf/assets/common"
	facets "github.com/odpf/meteor/models/odpf/assets/facets"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Bucket struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Representation of the resource
	Resource *common.Resource `protobuf:"bytes,1,opt,name=resource,proto3" json:"resource,omitempty"`
	// The description of the bucket.
	// Example: `This bucket was created by the product team.`
	Description string `protobuf:"bytes,4,opt,name=description,proto3" json:"description,omitempty"`
	// The location of the bucket. Can differ based on cloud storage used. (e.g. GCS, S3, etc)
	// Example: `ASIA`
	Location string `protobuf:"bytes,5,opt,name=location,proto3" json:"location,omitempty"`
	// The type of the storage. Can differ based on cloud storage used. (e.g. GCS, S3, etc)
	// Example: `STANDARD`
	StorageType string `protobuf:"bytes,6,opt,name=storage_type,json=storageType,proto3" json:"storage_type,omitempty"`
	// List of blobs in the bucket.
	Blobs []*Blob `protobuf:"bytes,7,rep,name=blobs,proto3" json:"blobs,omitempty"`
	// The ownership of the bucket.
	// For an example check out ownership.
	Ownership *facets.Ownership `protobuf:"bytes,31,opt,name=ownership,proto3" json:"ownership,omitempty"`
	// List of the user's custom properties.
	// Properties facet can be used to set custom properties, tags and labels for a user.
	Properties *facets.Properties `protobuf:"bytes,32,opt,name=properties,proto3" json:"properties,omitempty"`
	// The timestamp of the bucket's creation.
	// Timstamp facet can be used to set the creation and updation timestamp of a bucket.
	Timestamps *common.Timestamp `protobuf:"bytes,33,opt,name=timestamps,proto3" json:"timestamps,omitempty"`
	// The timestamp of the generated event.
	// Event schemas is defined in the common event schema.
	Event *common.Event `protobuf:"bytes,100,opt,name=event,proto3" json:"event,omitempty"`
}

func (x *Bucket) Reset() {
	*x = Bucket{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_assets_bucket_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Bucket) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Bucket) ProtoMessage() {}

func (x *Bucket) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_assets_bucket_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Bucket.ProtoReflect.Descriptor instead.
func (*Bucket) Descriptor() ([]byte, []int) {
	return file_odpf_assets_bucket_proto_rawDescGZIP(), []int{0}
}

func (x *Bucket) GetResource() *common.Resource {
	if x != nil {
		return x.Resource
	}
	return nil
}

func (x *Bucket) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Bucket) GetLocation() string {
	if x != nil {
		return x.Location
	}
	return ""
}

func (x *Bucket) GetStorageType() string {
	if x != nil {
		return x.StorageType
	}
	return ""
}

func (x *Bucket) GetBlobs() []*Blob {
	if x != nil {
		return x.Blobs
	}
	return nil
}

func (x *Bucket) GetOwnership() *facets.Ownership {
	if x != nil {
		return x.Ownership
	}
	return nil
}

func (x *Bucket) GetProperties() *facets.Properties {
	if x != nil {
		return x.Properties
	}
	return nil
}

func (x *Bucket) GetTimestamps() *common.Timestamp {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

func (x *Bucket) GetEvent() *common.Event {
	if x != nil {
		return x.Event
	}
	return nil
}

type Blob struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The URN of the blob.
	// Example: `location/bucket-name/file-name`.
	Urn string `protobuf:"bytes,1,opt,name=urn,proto3" json:"urn,omitempty"`
	// The name of the blob.
	// Example: `file-name`.
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// The source of the blob.
	// Example: `gcs`.
	Source string `protobuf:"bytes,3,opt,name=source,proto3" json:"source,omitempty"`
	// The description of the blob.
	// Example: `This is a config file for x app`
	Description string `protobuf:"bytes,4,opt,name=description,proto3" json:"description,omitempty"`
	// The length of the object content.
	// Example: `300`
	Size int64 `protobuf:"varint,5,opt,name=size,proto3" json:"size,omitempty"`
	// Delete time of the blob object.
	DeleteTime *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=delete_time,json=deleteTime,proto3" json:"delete_time,omitempty"`
	// Expire time of the blob object.
	ExpireTime *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=expire_time,json=expireTime,proto3" json:"expire_time,omitempty"`
	// The ownership of the blob.
	// For an example check out ownership.
	Ownership *facets.Ownership `protobuf:"bytes,31,opt,name=ownership,proto3" json:"ownership,omitempty"`
	// List of the user's custom properties.
	// Properties facet can be used to set custom properties, tags and labels for a user.
	Properties *facets.Properties `protobuf:"bytes,32,opt,name=properties,proto3" json:"properties,omitempty"`
	// The timestamp of the blob's creation.
	// Timstamp facet can be used to set the creation and updation timestamp of a blob.
	Timestamps *common.Timestamp `protobuf:"bytes,33,opt,name=timestamps,proto3" json:"timestamps,omitempty"`
}

func (x *Blob) Reset() {
	*x = Blob{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_assets_bucket_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Blob) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Blob) ProtoMessage() {}

func (x *Blob) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_assets_bucket_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Blob.ProtoReflect.Descriptor instead.
func (*Blob) Descriptor() ([]byte, []int) {
	return file_odpf_assets_bucket_proto_rawDescGZIP(), []int{1}
}

func (x *Blob) GetUrn() string {
	if x != nil {
		return x.Urn
	}
	return ""
}

func (x *Blob) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Blob) GetSource() string {
	if x != nil {
		return x.Source
	}
	return ""
}

func (x *Blob) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Blob) GetSize() int64 {
	if x != nil {
		return x.Size
	}
	return 0
}

func (x *Blob) GetDeleteTime() *timestamppb.Timestamp {
	if x != nil {
		return x.DeleteTime
	}
	return nil
}

func (x *Blob) GetExpireTime() *timestamppb.Timestamp {
	if x != nil {
		return x.ExpireTime
	}
	return nil
}

func (x *Blob) GetOwnership() *facets.Ownership {
	if x != nil {
		return x.Ownership
	}
	return nil
}

func (x *Blob) GetProperties() *facets.Properties {
	if x != nil {
		return x.Properties
	}
	return nil
}

func (x *Blob) GetTimestamps() *common.Timestamp {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

var File_odpf_assets_bucket_proto protoreflect.FileDescriptor

var file_odpf_assets_bucket_proto_rawDesc = []byte{
	0x0a, 0x18, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x62, 0x75,
	0x63, 0x6b, 0x65, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x6f, 0x64, 0x70, 0x66,
	0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x22, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x61,
	0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x66, 0x61, 0x63, 0x65, 0x74, 0x73, 0x2f, 0x6f, 0x77, 0x6e,
	0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x23, 0x6f, 0x64,
	0x70, 0x66, 0x2f, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x66, 0x61, 0x63, 0x65, 0x74, 0x73,
	0x2f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x21, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x22, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x61, 0x73, 0x73, 0x65, 0x74,
	0x73, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x61,
	0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x65, 0x76, 0x65,
	0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xb9, 0x03, 0x0a, 0x06, 0x42, 0x75, 0x63,
	0x6b, 0x65, 0x74, 0x12, 0x38, 0x0a, 0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73,
	0x65, 0x74, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x52, 0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x20, 0x0a,
	0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12,
	0x1a, 0x0a, 0x08, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x21, 0x0a, 0x0c, 0x73,
	0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0b, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x27,
	0x0a, 0x05, 0x62, 0x6c, 0x6f, 0x62, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e,
	0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x42, 0x6c, 0x6f, 0x62,
	0x52, 0x05, 0x62, 0x6c, 0x6f, 0x62, 0x73, 0x12, 0x3b, 0x0a, 0x09, 0x6f, 0x77, 0x6e, 0x65, 0x72,
	0x73, 0x68, 0x69, 0x70, 0x18, 0x1f, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x6f, 0x64, 0x70,
	0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x66, 0x61, 0x63, 0x65, 0x74, 0x73, 0x2e,
	0x4f, 0x77, 0x6e, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x52, 0x09, 0x6f, 0x77, 0x6e, 0x65, 0x72,
	0x73, 0x68, 0x69, 0x70, 0x12, 0x3e, 0x0a, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69,
	0x65, 0x73, 0x18, 0x20, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e,
	0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x66, 0x61, 0x63, 0x65, 0x74, 0x73, 0x2e, 0x50, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72,
	0x74, 0x69, 0x65, 0x73, 0x12, 0x3d, 0x0a, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x73, 0x18, 0x21, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e,
	0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x54, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x73, 0x12, 0x2f, 0x0a, 0x05, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x18, 0x64, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x19, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x52, 0x05, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x22, 0xb0, 0x03, 0x0a, 0x04, 0x42, 0x6c, 0x6f, 0x62, 0x12, 0x10, 0x0a,
	0x03, 0x75, 0x72, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x72, 0x6e, 0x12,
	0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e,
	0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x64,
	0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x12, 0x0a,
	0x04, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x04, 0x73, 0x69, 0x7a,
	0x65, 0x12, 0x3b, 0x0a, 0x0b, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x52, 0x0a, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x3b,
	0x0a, 0x0b, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x07, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x3b, 0x0a, 0x09, 0x6f,
	0x77, 0x6e, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x18, 0x1f, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d,
	0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x66, 0x61, 0x63,
	0x65, 0x74, 0x73, 0x2e, 0x4f, 0x77, 0x6e, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x52, 0x09, 0x6f,
	0x77, 0x6e, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x12, 0x3e, 0x0a, 0x0a, 0x70, 0x72, 0x6f, 0x70,
	0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x20, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x6f,
	0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x66, 0x61, 0x63, 0x65, 0x74,
	0x73, 0x2e, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x0a, 0x70, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x3d, 0x0a, 0x0a, 0x74, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x18, 0x21, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x6f,
	0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x74, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x42, 0x3c, 0x0a, 0x0e, 0x69, 0x6f, 0x2e, 0x6f, 0x64,
	0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x42, 0x0b, 0x42, 0x75, 0x63, 0x6b, 0x65,
	0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x5a, 0x1d, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x61,
	0x73, 0x73, 0x65, 0x74, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_odpf_assets_bucket_proto_rawDescOnce sync.Once
	file_odpf_assets_bucket_proto_rawDescData = file_odpf_assets_bucket_proto_rawDesc
)

func file_odpf_assets_bucket_proto_rawDescGZIP() []byte {
	file_odpf_assets_bucket_proto_rawDescOnce.Do(func() {
		file_odpf_assets_bucket_proto_rawDescData = protoimpl.X.CompressGZIP(file_odpf_assets_bucket_proto_rawDescData)
	})
	return file_odpf_assets_bucket_proto_rawDescData
}

var file_odpf_assets_bucket_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_odpf_assets_bucket_proto_goTypes = []interface{}{
	(*Bucket)(nil),                // 0: odpf.assets.Bucket
	(*Blob)(nil),                  // 1: odpf.assets.Blob
	(*common.Resource)(nil),       // 2: odpf.assets.common.Resource
	(*facets.Ownership)(nil),      // 3: odpf.assets.facets.Ownership
	(*facets.Properties)(nil),     // 4: odpf.assets.facets.Properties
	(*common.Timestamp)(nil),      // 5: odpf.assets.common.Timestamp
	(*common.Event)(nil),          // 6: odpf.assets.common.Event
	(*timestamppb.Timestamp)(nil), // 7: google.protobuf.Timestamp
}
var file_odpf_assets_bucket_proto_depIdxs = []int32{
	2,  // 0: odpf.assets.Bucket.resource:type_name -> odpf.assets.common.Resource
	1,  // 1: odpf.assets.Bucket.blobs:type_name -> odpf.assets.Blob
	3,  // 2: odpf.assets.Bucket.ownership:type_name -> odpf.assets.facets.Ownership
	4,  // 3: odpf.assets.Bucket.properties:type_name -> odpf.assets.facets.Properties
	5,  // 4: odpf.assets.Bucket.timestamps:type_name -> odpf.assets.common.Timestamp
	6,  // 5: odpf.assets.Bucket.event:type_name -> odpf.assets.common.Event
	7,  // 6: odpf.assets.Blob.delete_time:type_name -> google.protobuf.Timestamp
	7,  // 7: odpf.assets.Blob.expire_time:type_name -> google.protobuf.Timestamp
	3,  // 8: odpf.assets.Blob.ownership:type_name -> odpf.assets.facets.Ownership
	4,  // 9: odpf.assets.Blob.properties:type_name -> odpf.assets.facets.Properties
	5,  // 10: odpf.assets.Blob.timestamps:type_name -> odpf.assets.common.Timestamp
	11, // [11:11] is the sub-list for method output_type
	11, // [11:11] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_odpf_assets_bucket_proto_init() }
func file_odpf_assets_bucket_proto_init() {
	if File_odpf_assets_bucket_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_odpf_assets_bucket_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Bucket); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_odpf_assets_bucket_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Blob); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_odpf_assets_bucket_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_odpf_assets_bucket_proto_goTypes,
		DependencyIndexes: file_odpf_assets_bucket_proto_depIdxs,
		MessageInfos:      file_odpf_assets_bucket_proto_msgTypes,
	}.Build()
	File_odpf_assets_bucket_proto = out.File
	file_odpf_assets_bucket_proto_rawDesc = nil
	file_odpf_assets_bucket_proto_goTypes = nil
	file_odpf_assets_bucket_proto_depIdxs = nil
}
