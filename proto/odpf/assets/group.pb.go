// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.13.0
// source: odpf/assets/group.proto

package assets

import (
	common "github.com/odpf/meteor/proto/odpf/assets/common"
	facets "github.com/odpf/meteor/proto/odpf/assets/facets"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Group represents a group of users and assets.
type Group struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Representation of the resource
	Resource *common.Resource `protobuf:"bytes,1,opt,name=resource,proto3" json:"resource,omitempty"`
	// The email of the group.
	// Example: `xyz@xyz.com`
	Email string `protobuf:"bytes,3,opt,name=email,proto3" json:"email,omitempty"`
	// The members of the group.
	// For example look at schema of the member.
	Members []*Member `protobuf:"bytes,21,rep,name=members,proto3" json:"members,omitempty"`
	// List of the user's custom properties.
	// Properties facet can be used to set custom properties, tags and labels for a user.
	Properties *facets.Properties `protobuf:"bytes,31,opt,name=properties,proto3" json:"properties,omitempty"`
	// The timestamp of the user's creation.
	// Timstamp facet can be used to set the creation and updation timestamp of a user.
	Timestamps *common.Timestamp `protobuf:"bytes,32,opt,name=timestamps,proto3" json:"timestamps,omitempty"`
	// The timestamp of the generated event.
	// Event schemas is defined in the common event schema.
	Event *common.Event `protobuf:"bytes,100,opt,name=event,proto3" json:"event,omitempty"`
}

func (x *Group) Reset() {
	*x = Group{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_assets_group_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Group) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Group) ProtoMessage() {}

func (x *Group) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_assets_group_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Group.ProtoReflect.Descriptor instead.
func (*Group) Descriptor() ([]byte, []int) {
	return file_odpf_assets_group_proto_rawDescGZIP(), []int{0}
}

func (x *Group) GetResource() *common.Resource {
	if x != nil {
		return x.Resource
	}
	return nil
}

func (x *Group) GetEmail() string {
	if x != nil {
		return x.Email
	}
	return ""
}

func (x *Group) GetMembers() []*Member {
	if x != nil {
		return x.Members
	}
	return nil
}

func (x *Group) GetProperties() *facets.Properties {
	if x != nil {
		return x.Properties
	}
	return nil
}

func (x *Group) GetTimestamps() *common.Timestamp {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

func (x *Group) GetEvent() *common.Event {
	if x != nil {
		return x.Event
	}
	return nil
}

// Member represents a user.
type Member struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The unique identifier for the user.
	// Example: `user:example`.
	Urn string `protobuf:"bytes,1,opt,name=urn,proto3" json:"urn,omitempty"`
	// The role of the user.
	// Example: `owner`.
	Role string `protobuf:"bytes,2,opt,name=role,proto3" json:"role,omitempty"`
}

func (x *Member) Reset() {
	*x = Member{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_assets_group_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Member) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Member) ProtoMessage() {}

func (x *Member) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_assets_group_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Member.ProtoReflect.Descriptor instead.
func (*Member) Descriptor() ([]byte, []int) {
	return file_odpf_assets_group_proto_rawDescGZIP(), []int{1}
}

func (x *Member) GetUrn() string {
	if x != nil {
		return x.Urn
	}
	return ""
}

func (x *Member) GetRole() string {
	if x != nil {
		return x.Role
	}
	return ""
}

var File_odpf_assets_group_proto protoreflect.FileDescriptor

var file_odpf_assets_group_proto_rawDesc = []byte{
	0x0a, 0x17, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x67, 0x72,
	0x6f, 0x75, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x6f, 0x64, 0x70, 0x66, 0x2e,
	0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x1a, 0x22, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x61, 0x73, 0x73,
	0x65, 0x74, 0x73, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x6f, 0x64, 0x70, 0x66,
	0x2f, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x21, 0x6f, 0x64, 0x70, 0x66,
	0x2f, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x72,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x23, 0x6f,
	0x64, 0x70, 0x66, 0x2f, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2f, 0x66, 0x61, 0x63, 0x65, 0x74,
	0x73, 0x2f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0xb6, 0x02, 0x0a, 0x05, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x12, 0x38, 0x0a, 0x08,
	0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c,
	0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x63, 0x6f, 0x6d,
	0x6d, 0x6f, 0x6e, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x08, 0x72, 0x65,
	0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x65, 0x6d, 0x61, 0x69, 0x6c, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x65, 0x6d, 0x61, 0x69, 0x6c, 0x12, 0x2d, 0x0a, 0x07,
	0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x18, 0x15, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e,
	0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x4d, 0x65, 0x6d, 0x62,
	0x65, 0x72, 0x52, 0x07, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x12, 0x3e, 0x0a, 0x0a, 0x70,
	0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x1f, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1e, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x66, 0x61,
	0x63, 0x65, 0x74, 0x73, 0x2e, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52,
	0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x3d, 0x0a, 0x0a, 0x74,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x18, 0x20, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1d, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x63, 0x6f,
	0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x12, 0x2f, 0x0a, 0x05, 0x65, 0x76,
	0x65, 0x6e, 0x74, 0x18, 0x64, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x6f, 0x64, 0x70, 0x66,
	0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x45,
	0x76, 0x65, 0x6e, 0x74, 0x52, 0x05, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x22, 0x2e, 0x0a, 0x06, 0x4d,
	0x65, 0x6d, 0x62, 0x65, 0x72, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72, 0x6e, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x75, 0x72, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x72, 0x6f, 0x6c, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x72, 0x6f, 0x6c, 0x65, 0x42, 0x3b, 0x0a, 0x0e, 0x69,
	0x6f, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x42, 0x0a, 0x47,
	0x72, 0x6f, 0x75, 0x70, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x5a, 0x1d, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x6e, 0x2f, 0x61, 0x73, 0x73, 0x65, 0x74, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_odpf_assets_group_proto_rawDescOnce sync.Once
	file_odpf_assets_group_proto_rawDescData = file_odpf_assets_group_proto_rawDesc
)

func file_odpf_assets_group_proto_rawDescGZIP() []byte {
	file_odpf_assets_group_proto_rawDescOnce.Do(func() {
		file_odpf_assets_group_proto_rawDescData = protoimpl.X.CompressGZIP(file_odpf_assets_group_proto_rawDescData)
	})
	return file_odpf_assets_group_proto_rawDescData
}

var file_odpf_assets_group_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_odpf_assets_group_proto_goTypes = []interface{}{
	(*Group)(nil),             // 0: odpf.assets.Group
	(*Member)(nil),            // 1: odpf.assets.Member
	(*common.Resource)(nil),   // 2: odpf.assets.common.Resource
	(*facets.Properties)(nil), // 3: odpf.assets.facets.Properties
	(*common.Timestamp)(nil),  // 4: odpf.assets.common.Timestamp
	(*common.Event)(nil),      // 5: odpf.assets.common.Event
}
var file_odpf_assets_group_proto_depIdxs = []int32{
	2, // 0: odpf.assets.Group.resource:type_name -> odpf.assets.common.Resource
	1, // 1: odpf.assets.Group.members:type_name -> odpf.assets.Member
	3, // 2: odpf.assets.Group.properties:type_name -> odpf.assets.facets.Properties
	4, // 3: odpf.assets.Group.timestamps:type_name -> odpf.assets.common.Timestamp
	5, // 4: odpf.assets.Group.event:type_name -> odpf.assets.common.Event
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_odpf_assets_group_proto_init() }
func file_odpf_assets_group_proto_init() {
	if File_odpf_assets_group_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_odpf_assets_group_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Group); i {
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
		file_odpf_assets_group_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Member); i {
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
			RawDescriptor: file_odpf_assets_group_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_odpf_assets_group_proto_goTypes,
		DependencyIndexes: file_odpf_assets_group_proto_depIdxs,
		MessageInfos:      file_odpf_assets_group_proto_msgTypes,
	}.Build()
	File_odpf_assets_group_proto = out.File
	file_odpf_assets_group_proto_rawDesc = nil
	file_odpf_assets_group_proto_goTypes = nil
	file_odpf_assets_group_proto_depIdxs = nil
}
