// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.13.0
// source: odpf/entities/common/event.proto

package common

import (
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
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

// Event represents an event in the system.
// Event is majorly used to represent the state of the system in the form of events.
// It can be used in any schema which intend to produce events to message bus.
type Event struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Required. The timestamp of the event.
	// For example "2018-01-01T00:00:00Z".
	Timestamp *timestamp.Timestamp `protobuf:"bytes,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// Optional. The activity that created the event.
	// For example "create", "update".
	Action string `protobuf:"bytes,2,opt,name=action,proto3" json:"action,omitempty"`
	// Optional. The description of the event.
	// For example "user is created from signup form".
	Description string `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
}

func (x *Event) Reset() {
	*x = Event{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_entities_common_event_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Event) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Event) ProtoMessage() {}

func (x *Event) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_entities_common_event_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Event.ProtoReflect.Descriptor instead.
func (*Event) Descriptor() ([]byte, []int) {
	return file_odpf_entities_common_event_proto_rawDescGZIP(), []int{0}
}

func (x *Event) GetTimestamp() *timestamp.Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *Event) GetAction() string {
	if x != nil {
		return x.Action
	}
	return ""
}

func (x *Event) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

var File_odpf_entities_common_event_proto protoreflect.FileDescriptor

var file_odpf_entities_common_event_proto_rawDesc = []byte{
	0x0a, 0x20, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f,
	0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x14, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65,
	0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x7b, 0x0a, 0x05, 0x45, 0x76, 0x65,
	0x6e, 0x74, 0x12, 0x38, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x16, 0x0a, 0x06,
	0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x61, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72,
	0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x4d, 0x0a, 0x17, 0x69, 0x6f, 0x2e, 0x6f, 0x64, 0x70,
	0x66, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x42, 0x0a, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x5a, 0x26, 0x67,
	0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_odpf_entities_common_event_proto_rawDescOnce sync.Once
	file_odpf_entities_common_event_proto_rawDescData = file_odpf_entities_common_event_proto_rawDesc
)

func file_odpf_entities_common_event_proto_rawDescGZIP() []byte {
	file_odpf_entities_common_event_proto_rawDescOnce.Do(func() {
		file_odpf_entities_common_event_proto_rawDescData = protoimpl.X.CompressGZIP(file_odpf_entities_common_event_proto_rawDescData)
	})
	return file_odpf_entities_common_event_proto_rawDescData
}

var file_odpf_entities_common_event_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_odpf_entities_common_event_proto_goTypes = []interface{}{
	(*Event)(nil),               // 0: odpf.entities.common.Event
	(*timestamp.Timestamp)(nil), // 1: google.protobuf.Timestamp
}
var file_odpf_entities_common_event_proto_depIdxs = []int32{
	1, // 0: odpf.entities.common.Event.timestamp:type_name -> google.protobuf.Timestamp
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_odpf_entities_common_event_proto_init() }
func file_odpf_entities_common_event_proto_init() {
	if File_odpf_entities_common_event_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_odpf_entities_common_event_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Event); i {
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
			RawDescriptor: file_odpf_entities_common_event_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_odpf_entities_common_event_proto_goTypes,
		DependencyIndexes: file_odpf_entities_common_event_proto_depIdxs,
		MessageInfos:      file_odpf_entities_common_event_proto_msgTypes,
	}.Build()
	File_odpf_entities_common_event_proto = out.File
	file_odpf_entities_common_event_proto_rawDesc = nil
	file_odpf_entities_common_event_proto_goTypes = nil
	file_odpf_entities_common_event_proto_depIdxs = nil
}
