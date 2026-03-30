package models

import (
	"encoding/json"
	"fmt"

	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// NewURN builds a URN string: "urn:{service}:{scope}:{type}:{id}"
func NewURN(service, scope, kind, id string) string {
	return fmt.Sprintf(
		"urn:%s:%s:%s:%s",
		service, scope, kind, id,
	)
}

// NewEntity creates an entity with properties from a map.
// Values in props are sanitized to be compatible with structpb.NewStruct:
// map[string]string is converted to map[string]interface{}, etc.
func NewEntity(urn, typ, name, source string, props map[string]interface{}) *meteorv1beta1.Entity {
	var properties *structpb.Struct
	if len(props) > 0 {
		sanitized := sanitizeMap(props)
		properties, _ = structpb.NewStruct(sanitized)
	}
	return &meteorv1beta1.Entity{
		Urn:        urn,
		Type:       typ,
		Name:       name,
		Source:     source,
		Properties: properties,
	}
}

// LineageEdge creates a lineage edge from sourceURN to targetURN.
func LineageEdge(sourceURN, targetURN, source string) *meteorv1beta1.Edge {
	return &meteorv1beta1.Edge{
		SourceUrn: sourceURN,
		TargetUrn: targetURN,
		Type:      "lineage",
		Source:    source,
	}
}

// OwnerEdge creates an owned_by edge from entityURN to an owner.
func OwnerEdge(entityURN, ownerURN, source string) *meteorv1beta1.Edge {
	return &meteorv1beta1.Edge{
		SourceUrn: entityURN,
		TargetUrn: ownerURN,
		Type:      "owned_by",
		Source:    source,
	}
}

// EntityToJSON serializes an entity to JSON.
func EntityToJSON(entity *meteorv1beta1.Entity) ([]byte, error) {
	return protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(entity)
}

// RecordToJSON serializes a record (entity + edges) to JSON.
func RecordToJSON(r Record) ([]byte, error) {
	entityJSON, err := EntityToJSON(r.Entity())
	if err != nil {
		return nil, fmt.Errorf("marshal entity: %w", err)
	}

	edgesJSON := make([]json.RawMessage, 0, len(r.Edges()))
	for _, edge := range r.Edges() {
		b, err := protojson.MarshalOptions{
			UseProtoNames:   true,
			EmitUnpopulated: false,
		}.Marshal(edge)
		if err != nil {
			return nil, fmt.Errorf("marshal edge: %w", err)
		}
		edgesJSON = append(edgesJSON, b)
	}

	result := map[string]interface{}{
		"entity": json.RawMessage(entityJSON),
	}
	if len(edgesJSON) > 0 {
		result["edges"] = edgesJSON
	}

	return json.Marshal(result)
}

// sanitizeMap recursively converts typed maps (e.g., map[string]string) to
// map[string]interface{} so they are compatible with structpb.NewStruct.
func sanitizeMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = sanitizeValue(v)
	}
	return out
}

func sanitizeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]string:
		out := make(map[string]interface{}, len(val))
		for k, v := range val {
			out[k] = v
		}
		return out
	case map[string]interface{}:
		return sanitizeMap(val)
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, item := range val {
			out[i] = sanitizeValue(item)
		}
		return out
	default:
		return v
	}
}
