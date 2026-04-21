package models

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

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
func NewEntity(urn, typ, name, source string, props map[string]any) *meteorv1beta1.Entity {
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

// DerivedFromEdge creates a derived_from edge: entityURN is derived from dependencyURN.
// Use when an entity reads from or depends on another (e.g. view from table,
// dashboard from datasource, job from upstream).
func DerivedFromEdge(entityURN, dependencyURN, source string) *meteorv1beta1.Edge {
	return &meteorv1beta1.Edge{
		SourceUrn: entityURN,
		TargetUrn: dependencyURN,
		Type:      "derived_from",
		Source:    source,
	}
}

// GeneratesEdge creates a generates edge: entityURN generates outputURN.
// Use when an entity produces or writes to another (e.g. job writes to
// downstream table, application produces output).
func GeneratesEdge(entityURN, outputURN, source string) *meteorv1beta1.Edge {
	return &meteorv1beta1.Edge{
		SourceUrn: entityURN,
		TargetUrn: outputURN,
		Type:      "generates",
		Source:    source,
	}
}

// ReferencesEdge creates a references edge from sourceURN to targetURN.
// Use this for structural relationships like foreign keys, where one entity
// references another but data does not necessarily flow between them.
func ReferencesEdge(sourceURN, targetURN, source string) *meteorv1beta1.Edge {
	return &meteorv1beta1.Edge{
		SourceUrn: sourceURN,
		TargetUrn: targetURN,
		Type:      "references",
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

	result := map[string]any{
		"entity": json.RawMessage(entityJSON),
	}
	if len(edgesJSON) > 0 {
		result["edges"] = edgesJSON
	}

	return json.Marshal(result)
}

// RecordToMarkdown serializes a record (entity + edges) to Markdown.
func RecordToMarkdown(r Record) ([]byte, error) {
	var b strings.Builder

	e := r.Entity()
	b.WriteString("## ")
	b.WriteString(e.GetName())
	b.WriteString("\n\n")

	// Metadata table.
	b.WriteString("| Field | Value |\n|---|---|\n")
	b.WriteString("| URN | `")
	b.WriteString(e.GetUrn())
	b.WriteString("` |\n")
	b.WriteString("| Type | ")
	b.WriteString(e.GetType())
	b.WriteString(" |\n")
	b.WriteString("| Source | ")
	b.WriteString(e.GetSource())
	b.WriteString(" |\n")
	if desc := e.GetDescription(); desc != "" {
		b.WriteString("| Description | ")
		b.WriteString(desc)
		b.WriteString(" |\n")
	}
	if ct := e.GetCreateTime(); ct != nil && ct.IsValid() {
		b.WriteString("| Created | ")
		b.WriteString(ct.AsTime().Format("2006-01-02T15:04:05Z"))
		b.WriteString(" |\n")
	}
	if ut := e.GetUpdateTime(); ut != nil && ut.IsValid() {
		b.WriteString("| Updated | ")
		b.WriteString(ut.AsTime().Format("2006-01-02T15:04:05Z"))
		b.WriteString(" |\n")
	}

	// Properties.
	if props := e.GetProperties(); props != nil {
		m := props.AsMap()
		if len(m) > 0 {
			writePropertiesMarkdown(&b, m)
		}
	}

	// Edges.
	if edges := r.Edges(); len(edges) > 0 {
		b.WriteString("\n### Edges\n\n")
		b.WriteString("| Type | Source URN | Target URN |\n|---|---|---|\n")
		for _, edge := range edges {
			b.WriteString("| ")
			b.WriteString(edge.GetType())
			b.WriteString(" | `")
			b.WriteString(edge.GetSourceUrn())
			b.WriteString("` | `")
			b.WriteString(edge.GetTargetUrn())
			b.WriteString("` |\n")
		}
	}

	return []byte(b.String()), nil
}

func writePropertiesMarkdown(b *strings.Builder, m map[string]any) {
	keys := sortedKeys(m)

	// Split into scalar and list-of-maps properties.
	var scalarKeys []string
	var tableKeys []string
	for _, k := range keys {
		if items, ok := asSliceOfMaps(m[k]); ok && len(items) > 0 {
			_ = items
			tableKeys = append(tableKeys, k)
		} else {
			scalarKeys = append(scalarKeys, k)
		}
	}

	if len(scalarKeys) > 0 {
		b.WriteString("\n### Properties\n\n")
		for _, k := range scalarKeys {
			writePropertyValue(b, k, m[k])
		}
	}

	for _, k := range tableKeys {
		items, _ := asSliceOfMaps(m[k])
		writeMapSliceTable(b, k, items)
	}
}

func writePropertyValue(b *strings.Builder, key string, val any) {
	switch v := val.(type) {
	case []any:
		b.WriteString("- **")
		b.WriteString(key)
		b.WriteString("**: ")
		strs := make([]string, 0, len(v))
		for _, item := range v {
			strs = append(strs, fmt.Sprintf("%v", item))
		}
		b.WriteString(strings.Join(strs, ", "))
		b.WriteString("\n")
	case map[string]any:
		b.WriteString("- **")
		b.WriteString(key)
		b.WriteString("**:\n")
		for _, sk := range sortedKeys(v) {
			b.WriteString("  - **")
			b.WriteString(sk)
			b.WriteString("**: ")
			fmt.Fprintf(b, "%v", v[sk])
			b.WriteString("\n")
		}
	default:
		b.WriteString("- **")
		b.WriteString(key)
		b.WriteString("**: ")
		fmt.Fprintf(b, "%v", val)
		b.WriteString("\n")
	}
}

func writeMapSliceTable(b *strings.Builder, title string, items []map[string]any) {
	// Collect all headers from the union of keys.
	headerSet := make(map[string]struct{})
	for _, item := range items {
		for k := range item {
			headerSet[k] = struct{}{}
		}
	}
	headers := sortedKeys(headerSet)

	// Title case the section name.
	b.WriteString("\n### ")
	b.WriteString(titleCase(title))
	b.WriteString("\n\n")

	// Header row.
	b.WriteString("|")
	for _, h := range headers {
		b.WriteString(" ")
		b.WriteString(titleCase(strings.ReplaceAll(h, "_", " ")))
		b.WriteString(" |")
	}
	b.WriteString("\n|")
	for range headers {
		b.WriteString("---|")
	}
	b.WriteString("\n")

	// Data rows.
	for _, item := range items {
		b.WriteString("|")
		for _, h := range headers {
			b.WriteString(" ")
			if v, ok := item[h]; ok {
				fmt.Fprintf(b, "%v", v)
			}
			b.WriteString(" |")
		}
		b.WriteString("\n")
	}
}

// asSliceOfMaps checks if val is a []any where all elements are map[string]any.
func asSliceOfMaps(val any) ([]map[string]any, bool) {
	slice, ok := val.([]any)
	if !ok || len(slice) == 0 {
		return nil, false
	}
	result := make([]map[string]any, 0, len(slice))
	for _, item := range slice {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, false
		}
		result = append(result, m)
	}
	return result, true
}

func titleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		if len(w) > 0 {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}

func sortedKeys[M ~map[string]V, V any](m M) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sanitizeMap recursively converts typed maps (e.g., map[string]string) to
// map[string]interface{} so they are compatible with structpb.NewStruct.
func sanitizeMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = sanitizeValue(v)
	}
	return out
}

func sanitizeValue(v any) any {
	switch val := v.(type) {
	case map[string]string:
		out := make(map[string]any, len(val))
		for k, v := range val {
			out[k] = v
		}
		return out
	case map[string]any:
		return sanitizeMap(val)
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = sanitizeValue(item)
		}
		return out
	case []map[string]any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = sanitizeMap(item)
		}
		return out
	case []string:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = item
		}
		return out
	default:
		return v
	}
}
