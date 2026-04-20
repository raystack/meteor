package openapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"gopkg.in/yaml.v3"
)

//go:embed README.md
var summary string

// Config holds the extractor configuration.
type Config struct {
	// Path or URL to the OpenAPI/gRPC spec file. Supports globs for local files.
	Source string `json:"source" yaml:"source" mapstructure:"source" validate:"required"`
	// Format: "openapi" or "protobuf" (auto-detected from file extension if omitted).
	Format string `json:"format" yaml:"format" mapstructure:"format" validate:"omitempty,oneof=openapi protobuf"`
	// Service name override (defaults to info.title for OpenAPI or package name for proto).
	Service string `json:"service" yaml:"service" mapstructure:"service"`
}

var sampleConfig = `
# Path or URL to the spec file (required). Supports globs for local files.
source: ./specs/petstore.yaml
# Format: "openapi" or "protobuf" (auto-detected if omitted)
format: openapi
# Service name override (optional)
service: petstore`

var info = plugins.Info{
	Description:  "API schema metadata from OpenAPI and gRPC definitions.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"api", "schema"},
	Entities: []plugins.EntityInfo{
		{Type: "api", URNPattern: "urn:openapi:{scope}:api:{service_name}"},
	},
}

// Extractor extracts metadata from OpenAPI and protobuf schema files.
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
	client *Client
}

// New creates a new openapi extractor.
func New(logger log.Logger) *Extractor {
	e := &Extractor{logger: logger}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	return e
}

// Init initialises the extractor with the given config.
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}
	e.client = NewClient()
	return nil
}

// Extract reads the spec source(s) and emits records.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	src := e.config.Source

	// If the source is a URL, fetch and parse it directly.
	if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") {
		data, err := e.client.Fetch(ctx, src)
		if err != nil {
			return fmt.Errorf("fetch spec from URL: %w", err)
		}
		format := e.detectFormat(src)
		return e.processSpec(data, format, emit)
	}

	// Local file path; support globs.
	matches, err := filepath.Glob(src)
	if err != nil {
		return fmt.Errorf("invalid glob pattern %q: %w", src, err)
	}
	if len(matches) == 0 {
		return fmt.Errorf("no files matched source %q", src)
	}

	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read file %s: %w", path, err)
		}
		format := e.detectFormat(path)
		if err := e.processSpec(data, format, emit); err != nil {
			e.logger.Warn("failed to process spec, skipping", "path", path, "error", err)
		}
	}

	return nil
}

// detectFormat determines the spec format from the config or file extension.
func (e *Extractor) detectFormat(path string) string {
	if e.config.Format != "" {
		return e.config.Format
	}
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".proto" {
		return "protobuf"
	}
	return "openapi"
}

// processSpec parses a single spec file and emits a record.
func (e *Extractor) processSpec(data []byte, format string, emit plugins.Emit) error {
	switch format {
	case "protobuf":
		return e.processProtobuf(data, emit)
	default:
		return e.processOpenAPI(data, emit)
	}
}

// processOpenAPI parses an OpenAPI v2 or v3 spec and emits a record.
func (e *Extractor) processOpenAPI(data []byte, emit plugins.Emit) error {
	var spec map[string]any
	// Try YAML first (which is a superset of JSON).
	if err := yaml.Unmarshal(data, &spec); err != nil {
		// Fall back to JSON.
		if err2 := json.Unmarshal(data, &spec); err2 != nil {
			return fmt.Errorf("parse OpenAPI spec: %w", err)
		}
	}

	isV2 := false
	formatLabel := "openapi_v3"
	if sw, ok := spec["swagger"]; ok {
		if s, ok := sw.(string); ok && strings.HasPrefix(s, "2") {
			isV2 = true
			formatLabel = "openapi_v2"
		}
	}

	// Extract service name.
	serviceName := e.config.Service
	if serviceName == "" {
		serviceName = extractString(spec, "info", "title")
	}
	if serviceName == "" {
		serviceName = "unknown"
	}

	// Extract description.
	description := extractString(spec, "info", "description")

	// Extract version.
	version := extractString(spec, "info", "version")

	// Extract endpoints.
	endpoints := e.extractEndpoints(spec, isV2)

	// Count schemas.
	schemaCount := 0
	if isV2 {
		if defs, ok := spec["definitions"].(map[string]any); ok {
			schemaCount = len(defs)
		}
	} else {
		if components, ok := spec["components"].(map[string]any); ok {
			if schemas, ok := components["schemas"].(map[string]any); ok {
				schemaCount = len(schemas)
			}
		}
	}

	// Extract servers.
	servers := e.extractServers(spec, isV2)

	urn := models.NewURN("openapi", e.UrnScope, "api", sanitizeName(serviceName))

	props := map[string]any{
		"version":   version,
		"endpoints": endpoints,
		"schemas":   schemaCount,
		"format":    formatLabel,
		"servers":   servers,
	}

	entity := models.NewEntity(urn, "api", serviceName, "openapi", props)
	if description != "" {
		entity.Description = description
	}

	emit(models.NewRecord(entity))
	return nil
}

// extractEndpoints pulls method+path+summary+operationId from the paths object.
func (e *Extractor) extractEndpoints(spec map[string]any, _ bool) []map[string]any {
	paths, ok := spec["paths"].(map[string]any)
	if !ok {
		return nil
	}

	httpMethods := map[string]bool{
		"get": true, "post": true, "put": true, "delete": true,
		"patch": true, "options": true, "head": true, "trace": true,
	}

	var endpoints []map[string]any
	for path, methodsRaw := range paths {
		methods, ok := methodsRaw.(map[string]any)
		if !ok {
			continue
		}
		for method, opRaw := range methods {
			if !httpMethods[strings.ToLower(method)] {
				continue
			}
			ep := map[string]any{
				"method": strings.ToUpper(method),
				"path":   path,
			}
			if op, ok := opRaw.(map[string]any); ok {
				if s, ok := op["summary"].(string); ok {
					ep["summary"] = s
				}
				if oid, ok := op["operationId"].(string); ok {
					ep["operation_id"] = oid
				}
			}
			endpoints = append(endpoints, ep)
		}
	}
	return endpoints
}

// extractServers returns server URLs depending on spec version.
func (e *Extractor) extractServers(spec map[string]any, isV2 bool) []string {
	if isV2 {
		host, _ := spec["host"].(string)
		basePath, _ := spec["basePath"].(string)
		if host != "" {
			scheme := "https"
			if schemes, ok := spec["schemes"].([]any); ok && len(schemes) > 0 {
				if s, ok := schemes[0].(string); ok {
					scheme = s
				}
			}
			return []string{scheme + "://" + host + basePath}
		}
		return nil
	}

	serversRaw, ok := spec["servers"].([]any)
	if !ok {
		return nil
	}
	var servers []string
	for _, sRaw := range serversRaw {
		if s, ok := sRaw.(map[string]any); ok {
			if u, ok := s["url"].(string); ok {
				servers = append(servers, u)
			}
		}
	}
	return servers
}

// processProtobuf parses a .proto file using regex and emits a record.
func (e *Extractor) processProtobuf(data []byte, emit plugins.Emit) error {
	content := string(data)

	// Extract syntax version.
	version := "proto3"
	if m := reSyntax.FindStringSubmatch(content); len(m) > 1 {
		version = m[1]
	}

	// Extract package name.
	pkg := ""
	if m := rePackage.FindStringSubmatch(content); len(m) > 1 {
		pkg = m[1]
	}

	serviceName := e.config.Service
	if serviceName == "" {
		serviceName = pkg
	}
	if serviceName == "" {
		serviceName = "unknown"
	}

	// Extract services and their RPCs.
	services := e.extractProtoServices(content)

	// Count message types.
	messageCount := len(reMessage.FindAllStringSubmatch(content, -1))

	urn := models.NewURN("openapi", e.UrnScope, "api", sanitizeName(serviceName))

	props := map[string]any{
		"version":  version,
		"package":  pkg,
		"services": services,
		"messages": messageCount,
		"format":   "protobuf",
	}

	entity := models.NewEntity(urn, "api", serviceName, "openapi", props)

	emit(models.NewRecord(entity))
	return nil
}

// extractProtoServices parses service blocks and their RPC methods.
func (e *Extractor) extractProtoServices(content string) []map[string]any {
	svcMatches := reService.FindAllStringSubmatchIndex(content, -1)
	var services []map[string]any

	for _, idx := range svcMatches {
		svcName := content[idx[2]:idx[3]]
		// Find the service body between braces.
		bodyStart := strings.Index(content[idx[0]:], "{")
		if bodyStart < 0 {
			continue
		}
		bodyStart += idx[0]
		braceCount := 1
		bodyEnd := bodyStart + 1
		for bodyEnd < len(content) && braceCount > 0 {
			switch content[bodyEnd] {
			case '{':
				braceCount++
			case '}':
				braceCount--
			}
			bodyEnd++
		}
		body := content[bodyStart:bodyEnd]

		rpcMatches := reRPC.FindAllStringSubmatch(body, -1)
		var methods []map[string]any
		for _, m := range rpcMatches {
			methods = append(methods, map[string]any{
				"name":        m[1],
				"input_type":  m[2],
				"output_type": m[3],
			})
		}

		services = append(services, map[string]any{
			"name":    svcName,
			"methods": methods,
		})
	}

	return services
}

// Regex patterns for protobuf parsing.
var (
	reSyntax  = regexp.MustCompile(`syntax\s*=\s*"(proto[23])"`)
	rePackage = regexp.MustCompile(`package\s+([\w.]+)\s*;`)
	reService = regexp.MustCompile(`service\s+(\w+)\s*\{`)
	reRPC     = regexp.MustCompile(`rpc\s+(\w+)\s*\(\s*([\w.]+)\s*\)\s*returns\s*\(\s*([\w.]+)\s*\)`)
	reMessage = regexp.MustCompile(`message\s+(\w+)\s*\{`)
)

// extractString navigates nested maps to extract a string value.
func extractString(m map[string]any, keys ...string) string {
	current := any(m)
	for _, k := range keys {
		cm, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = cm[k]
	}
	s, _ := current.(string)
	return s
}

// sanitizeName replaces spaces and special characters for URN-safe names.
func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "_")
	// Remove characters that are not alphanumeric, underscore, hyphen, or dot.
	safe := regexp.MustCompile(`[^a-z0-9_.\-]`)
	return safe.ReplaceAllString(name, "")
}

func init() {
	if err := registry.Extractors.Register("openapi", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
