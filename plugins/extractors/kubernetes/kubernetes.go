package kubernetes

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

type Config struct {
	Kubeconfig string   `mapstructure:"kubeconfig"`
	Namespaces []string `mapstructure:"namespaces"`
	Extract    []string `mapstructure:"extract" validate:"omitempty,dive,oneof=namespaces deployments services pods jobs"`
	Exclude    []string `mapstructure:"exclude"`
}

var sampleConfig = `
# Path to kubeconfig file (optional, auto-detected if omitted)
kubeconfig: ~/.kube/config
# Namespaces to extract from (optional, defaults to all)
namespaces:
  - default
  - production
# Resource types to extract (optional, defaults to all except pods)
extract:
  - namespaces
  - deployments
  - services
  - jobs
# Namespaces to exclude (optional)
exclude:
  - kube-system
  - kube-public`

var defaultExtract = []string{"namespaces", "deployments", "services", "jobs"}

var info = plugins.Info{
	Description:  "Infrastructure topology from a Kubernetes cluster.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "infrastructure"},
	Entities: []plugins.EntityInfo{
		{Type: "namespace", URNPattern: "urn:kubernetes:{scope}:namespace:{name}"},
		{Type: "deployment", URNPattern: "urn:kubernetes:{scope}:deployment:{namespace}/{name}"},
		{Type: "service", URNPattern: "urn:kubernetes:{scope}:service:{namespace}/{name}"},
		{Type: "job", URNPattern: "urn:kubernetes:{scope}:job:{namespace}/{name}"},
	},
	Edges: []plugins.EdgeInfo{
		{Type: "belongs_to", From: "deployment", To: "namespace"},
		{Type: "belongs_to", From: "service", To: "namespace"},
		{Type: "belongs_to", From: "job", To: "namespace"},
	},
}

type Extractor struct {
	plugins.BaseExtractor
	logger  log.Logger
	config  Config
	client  *Client
	exclude map[string]bool
}

func New(logger log.Logger) *Extractor {
	e := &Extractor{logger: logger}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}
	client, err := NewClient(e.config.Kubeconfig)
	if err != nil {
		return fmt.Errorf("create kubernetes client: %w", err)
	}
	e.client = client
	e.exclude = make(map[string]bool, len(e.config.Exclude))
	for _, ns := range e.config.Exclude {
		e.exclude[ns] = true
	}
	return nil
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	extract := e.config.Extract
	if len(extract) == 0 {
		extract = defaultExtract
	}
	should := make(map[string]bool, len(extract))
	for _, r := range extract {
		should[r] = true
	}

	namespaces := e.config.Namespaces
	if len(namespaces) == 0 {
		nsList, err := e.client.ListNamespaces(ctx)
		if err != nil {
			return fmt.Errorf("list namespaces: %w", err)
		}
		for _, item := range getItems(nsList) {
			name := getStr(getMeta(item), "name")
			if name != "" && !e.exclude[name] {
				namespaces = append(namespaces, name)
			}
		}
	}

	if should["namespaces"] {
		e.extractNamespaces(ctx, emit, namespaces)
	}
	if should["deployments"] {
		e.extractDeployments(ctx, emit, namespaces)
	}
	if should["services"] {
		e.extractServices(ctx, emit, namespaces)
	}
	if should["pods"] {
		e.extractPods(ctx, emit, namespaces)
	}
	if should["jobs"] {
		e.extractJobs(ctx, emit, namespaces)
	}
	return nil
}

func (e *Extractor) extractNamespaces(ctx context.Context, emit plugins.Emit, namespaces []string) {
	result, err := e.client.ListNamespaces(ctx)
	if err != nil {
		e.logger.Warn("failed to list namespaces", "error", err)
		return
	}
	for _, item := range getItems(result) {
		meta := getMeta(item)
		name := getStr(meta, "name")
		if name == "" || e.exclude[name] || !contains(namespaces, name) {
			continue
		}
		urn := models.NewURN("kubernetes", e.UrnScope, "namespace", name)
		props := map[string]any{"labels": getMap(meta, "labels"), "created_at": getStr(meta, "creationTimestamp")}
		if s := getNestedStr(item, "status", "phase"); s != "" {
			props["status"] = s
		}
		emit(models.NewRecord(models.NewEntity(urn, "namespace", name, "kubernetes", props)))
	}
}

func (e *Extractor) extractDeployments(ctx context.Context, emit plugins.Emit, namespaces []string) {
	for _, ns := range namespaces {
		result, err := e.client.ListDeployments(ctx, ns)
		if err != nil {
			e.logger.Warn("failed to list deployments", "namespace", ns, "error", err)
			continue
		}
		for _, item := range getItems(result) {
			meta := getMeta(item)
			name := getStr(meta, "name")
			if name == "" {
				continue
			}
			fqn := ns + "/" + name
			urn := models.NewURN("kubernetes", e.UrnScope, "deployment", fqn)
			nsURN := models.NewURN("kubernetes", e.UrnScope, "namespace", ns)
			spec, status := getMapField(item, "spec"), getMapField(item, "status")
			props := map[string]any{
				"namespace": ns, "labels": getMap(meta, "labels"), "created_at": getStr(meta, "creationTimestamp"),
				"replicas": getNum(spec, "replicas"), "ready_replicas": getNum(status, "readyReplicas"),
				"containers": extractContainers(spec),
			}
			if s := getNestedStr(spec, "strategy", "type"); s != "" {
				props["strategy"] = s
			}
			emit(models.NewRecord(models.NewEntity(urn, "deployment", name, "kubernetes", props),
				&meteorv1beta1.Edge{SourceUrn: urn, TargetUrn: nsURN, Type: "belongs_to", Source: "kubernetes"}))
		}
	}
}

func (e *Extractor) extractServices(ctx context.Context, emit plugins.Emit, namespaces []string) {
	for _, ns := range namespaces {
		result, err := e.client.ListServices(ctx, ns)
		if err != nil {
			e.logger.Warn("failed to list services", "namespace", ns, "error", err)
			continue
		}
		for _, item := range getItems(result) {
			meta := getMeta(item)
			name := getStr(meta, "name")
			if name == "" {
				continue
			}
			fqn := ns + "/" + name
			urn := models.NewURN("kubernetes", e.UrnScope, "service", fqn)
			nsURN := models.NewURN("kubernetes", e.UrnScope, "namespace", ns)
			spec := getMapField(item, "spec")
			props := map[string]any{
				"namespace": ns, "labels": getMap(meta, "labels"), "created_at": getStr(meta, "creationTimestamp"),
				"type": getStr(spec, "type"), "cluster_ip": getStr(spec, "clusterIP"),
				"selector": getMap(spec, "selector"), "ports": extractPorts(spec),
			}
			emit(models.NewRecord(models.NewEntity(urn, "service", name, "kubernetes", props),
				&meteorv1beta1.Edge{SourceUrn: urn, TargetUrn: nsURN, Type: "belongs_to", Source: "kubernetes"}))
		}
	}
}

func (e *Extractor) extractPods(ctx context.Context, emit plugins.Emit, namespaces []string) {
	for _, ns := range namespaces {
		result, err := e.client.ListPods(ctx, ns)
		if err != nil {
			e.logger.Warn("failed to list pods", "namespace", ns, "error", err)
			continue
		}
		for _, item := range getItems(result) {
			meta := getMeta(item)
			name := getStr(meta, "name")
			if name == "" {
				continue
			}
			fqn := ns + "/" + name
			urn := models.NewURN("kubernetes", e.UrnScope, "pod", fqn)
			nsURN := models.NewURN("kubernetes", e.UrnScope, "namespace", ns)
			spec, status := getMapField(item, "spec"), getMapField(item, "status")
			props := map[string]any{
				"namespace": ns, "labels": getMap(meta, "labels"), "created_at": getStr(meta, "creationTimestamp"),
				"status": getStr(status, "phase"), "node_name": getStr(spec, "nodeName"),
				"containers": extractContainersFromSpec(spec),
			}
			emit(models.NewRecord(models.NewEntity(urn, "pod", name, "kubernetes", props),
				&meteorv1beta1.Edge{SourceUrn: urn, TargetUrn: nsURN, Type: "belongs_to", Source: "kubernetes"}))
		}
	}
}

func (e *Extractor) extractJobs(ctx context.Context, emit plugins.Emit, namespaces []string) {
	for _, ns := range namespaces {
		result, err := e.client.ListJobs(ctx, ns)
		if err != nil {
			e.logger.Warn("failed to list jobs", "namespace", ns, "error", err)
			continue
		}
		for _, item := range getItems(result) {
			meta := getMeta(item)
			name := getStr(meta, "name")
			if name == "" {
				continue
			}
			fqn := ns + "/" + name
			urn := models.NewURN("kubernetes", e.UrnScope, "job", fqn)
			nsURN := models.NewURN("kubernetes", e.UrnScope, "namespace", ns)
			spec, status := getMapField(item, "spec"), getMapField(item, "status")
			props := map[string]any{
				"namespace": ns, "labels": getMap(meta, "labels"), "created_at": getStr(meta, "creationTimestamp"),
				"completions": getNum(spec, "completions"), "active": getNum(status, "active"),
				"succeeded": getNum(status, "succeeded"), "failed": getNum(status, "failed"),
			}
			emit(models.NewRecord(models.NewEntity(urn, "job", name, "kubernetes", props),
				&meteorv1beta1.Edge{SourceUrn: urn, TargetUrn: nsURN, Type: "belongs_to", Source: "kubernetes"}))
		}
	}
}

// --- JSON helpers for untyped K8s API responses ---

func getItems(m map[string]any) []map[string]any {
	items, ok := m["items"].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, v := range items {
		if im, ok := v.(map[string]any); ok {
			out = append(out, im)
		}
	}
	return out
}

func getMeta(item map[string]any) map[string]any { return getMapField(item, "metadata") }

func getMapField(m map[string]any, key string) map[string]any {
	if v, ok := m[key].(map[string]any); ok {
		return v
	}
	return map[string]any{}
}

func getStr(m map[string]any, key string) string { s, _ := m[key].(string); return s }

func getNestedStr(m map[string]any, keys ...string) string {
	cur := m
	for i, k := range keys {
		if i == len(keys)-1 {
			return getStr(cur, k)
		}
		cur = getMapField(cur, k)
	}
	return ""
}

func getNum(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func getMap(m map[string]any, key string) map[string]any { v, _ := m[key].(map[string]any); return v }

func extractContainers(spec map[string]any) []map[string]any {
	return extractContainersFromSpec(getMapField(getMapField(spec, "template"), "spec"))
}

func extractContainersFromSpec(podSpec map[string]any) []map[string]any {
	cs, ok := podSpec["containers"].([]any)
	if !ok {
		return nil
	}
	var out []map[string]any
	for _, c := range cs {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, map[string]any{"name": getStr(cm, "name"), "image": getStr(cm, "image")})
	}
	return out
}

func extractPorts(spec map[string]any) []map[string]any {
	ps, ok := spec["ports"].([]any)
	if !ok {
		return nil
	}
	var out []map[string]any
	for _, p := range ps {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		port := map[string]any{"port": getNum(pm, "port"), "protocol": getStr(pm, "protocol")}
		if tp := getNum(pm, "targetPort"); tp != 0 {
			port["target_port"] = tp
		}
		if n := getStr(pm, "name"); n != "" {
			port["name"] = n
		}
		out = append(out, port)
	}
	return out
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

func init() {
	if err := registry.Extractors.Register("kubernetes", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
