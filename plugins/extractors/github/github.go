package github

import (
	"context"
	_ "embed"
	"fmt"
	"path/filepath"
	"strings"

	gh "github.com/google/go-github/v68/github"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

type Config struct {
	Org     string     `json:"org" yaml:"org" mapstructure:"org" validate:"required"`
	Token   string     `json:"token" yaml:"token" mapstructure:"token" validate:"required"`
	Extract []string   `json:"extract" yaml:"extract" mapstructure:"extract" validate:"omitempty,dive,oneof=users repositories teams documents collaborators"`
	Docs    DocsConfig `json:"docs" yaml:"docs" mapstructure:"docs"`
}

type DocsConfig struct {
	Repos   []string `json:"repos" yaml:"repos" mapstructure:"repos"`
	Paths   []string `json:"paths" yaml:"paths" mapstructure:"paths"`
	Pattern string   `json:"pattern" yaml:"pattern" mapstructure:"pattern"`
}

var sampleConfig = `
org: raystack
token: github_token
# extract specifies which entity types to extract.
# Defaults to all: ["users", "repositories", "teams", "documents", "collaborators"]
extract:
  - users
  - repositories
  - teams
  - documents
  - collaborators
# docs configures document extraction (only used when "documents" is in extract).
docs:
  # repos limits which repositories to scan. If empty, scans all org repos.
  repos: []
  # paths specifies directory paths to scan for documents. Defaults to ["docs"].
  paths:
    - docs
  # pattern is a glob pattern to match files. Defaults to "*.md".
  pattern: "*.md"`

var info = plugins.Info{
	Description:  "Metadata from a GitHub organisation.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"saas", "scm"},
	Entities: []plugins.EntityInfo{
		{Type: "user", URNPattern: "urn:github:{scope}:user:{user_id}"},
		{Type: "repository", URNPattern: "urn:github:{scope}:repository:{repo_id}"},
		{Type: "team", URNPattern: "urn:github:{scope}:team:{team_id}"},
		{Type: "document", URNPattern: "urn:github:{scope}:document:{doc_id}"},
	},
	Edges: []plugins.EdgeInfo{
		{Type: "member_of", From: "user", To: "team"},
		{Type: "owned_by", From: "repository", To: "user"},
		{Type: "belongs_to", From: "repository", To: "team"},
		{Type: "has_access_to", From: "user", To: "repository"},
	},
}

type Extractor struct {
	plugins.BaseExtractor
	logger  log.Logger
	config  Config
	client  *gh.Client
	extract map[string]bool
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

	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: e.config.Token})
	tc := oauth2.NewClient(ctx, ts)
	e.client = gh.NewClient(tc)

	e.extract = map[string]bool{
		"users":         true,
		"repositories":  true,
		"teams":         true,
		"documents":     true,
		"collaborators": true,
	}
	if len(e.config.Extract) > 0 {
		e.extract = make(map[string]bool, len(e.config.Extract))
		for _, v := range e.config.Extract {
			e.extract[v] = true
		}
	}

	return nil
}

// SetBaseURL overrides the GitHub API base URL (used for testing).
func (e *Extractor) SetBaseURL(url string) {
	e.client.BaseURL, _ = e.client.BaseURL.Parse(url + "/api/v3/")
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	if e.extract["users"] {
		if err := e.extractUsers(ctx, emit); err != nil {
			return fmt.Errorf("extract users: %w", err)
		}
	}
	if e.extract["repositories"] {
		if err := e.extractRepositories(ctx, emit); err != nil {
			return fmt.Errorf("extract repositories: %w", err)
		}
	}
	if e.extract["teams"] {
		if err := e.extractTeams(ctx, emit); err != nil {
			return fmt.Errorf("extract teams: %w", err)
		}
	}
	if e.extract["documents"] {
		if err := e.extractDocuments(ctx, emit); err != nil {
			return fmt.Errorf("extract documents: %w", err)
		}
	}
	if e.extract["collaborators"] {
		if err := e.extractCollaborators(ctx, emit); err != nil {
			return fmt.Errorf("extract collaborators: %w", err)
		}
	}
	return nil
}

func (e *Extractor) extractUsers(ctx context.Context, emit plugins.Emit) error {
	opts := &gh.ListMembersOptions{
		ListOptions: gh.ListOptions{PerPage: 100},
	}
	for {
		members, resp, err := e.client.Organizations.ListMembers(ctx, e.config.Org, opts)
		if err != nil {
			return fmt.Errorf("list members: %w", err)
		}

		for _, member := range members {
			usr, _, err := e.client.Users.Get(ctx, member.GetLogin())
			if err != nil {
				e.logger.Warn("failed to fetch user, skipping", "login", member.GetLogin(), "error", err)
				continue
			}
			emit(e.buildUserRecord(usr))
		}

		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return nil
}

func (e *Extractor) buildUserRecord(usr *gh.User) models.Record {
	urn := models.NewURN("github", e.UrnScope, "user", usr.GetNodeID())
	props := map[string]any{
		"email":      usr.GetEmail(),
		"username":   usr.GetLogin(),
		"full_name":  usr.GetName(),
		"company":    usr.GetCompany(),
		"location":   usr.GetLocation(),
		"bio":        usr.GetBio(),
		"avatar_url": usr.GetAvatarURL(),
		"html_url":   usr.GetHTMLURL(),
		"status":     "active",
	}

	entity := models.NewEntity(urn, "user", usr.GetName(), "github", props)
	var edges []*meteorv1beta1.Edge
	edges = append(edges, &meteorv1beta1.Edge{
		SourceUrn: urn,
		TargetUrn: models.NewURN("github", e.UrnScope, "org", e.config.Org),
		Type:      "member_of",
		Source:    "github",
	})
	return models.NewRecord(entity, edges...)
}

func (e *Extractor) extractRepositories(ctx context.Context, emit plugins.Emit) error {
	opts := &gh.RepositoryListByOrgOptions{
		ListOptions: gh.ListOptions{PerPage: 100},
	}
	for {
		repos, resp, err := e.client.Repositories.ListByOrg(ctx, e.config.Org, opts)
		if err != nil {
			return fmt.Errorf("list repositories: %w", err)
		}

		for _, repo := range repos {
			emit(e.buildRepoRecord(repo))
		}

		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return nil
}

func (e *Extractor) buildRepoRecord(repo *gh.Repository) models.Record {
	urn := models.NewURN("github", e.UrnScope, "repository", repo.GetNodeID())
	props := map[string]any{
		"full_name":     repo.GetFullName(),
		"description":   repo.GetDescription(),
		"html_url":      repo.GetHTMLURL(),
		"language":      repo.GetLanguage(),
		"visibility":    repo.GetVisibility(),
		"default_branch": repo.GetDefaultBranch(),
		"archived":      repo.GetArchived(),
		"fork":          repo.GetFork(),
		"stargazers":    repo.GetStargazersCount(),
		"forks":         repo.GetForksCount(),
		"open_issues":   repo.GetOpenIssuesCount(),
	}
	if len(repo.Topics) > 0 {
		props["topics"] = repo.Topics
	}

	entity := models.NewEntity(urn, "repository", repo.GetName(), "github", props)

	var edges []*meteorv1beta1.Edge
	if owner := repo.GetOwner(); owner != nil {
		edges = append(edges, models.OwnerEdge(
			urn,
			models.NewURN("github", e.UrnScope, "user", owner.GetNodeID()),
			"github",
		))
	}

	return models.NewRecord(entity, edges...)
}

func (e *Extractor) extractTeams(ctx context.Context, emit plugins.Emit) error {
	opts := &gh.ListOptions{PerPage: 100}
	for {
		teams, resp, err := e.client.Teams.ListTeams(ctx, e.config.Org, opts)
		if err != nil {
			return fmt.Errorf("list teams: %w", err)
		}

		for _, team := range teams {
			record, err := e.buildTeamRecord(ctx, team)
			if err != nil {
				e.logger.Warn("failed to build team record, skipping", "team", team.GetSlug(), "error", err)
				continue
			}
			emit(record)
		}

		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return nil
}

func (e *Extractor) buildTeamRecord(ctx context.Context, team *gh.Team) (models.Record, error) {
	urn := models.NewURN("github", e.UrnScope, "team", team.GetNodeID())
	props := map[string]any{
		"slug":        team.GetSlug(),
		"description": team.GetDescription(),
		"privacy":     team.GetPrivacy(),
		"permission":  team.GetPermission(),
		"html_url":    fmt.Sprintf("https://github.com/orgs/%s/teams/%s", e.config.Org, team.GetSlug()),
	}

	entity := models.NewEntity(urn, "team", team.GetName(), "github", props)

	var edges []*meteorv1beta1.Edge

	// Fetch team members and create member_of edges.
	memberOpts := &gh.TeamListTeamMembersOptions{
		ListOptions: gh.ListOptions{PerPage: 100},
	}
	for {
		members, resp, err := e.client.Teams.ListTeamMembersBySlug(ctx, e.config.Org, team.GetSlug(), memberOpts)
		if err != nil {
			return models.Record{}, fmt.Errorf("list team members for %s: %w", team.GetSlug(), err)
		}

		for _, member := range members {
			edges = append(edges, &meteorv1beta1.Edge{
				SourceUrn: models.NewURN("github", e.UrnScope, "user", member.GetNodeID()),
				TargetUrn: urn,
				Type:      "member_of",
				Source:    "github",
			})
		}

		if resp.NextPage == 0 {
			break
		}
		memberOpts.Page = resp.NextPage
	}

	return models.NewRecord(entity, edges...), nil
}

func (e *Extractor) extractDocuments(ctx context.Context, emit plugins.Emit) error {
	paths := e.config.Docs.Paths
	if len(paths) == 0 {
		paths = []string{"docs"}
	}
	pattern := e.config.Docs.Pattern
	if pattern == "" {
		pattern = "*.md"
	}

	repos, err := e.listDocRepos(ctx)
	if err != nil {
		return err
	}

	for _, repo := range repos {
		repoURN := models.NewURN("github", e.UrnScope, "repository", repo.GetNodeID())
		for _, dir := range paths {
			if err := e.extractDocsFromPath(ctx, emit, repo, repoURN, dir, pattern); err != nil {
				e.logger.Warn("failed to extract docs from path, skipping",
					"repo", repo.GetFullName(), "path", dir, "error", err)
			}
		}
	}
	return nil
}

func (e *Extractor) listDocRepos(ctx context.Context) ([]*gh.Repository, error) {
	if len(e.config.Docs.Repos) > 0 {
		var repos []*gh.Repository
		for _, name := range e.config.Docs.Repos {
			repo, _, err := e.client.Repositories.Get(ctx, e.config.Org, name)
			if err != nil {
				e.logger.Warn("failed to get repo for docs, skipping", "repo", name, "error", err)
				continue
			}
			repos = append(repos, repo)
		}
		return repos, nil
	}

	// Fall back to all org repos.
	var all []*gh.Repository
	opts := &gh.RepositoryListByOrgOptions{
		ListOptions: gh.ListOptions{PerPage: 100},
	}
	for {
		repos, resp, err := e.client.Repositories.ListByOrg(ctx, e.config.Org, opts)
		if err != nil {
			return nil, fmt.Errorf("list repositories for docs: %w", err)
		}
		all = append(all, repos...)
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return all, nil
}

func (e *Extractor) extractDocsFromPath(ctx context.Context, emit plugins.Emit, repo *gh.Repository, repoURN, dir, pattern string) error {
	_, dirContents, _, err := e.client.Repositories.GetContents(ctx, e.config.Org, repo.GetName(), dir, nil)
	if err != nil {
		return fmt.Errorf("get contents of %s: %w", dir, err)
	}

	for _, entry := range dirContents {
		switch entry.GetType() {
		case "file":
			matched, _ := filepath.Match(pattern, entry.GetName())
			if !matched {
				continue
			}
			if err := e.emitDocument(ctx, emit, repo, repoURN, entry); err != nil {
				e.logger.Warn("failed to emit document, skipping",
					"repo", repo.GetFullName(), "path", entry.GetPath(), "error", err)
			}
		case "dir":
			if err := e.extractDocsFromPath(ctx, emit, repo, repoURN, entry.GetPath(), pattern); err != nil {
				e.logger.Warn("failed to recurse into directory, skipping",
					"repo", repo.GetFullName(), "path", entry.GetPath(), "error", err)
			}
		}
	}
	return nil
}

func (e *Extractor) emitDocument(ctx context.Context, emit plugins.Emit, repo *gh.Repository, repoURN string, entry *gh.RepositoryContent) error {
	// Fetch full file content (the directory listing doesn't include content).
	file, _, _, err := e.client.Repositories.GetContents(ctx, e.config.Org, repo.GetName(), entry.GetPath(), nil)
	if err != nil {
		return fmt.Errorf("get file %s: %w", entry.GetPath(), err)
	}

	content, err := file.GetContent()
	if err != nil {
		return fmt.Errorf("decode content of %s: %w", entry.GetPath(), err)
	}

	name := strings.TrimSuffix(entry.GetName(), filepath.Ext(entry.GetName()))
	urn := models.NewURN("github", e.UrnScope, "document", file.GetSHA())

	props := map[string]any{
		"path":      file.GetPath(),
		"file_name": file.GetName(),
		"content":   content,
		"html_url":  file.GetHTMLURL(),
		"repo":      repo.GetFullName(),
		"size":      file.GetSize(),
		"sha":       file.GetSHA(),
	}

	entity := models.NewEntity(urn, "document", name, "github", props)

	edges := []*meteorv1beta1.Edge{
		{
			SourceUrn: urn,
			TargetUrn: repoURN,
			Type:      "belongs_to",
			Source:    "github",
		},
	}

	emit(models.NewRecord(entity, edges...))
	return nil
}

func (e *Extractor) extractCollaborators(ctx context.Context, emit plugins.Emit) error {
	repoOpts := &gh.RepositoryListByOrgOptions{
		ListOptions: gh.ListOptions{PerPage: 100},
	}
	for {
		repos, resp, err := e.client.Repositories.ListByOrg(ctx, e.config.Org, repoOpts)
		if err != nil {
			return fmt.Errorf("list repositories: %w", err)
		}

		for _, repo := range repos {
			if err := e.extractRepoCollaborators(ctx, emit, repo); err != nil {
				e.logger.Warn("failed to extract collaborators, skipping",
					"repo", repo.GetFullName(), "error", err)
			}
		}

		if resp.NextPage == 0 {
			break
		}
		repoOpts.Page = resp.NextPage
	}
	return nil
}

func (e *Extractor) extractRepoCollaborators(ctx context.Context, emit plugins.Emit, repo *gh.Repository) error {
	repoURN := models.NewURN("github", e.UrnScope, "repository", repo.GetNodeID())
	opts := &gh.ListCollaboratorsOptions{
		ListOptions: gh.ListOptions{PerPage: 100},
	}

	var edges []*meteorv1beta1.Edge
	for {
		collaborators, resp, err := e.client.Repositories.ListCollaborators(ctx, e.config.Org, repo.GetName(), opts)
		if err != nil {
			return fmt.Errorf("list collaborators for %s: %w", repo.GetName(), err)
		}

		for _, collab := range collaborators {
			userURN := models.NewURN("github", e.UrnScope, "user", collab.GetNodeID())
			props, _ := structpb.NewStruct(map[string]any{
				"permission": resolvePermission(collab.GetPermissions()),
			})
			edges = append(edges, &meteorv1beta1.Edge{
				SourceUrn:  userURN,
				TargetUrn:  repoURN,
				Type:       "has_access_to",
				Source:     "github",
				Properties: props,
			})
		}

		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	if len(edges) > 0 {
		entity := models.NewEntity(repoURN, "repository", repo.GetName(), "github", nil)
		emit(models.NewRecord(entity, edges...))
	}
	return nil
}

// resolvePermission returns the highest permission level from the permissions map.
func resolvePermission(perms map[string]bool) string {
	for _, level := range []string{"admin", "maintain", "push", "triage", "pull"} {
		if perms[level] {
			return level
		}
	}
	return "pull"
}

func init() {
	if err := registry.Extractors.Register("github", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
