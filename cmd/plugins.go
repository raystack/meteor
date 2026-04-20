package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/cli/printer"
	"github.com/spf13/cobra"
)

// PluginsCmd creates the top-level plugins command.
func PluginsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugins <command>",
		Short: "Discover and inspect available plugins",
		Annotations: map[string]string{
			"group": "core",
		},
	}
	cmd.AddCommand(pluginsListCmd())
	cmd.AddCommand(pluginsInfoCmd())
	return cmd
}

func pluginsListCmd() *cobra.Command {
	var (
		pluginType string
		entityType string
		edgeType   string
		tag        string
		format     string
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available plugins",
		Long: heredoc.Doc(`
			List available extractors, sinks, and processors.

			By default all plugin types are shown. Use --type to filter by
			extractor, sink, or processor. Extractors can also be filtered
			by --entity-type, --edge-type, and --tag.
		`),
		Example: heredoc.Doc(`
			$ meteor plugins list
			$ meteor plugins list --type extractor
			$ meteor plugins list --type sink
			$ meteor plugins list --entity-type table
			$ meteor plugins list --edge-type derived_from
			$ meteor plugins list --tag gcp
			$ meteor plugins list --format json
		`),
		Annotations: map[string]string{
			"group": "core",
		},
		Run: func(cmd *cobra.Command, args []string) {
			showExtractors := pluginType == "" || pluginType == "extractor"
			showSinks := pluginType == "" || pluginType == "sink"
			showProcessors := pluginType == "" || pluginType == "processor"

			// If entity/edge/tag filters are set, only show extractors.
			if entityType != "" || edgeType != "" || tag != "" {
				showSinks = false
				showProcessors = false
				showExtractors = true
			}

			if format == "json" {
				printPluginListJSON(showExtractors, showSinks, showProcessors, entityType, edgeType, tag)
				return
			}

			if showExtractors {
				printExtractorList(entityType, edgeType, tag)
			}
			if showSinks {
				printPluginList("sinks", registry.Sinks.List())
			}
			if showProcessors {
				printPluginList("processors", registry.Processors.List())
			}
		},
	}

	cmd.Flags().StringVar(&pluginType, "type", "", "Filter by plugin type (extractor, sink, processor)")
	cmd.Flags().StringVar(&entityType, "entity-type", "", "Filter extractors by entity type (e.g. table, dashboard)")
	cmd.Flags().StringVar(&edgeType, "edge-type", "", "Filter extractors by edge type (e.g. derived_from, owned_by)")
	cmd.Flags().StringVar(&tag, "tag", "", "Filter extractors by tag (e.g. gcp, oss, database)")
	cmd.Flags().StringVarP(&format, "format", "f", "table", "Output format (table, json)")

	return cmd
}

func pluginsInfoCmd() *cobra.Command {
	var (
		full   bool
		format string
	)

	cmd := &cobra.Command{
		Use:   "info [name]",
		Short: "Display plugin information",
		Long: heredoc.Doc(`
			Display detailed information about a plugin.

			The plugin is looked up across extractors, sinks, and processors.
			If the name exists in multiple registries, all matches are shown.
		`),
		Example: heredoc.Doc(`
			$ meteor plugins info bigquery
			$ meteor plugins info compass
			$ meteor plugins info enrich
			$ meteor plugins info kafka
			$ meteor plugins info bigquery --full
			$ meteor plugins info bigquery --format json
		`),
		Args: cobra.MaximumNArgs(1),
		Annotations: map[string]string{
			"group": "core",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name, err := resolvePluginName(args)
			if err != nil {
				return err
			}

			if format == "json" {
				return printPluginInfoJSON(name)
			}

			found := false

			if info, err := registry.Extractors.Info(name); err == nil {
				found = true
				if full {
					fmt.Printf("\n%s\n", printer.Greenf("Extractor: %s", name))
					if err := printFullSummary(info.Summary); err != nil {
						return err
					}
				} else {
					printStructuredPluginInfo(name, "extractor", info)
				}
			}

			if info, err := registry.Sinks.Info(name); err == nil {
				found = true
				if full {
					fmt.Printf("\n%s\n", printer.Greenf("Sink: %s", name))
					if err := printFullSummary(info.Summary); err != nil {
						return err
					}
				} else {
					printStructuredPluginInfo(name, "sink", info)
				}
			}

			if info, err := registry.Processors.Info(name); err == nil {
				found = true
				if full {
					fmt.Printf("\n%s\n", printer.Greenf("Processor: %s", name))
					if err := printFullSummary(info.Summary); err != nil {
						return err
					}
				} else {
					printStructuredPluginInfo(name, "processor", info)
				}
			}

			if !found {
				fmt.Println(printer.Redf("ERROR: plugin %q not found", name))
				fmt.Println(printer.Bluef("\nUse 'meteor plugins list' to see available plugins."))
			}

			return nil
		},
	}

	cmd.Flags().BoolVar(&full, "full", false, "Show full markdown documentation")
	cmd.Flags().StringVarP(&format, "format", "f", "table", "Output format (table, json)")

	return cmd
}

func resolvePluginName(args []string) (string, error) {
	if len(args) == 1 {
		return args[0], nil
	}

	// Build combined list of all plugin names.
	names := make(map[string]bool)
	for n := range registry.Extractors.List() {
		names[n] = true
	}
	for n := range registry.Sinks.List() {
		names[n] = true
	}
	for n := range registry.Processors.List() {
		names[n] = true
	}

	options := make([]string, 0, len(names))
	for n := range names {
		options = append(options, n)
	}
	sort.Strings(options)

	var name string
	if err := survey.AskOne(&survey.Select{
		Message: "Select a plugin",
		Options: options,
	}, &name); err != nil {
		return "", err
	}
	return name, nil
}

func printExtractorList(entityType, edgeType, tag string) {
	extractors := registry.Extractors.List()

	type entry struct {
		name string
		info plugins.Info
	}

	var filtered []entry
	for n, i := range extractors {
		if !matchFilters(i, entityType, edgeType, tag) {
			continue
		}
		filtered = append(filtered, entry{name: n, info: i})
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].name < filtered[j].name
	})

	fmt.Printf("\n%s %d of %d extractors\n\n", printer.Greenf("Showing"), len(filtered), len(extractors))

	report := [][]string{}
	for idx, e := range filtered {
		entities := entityTypeNames(e.info.Entities)
		edges := edgeTypeNames(e.info.Edges)

		report = append(report, []string{
			printer.Greenf("#%02d", idx+1),
			e.name,
			e.info.Description,
			printer.Cyanf("%s", entities),
			printer.Yellowf("%s", edges),
			printer.Greyf("(%s)", strings.Join(e.info.Tags, ", ")),
		})
	}
	printer.Table(os.Stdout, report)
}

func printPluginList(label string, items map[string]plugins.Info) {
	type entry struct {
		name string
		info plugins.Info
	}

	var sorted []entry
	for n, i := range items {
		sorted = append(sorted, entry{name: n, info: i})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].name < sorted[j].name
	})

	fmt.Printf("\n%s %d %s\n\n", printer.Greenf("Showing"), len(sorted), label)

	report := [][]string{}
	for idx, e := range sorted {
		report = append(report, []string{
			printer.Greenf("#%02d", idx+1),
			e.name,
			e.info.Description,
			printer.Greyf("(%s)", strings.Join(e.info.Tags, ", ")),
		})
	}
	printer.Table(os.Stdout, report)
}

// pluginJSON is used for JSON output of plugin information.
type pluginJSON struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Description string   `json:"description"`
	Tags        []string `json:"tags,omitempty"`
	Entities    []string `json:"entities,omitempty"`
	Edges       []string `json:"edges,omitempty"`
}

// pluginInfoJSON is used for detailed JSON output of a single plugin.
type pluginInfoJSON struct {
	Name         string             `json:"name"`
	Type         string             `json:"type"`
	Description  string             `json:"description"`
	Tags         []string           `json:"tags,omitempty"`
	Entities     []plugins.EntityInfo `json:"entities,omitempty"`
	Edges        []plugins.EdgeInfo   `json:"edges,omitempty"`
	SampleConfig string             `json:"sample_config,omitempty"`
}

func printPluginListJSON(showExtractors, showSinks, showProcessors bool, entityType, edgeType, tag string) {
	var result []pluginJSON

	if showExtractors {
		for n, i := range registry.Extractors.List() {
			if !matchFilters(i, entityType, edgeType, tag) {
				continue
			}
			result = append(result, pluginJSON{
				Name:        n,
				Type:        "extractor",
				Description: i.Description,
				Tags:        i.Tags,
				Entities:    entityTypeList(i.Entities),
				Edges:       edgeTypeList(i.Edges),
			})
		}
	}
	if showSinks {
		for n, i := range registry.Sinks.List() {
			result = append(result, pluginJSON{
				Name:        n,
				Type:        "sink",
				Description: i.Description,
				Tags:        i.Tags,
			})
		}
	}
	if showProcessors {
		for n, i := range registry.Processors.List() {
			result = append(result, pluginJSON{
				Name:        n,
				Type:        "processor",
				Description: i.Description,
				Tags:        i.Tags,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Type != result[j].Type {
			return result[i].Type < result[j].Type
		}
		return result[i].Name < result[j].Name
	})

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(result)
}

func printPluginInfoJSON(name string) error {
	var result []pluginInfoJSON

	if info, err := registry.Extractors.Info(name); err == nil {
		result = append(result, pluginInfoJSON{
			Name:         name,
			Type:         "extractor",
			Description:  info.Description,
			Tags:         info.Tags,
			Entities:     info.Entities,
			Edges:        info.Edges,
			SampleConfig: info.SampleConfig,
		})
	}
	if info, err := registry.Sinks.Info(name); err == nil {
		result = append(result, pluginInfoJSON{
			Name:         name,
			Type:         "sink",
			Description:  info.Description,
			Tags:         info.Tags,
			Entities:     info.Entities,
			Edges:        info.Edges,
			SampleConfig: info.SampleConfig,
		})
	}
	if info, err := registry.Processors.Info(name); err == nil {
		result = append(result, pluginInfoJSON{
			Name:         name,
			Type:         "processor",
			Description:  info.Description,
			Tags:         info.Tags,
			Entities:     info.Entities,
			Edges:        info.Edges,
			SampleConfig: info.SampleConfig,
		})
	}

	if len(result) == 0 {
		return fmt.Errorf("plugin %q not found", name)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func entityTypeList(entities []plugins.EntityInfo) []string {
	if len(entities) == 0 {
		return nil
	}
	names := make([]string, len(entities))
	for i, e := range entities {
		names[i] = e.Type
	}
	return names
}

func edgeTypeList(edges []plugins.EdgeInfo) []string {
	if len(edges) == 0 {
		return nil
	}
	names := make([]string, len(edges))
	for i, e := range edges {
		names[i] = e.Type
	}
	return names
}

func printStructuredPluginInfo(name, pluginType string, info plugins.Info) {
	fmt.Println()
	fmt.Printf("  %s %s\n", printer.Greenf("%s", name), printer.Greyf("[%s]", pluginType))
	fmt.Printf("  %s\n", info.Description)
	fmt.Println()

	if len(info.Entities) > 0 {
		fmt.Println(printer.Cyanf("  Entities:"))
		for _, e := range info.Entities {
			fmt.Printf("    %-20s %s\n", e.Type, printer.Greyf("%s", e.URNPattern))
		}
		fmt.Println()
	}

	if len(info.Edges) > 0 {
		fmt.Println(printer.Yellowf("  Edges:"))
		for _, e := range info.Edges {
			fmt.Printf("    %-20s %s -> %s\n", e.Type, e.From, e.To)
		}
		fmt.Println()
	}

	if len(info.Tags) > 0 {
		fmt.Printf("  %s %s\n", printer.Greyf("Tags:"), strings.Join(info.Tags, ", "))
		fmt.Println()
	}

	if info.SampleConfig != "" {
		fmt.Println(printer.Greyf("  Sample Config:"))
		for _, line := range strings.Split(strings.TrimSpace(info.SampleConfig), "\n") {
			fmt.Printf("    %s\n", strings.TrimLeft(line, " \t"))
		}
		fmt.Println()
	}

	fmt.Printf("  Use %s for full documentation.\n\n", printer.Bluef("--full"))
}

func matchFilters(info plugins.Info, entityType, edgeType, tag string) bool {
	if entityType != "" {
		found := false
		for _, e := range info.Entities {
			if e.Type == entityType {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if edgeType != "" {
		found := false
		for _, e := range info.Edges {
			if e.Type == edgeType {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if tag != "" {
		found := false
		for _, t := range info.Tags {
			if t == tag {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func entityTypeNames(entities []plugins.EntityInfo) string {
	if len(entities) == 0 {
		return "-"
	}
	names := make([]string, len(entities))
	for i, e := range entities {
		names[i] = e.Type
	}
	return strings.Join(names, ", ")
}

func edgeTypeNames(edges []plugins.EdgeInfo) string {
	if len(edges) == 0 {
		return "-"
	}
	names := make([]string, len(edges))
	for i, e := range edges {
		names[i] = e.Type
	}
	return strings.Join(names, ", ")
}

func printFullSummary(summary string) error {
	out, err := printer.MarkdownWithWrap(summary, 130)
	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}
