package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/cli/printer"
	"github.com/spf13/cobra"
)

// EntitiesCmd creates a command to list entity types across all extractors.
func EntitiesCmd() *cobra.Command {
	var format string

	cmd := &cobra.Command{
		Use:   "entities",
		Short: "List entity types across all extractors",
		Long: heredoc.Doc(`
			List all entity types produced by extractors.

			Shows each entity type and which extractors produce it.
		`),
		Example: heredoc.Doc(`
			$ meteor entities
			$ meteor entities --format json
		`),
		Annotations: map[string]string{
			"group": "core",
		},
		Run: func(cmd *cobra.Command, args []string) {
			extractors := registry.Extractors.List()

			entityMap := make(map[string][]string)
			for name, info := range extractors {
				for _, e := range info.Entities {
					entityMap[e.Type] = append(entityMap[e.Type], name)
				}
			}

			types := make([]string, 0, len(entityMap))
			for t := range entityMap {
				types = append(types, t)
			}
			sort.Strings(types)

			if format == "json" {
				type entityJSON struct {
					Type       string   `json:"type"`
					Extractors []string `json:"extractors"`
				}
				var result []entityJSON
				for _, t := range types {
					names := entityMap[t]
					sort.Strings(names)
					result = append(result, entityJSON{Type: t, Extractors: names})
				}
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				_ = enc.Encode(result)
				return
			}

			fmt.Printf("\n%s %d entity types\n\n", printer.Greenf("Showing"), len(types))

			report := [][]string{}
			for _, t := range types {
				names := entityMap[t]
				sort.Strings(names)
				report = append(report, []string{
					printer.Cyanf("%s", t),
					strings.Join(names, ", "),
				})
			}
			printer.Table(os.Stdout, report)
		},
	}

	cmd.Flags().StringVarP(&format, "format", "f", "table", "Output format (table, json)")

	return cmd
}

// EdgesCmd creates a command to list edge types across all extractors.
func EdgesCmd() *cobra.Command {
	var format string

	cmd := &cobra.Command{
		Use:   "edges",
		Short: "List edge types across all extractors",
		Long: heredoc.Doc(`
			List all edge types produced by extractors.

			Shows each edge type, the relationship direction, and which extractors produce it.
		`),
		Example: heredoc.Doc(`
			$ meteor edges
			$ meteor edges --format json
		`),
		Annotations: map[string]string{
			"group": "core",
		},
		Run: func(cmd *cobra.Command, args []string) {
			extractors := registry.Extractors.List()

			type edgeEntry struct {
				from       string
				to         string
				extractors []string
			}

			edgeMap := make(map[string]*edgeEntry)
			for name, info := range extractors {
				for _, e := range info.Edges {
					if _, ok := edgeMap[e.Type]; !ok {
						edgeMap[e.Type] = &edgeEntry{from: e.From, to: e.To}
					}
					edgeMap[e.Type].extractors = append(edgeMap[e.Type].extractors, name)
				}
			}

			types := make([]string, 0, len(edgeMap))
			for t := range edgeMap {
				types = append(types, t)
			}
			sort.Strings(types)

			if format == "json" {
				type edgeJSON struct {
					Type       string   `json:"type"`
					From       string   `json:"from"`
					To         string   `json:"to"`
					Extractors []string `json:"extractors"`
				}
				var result []edgeJSON
				for _, t := range types {
					e := edgeMap[t]
					sort.Strings(e.extractors)
					result = append(result, edgeJSON{Type: t, From: e.from, To: e.to, Extractors: e.extractors})
				}
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				_ = enc.Encode(result)
				return
			}

			fmt.Printf("\n%s %d edge types\n\n", printer.Greenf("Showing"), len(types))

			report := [][]string{}
			for _, t := range types {
				e := edgeMap[t]
				sort.Strings(e.extractors)
				report = append(report, []string{
					printer.Yellowf("%s", t),
					printer.Greyf("%s -> %s", e.from, e.to),
					strings.Join(e.extractors, ", "),
				})
			}
			printer.Table(os.Stdout, report)
		},
	}

	cmd.Flags().StringVarP(&format, "format", "f", "table", "Output format (table, json)")

	return cmd
}
