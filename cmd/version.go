package cmd

import (
	"fmt"

	"github.com/odpf/salt/term"
	"github.com/odpf/salt/version"
	"github.com/spf13/cobra"
)

var (
	Version     string
	BuildCommit string
	BuildDate   string
)

// VersionCmd prints the version of the binary
func VersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "version",
		Aliases: []string{"v"},
		Short:   "Print meteor version information",
		RunE: func(cmd *cobra.Command, args []string) error {
			cs := term.NewColorScheme()

			if Version == "" {
				fmt.Println("Version information not available.")
				return nil
			}

			fmt.Println(Version)
			fmt.Println(cs.Yellow(version.UpdateNotice(Version, "odpf/meteor")))

			return nil
		},
	}
}
