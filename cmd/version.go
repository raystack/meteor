package cmd

import (
	"fmt"

	"github.com/goto/salt/term"
	"github.com/goto/salt/version"
	"github.com/spf13/cobra"
)

var (
	// Version is the version of the binary
	Version string
	// BuildCommit is the commit hash of the binary
	BuildCommit string
	// BuildDate is the date of the build
	BuildDate string
)

// VersionCmd prints the version of the binary
func VersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "version",
		Aliases: []string{"v"},
		Short:   "Print meteor version information",
		RunE: func(cmd *cobra.Command, args []string) error {
			if Version == "" {
				fmt.Println("Version information not available.")
				return nil
			}

			fmt.Println(Version)
			fmt.Println(term.Yellow(version.UpdateNotice(Version, "gotocompany/meteor")))

			return nil
		},
	}
}
