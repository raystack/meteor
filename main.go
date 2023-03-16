package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/goto/meteor/cmd"

	_ "github.com/goto/meteor/plugins/extractors"
	_ "github.com/goto/meteor/plugins/processors"
	_ "github.com/goto/meteor/plugins/sinks"
	"github.com/goto/salt/cmdx"
)

const (
	exitOK    = 0
	exitError = 1
)

func main() {
	// Execute the root command
	root := cmd.New()

	cmd, err := root.ExecuteC()

	if err == nil {
		return
	}

	if cmdx.IsCmdErr(err) {
		if !strings.HasSuffix(err.Error(), "\n") {
			fmt.Println()
		}
		fmt.Println(cmd.UsageString())
		os.Exit(exitOK)
	}

	fmt.Println(err)
	os.Exit(exitError)
}
