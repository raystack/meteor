package main

import (
	"github.com/odpf/meteor/cmd"
	_ "github.com/odpf/meteor/plugins/extractors"
	_ "github.com/odpf/meteor/plugins/processors"
	_ "github.com/odpf/meteor/plugins/sinks"
)

func main() {
	cmd.Execute()
}
