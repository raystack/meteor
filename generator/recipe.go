package generator

import "fmt"

func Recipe(name string) {
	fmt.Println(fmt.Sprintf(sample, name))
}

var sample = `
name: %s
source:
  type: date
sinks:
  - name: console
`
