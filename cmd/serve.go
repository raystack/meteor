package cmd

import (
	"fmt"
	"net/http"

	"github.com/odpf/meteor/api"
)

var (
	PORT = "3000"
)

func Serve() {
	router := api.NewRouter()
	api.SetupRoutes(router)

	fmt.Println("Listening on port :" + PORT)
	err := http.ListenAndServe(":"+PORT, router)
	if err != nil {
		fmt.Println(err)
	}
}
