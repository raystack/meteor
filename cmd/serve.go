package cmd

import (
	"fmt"
	"net/http"
)

var (
	PORT = "3000"
)

func Serve() {
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/ping", handlePing)

	fmt.Println("Listening on port :" + PORT)
	err := http.ListenAndServe(":"+PORT, nil)
	if err != nil {
		fmt.Println(err)
	}
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello"))
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong"))
}
