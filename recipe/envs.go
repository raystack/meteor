package recipe

import (
	"log"
	"strings"

	"github.com/joho/godotenv"
)

func populateData() map[string]string {
	data, err := godotenv.Read()
	if err != nil {
		log.Fatal(err)
	}

	for key, val := range data {
		k := strings.ToLower(key)
		data[k] = val
	}

	return data
}
