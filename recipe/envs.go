package recipe

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

var (
	recipeEnvVarPrefix = "METEOR_"
)

func populateData() map[string]string {
	dataUpperCase, err := godotenv.Read()

	// warns user about missing .env file
	if err != nil && err.Error() == "open .env: no such file or directory" {
		log.Warnln(".env file not found")
	} else if err != nil {
		log.Error(err)
	}

	data := make(map[string]string)
	// convert variable names to lower case
	for key, val := range dataUpperCase {
		k := strings.ToLower(key)
		data[k] = val
	}

	// fecthes local environment variable with prefix METEOR_
	envData := populateDataFromLocal()

	for key, val := range envData {
		_, keyAlreadyExists := data[key]
		// prefer variables from .env file in cases of replication
		if keyAlreadyExists {
			continue
		}
		data[key] = val
	}
	return data
}

func populateDataFromLocal() map[string]string {
	data := make(map[string]string)
	for _, envvar := range os.Environ() {
		keyval := strings.SplitN(envvar, "=", 2) // "sampleKey=sample=Value" returns ["sampleKey", "sample=value"]
		key := keyval[0]
		val := os.ExpandEnv(keyval[1])

		key, ok := mapToMeteorKey(key)
		if !ok {
			continue
		}

		data[key] = val
	}

	return data
}

func mapToMeteorKey(rawKey string) (key string, ok bool) {
	// we are doing everything in lowercase for case insensitivity
	key = strings.ToLower(rawKey)
	meteorPrefix := strings.ToLower(recipeEnvVarPrefix)
	keyPrefixLen := len(meteorPrefix)

	isMeteorKeyFormat := len(key) > keyPrefixLen && key[:keyPrefixLen] == meteorPrefix
	if !isMeteorKeyFormat {
		return
	}
	key = key[keyPrefixLen:] // strips prefix - meteor_user_id becomes user_id
	ok = true

	return
}
