package recipe

import (
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

var recipeEnvVarPrefix = "METEOR_"

func populateData(pathToConfig string) map[string]string {
	viper.AddConfigPath(".")
	data := make(map[string]string)

	if pathToConfig != "" {
		data = loadDataFromYaml(pathToConfig)
	}

	// fetches local environment variable with prefix METEOR_
	envData := populateDataFromLocal()

	for key, val := range envData {
		_, keyAlreadyExists := data[key]
		// prefer variables from .yaml file in cases of replication
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
		val := keyval[1]

		key, ok := mapToMeteorKey(key)
		if !ok {
			continue
		}

		data[key] = val
	}

	return data
}

func mapToMeteorKey(rawKey string) (string, bool) {
	// we are doing everything in lowercase for case insensitivity
	key := strings.ToLower(rawKey)
	meteorPrefix := strings.ToLower(recipeEnvVarPrefix)
	if !strings.HasPrefix(key, meteorPrefix) {
		return "", false
	}

	// strips prefix - meteor_user_id becomes user_id
	return key[len(meteorPrefix):], true
}

func loadDataFromYaml(path string) map[string]string {
	data := make(map[string]string)
	splitPath := strings.Split(path, "/")
	fileName := strings.Split(splitPath[len(splitPath)-1], ".")
	st := 0
	if splitPath[0] == "." {
		st++
	}

	// set config details for Viper
	viper.SetConfigName(fileName[0])
	viper.SetConfigType(fileName[1])
	viper.AddConfigPath(strings.Join(splitPath[st:len(splitPath)-1], ""))
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(err)
	}

	keys := viper.AllKeys()
	for i := range keys {
		varConvention := strings.Join(strings.Split(keys[i], "."), "_")
		data[varConvention] = viper.GetString(keys[i])
	}

	return data
}
