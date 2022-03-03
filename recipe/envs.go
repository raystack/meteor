package recipe

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

var (
	recipeEnvVarPrefix = "METEOR_"
)

func populateData(pathToConfig string) map[string]string {
	viper.AddConfigPath(".")
	data := make(map[string]string)

	if pathToConfig != "" {
		splitPath := strings.Split(pathToConfig, "/")
		fileName := strings.Split(splitPath[len(splitPath)-1], ".")
		// fmt.Println("Config Path: ", strings.Join(splitPath[0:len(splitPath)-1], ""))
		// fmt.Println("File Name: ", fileName)
		viper.SetConfigName(fileName[0])
		viper.SetConfigType(fileName[1])
		st := 0
		if splitPath[0] == "." {
			st++
		}
		viper.AddConfigPath(strings.Join(splitPath[st:len(splitPath)-1], ""))
		if err := viper.ReadInConfig(); err != nil {
			fmt.Printf("Error reading config file, %s", err)
		}
		keys := viper.AllKeys()
		for i := range keys {
			ch := strings.Join(strings.Split(keys[i], "."), "_")
			fmt.Println(ch, ": ", viper.Get(keys[i]))
			data[ch] = viper.Get(keys[i]).(string)
		}
	}

	// fetches local environment variable with prefix METEOR_
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
