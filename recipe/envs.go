package recipe

import (
	"os"
	"strings"
)

var (
	recipeEnvVarPrefix = "METEOR_"
)

func populateData() map[string]string {
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
