package secrets

import (
	"fmt"
	"strings"
)

func MapConfig(c map[string]interface{}, store Store) (err error) {
	for key, val := range c {
		secretRef, ok := isReferencingSecret(val)
		if !ok {
			continue
		}

		secretVal, err := mapSecretToValue(secretRef, store)
		if err != nil {
			return err
		}
		c[key] = secretVal
	}

	return
}

func isReferencingSecret(val interface{}) (secretRef string, ok bool) {
	secretRef, ok = val.(string)
	if !ok {
		return secretRef, ok
	}

	return secretRef, strings.Contains(secretRef, "$secret.")
}

func mapSecretToValue(secretRef string, store Store) (res interface{}, err error) {
	secretName, secretKey := getSecretRefComponent(secretRef)
	secret, err := store.Find(secretName)
	if err != nil {
		return
	}
	res, ok := secret.Data[secretKey]
	if !ok {
		return res, fmt.Errorf("could not find \"%s\" key in \"%s\" secret", secretKey, secretName)
	}

	return
}

func getSecretRefComponent(secretRef string) (secretName, secretKey string) {
	comps := strings.Split(secretRef, ".")

	secretName = comps[1]
	secretKey = comps[2]

	return
}
