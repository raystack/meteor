package stores

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/odpf/meteor/secrets"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	"gocloud.dev/gcerrors"
)

type secretStore struct {
	bucket *blob.Bucket
}

func NewSecretStore(storageURL string) (*secretStore, error) {
	bucket, err := blob.OpenBucket(context.Background(), storageURL)
	if err != nil {
		return nil, err
	}

	return &secretStore{
		bucket: bucket,
	}, nil
}

func (s *secretStore) Find(name string) (secret secrets.Secret, err error) {
	fileName := s.buildFileName(name)
	r, err := s.bucket.NewReader(context.Background(), fileName, nil)
	if err != nil {
		if s.isBlobNotFoundError(err) {
			return secret, secrets.NotFoundError{SecretName: name}
		}
		return secret, err
	}
	defer r.Close()

	err = json.NewDecoder(r).Decode(&secret)
	if err != nil {
		return secret, err
	}

	return
}

func (s *secretStore) Upsert(secret secrets.Secret) (err error) {
	fileName := s.buildFileName(secret.Name)
	w, err := s.bucket.NewWriter(context.Background(), fileName, nil)
	if err != nil {
		return err
	}

	err = json.NewEncoder(w).Encode(secret)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}

	return
}

func (s *secretStore) buildFileName(secretName string) string {
	return fmt.Sprintf("%s.json", secretName)
}

func (s *secretStore) isBlobNotFoundError(err error) bool {
	return strings.Contains(err.Error(), gcerrors.NotFound.String())
}
