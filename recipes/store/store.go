package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/odpf/meteor/domain"
	"github.com/odpf/meteor/recipes"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	"gocloud.dev/gcerrors"
)

type store struct {
	bucket *blob.Bucket
}

func New(storageURL string) (*store, error) {
	bucket, err := blob.OpenBucket(context.Background(), storageURL)
	if err != nil {
		return nil, err
	}

	return &store{
		bucket: bucket,
	}, nil
}

func (s *store) GetByName(name string) (recipe domain.Recipe, err error) {
	fileName := s.buildFileName(name)
	r, err := s.bucket.NewReader(context.Background(), fileName, nil)
	if err != nil {
		if s.isBlobNotFoundError(err) {
			return recipe, recipes.NotFoundError{RecipeName: name}
		}
		return recipe, err
	}
	defer r.Close()

	err = json.NewDecoder(r).Decode(&recipe)
	if err != nil {
		return recipe, err
	}

	return
}

func (s *store) Create(recipe domain.Recipe) (err error) {
	fileName := s.buildFileName(recipe.Name)
	w, err := s.bucket.NewWriter(context.Background(), fileName, nil)
	if err != nil {
		return err
	}

	err = json.NewEncoder(w).Encode(recipe)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}

	return
}

func (s *store) buildFileName(recipeName string) string {
	return fmt.Sprintf("%s.json", recipeName)
}

func (s *store) isBlobNotFoundError(err error) bool {
	return strings.Contains(err.Error(), gcerrors.NotFound.String())
}
