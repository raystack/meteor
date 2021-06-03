package recipes

import "github.com/odpf/meteor/domain"

type Store interface {
	GetByName(string) (domain.Recipe, error)
	Create(domain.Recipe) error
}

type MemoryStore struct {
	list    []domain.Recipe
	index   map[string]int
	nextRow int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		list:    []domain.Recipe{},
		index:   map[string]int{},
		nextRow: 0,
	}
}

func (store *MemoryStore) GetByName(name string) (recipe domain.Recipe, err error) {
	row, ok := store.index[name]
	if !ok || row > len(store.list) {
		return recipe, NotFoundError{RecipeName: name}
	}

	recipe = store.list[row]

	return recipe, err
}

func (store *MemoryStore) Create(recipe domain.Recipe) error {
	existing, err := store.GetByName(recipe.Name)
	if err != nil {
		if _, ok := err.(NotFoundError); !ok {
			return err
		}
	}
	if existing.Name == recipe.Name {
		return ErrDuplicateRecipeName
	}

	store.list = append(store.list, recipe)
	store.index[recipe.Name] = store.nextRow
	store.nextRow++

	return nil
}
