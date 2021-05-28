package recipes

type Store interface {
	GetByName(string) (Recipe, error)
	Create(Recipe) error
}

type MemoryStore struct {
	list    []Recipe
	index   map[string]int
	nextRow int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		list:    []Recipe{},
		index:   map[string]int{},
		nextRow: 0,
	}
}

func (store *MemoryStore) GetByName(name string) (recipe Recipe, err error) {
	row, ok := store.index[name]
	if !ok || row > len(store.list) {
		return recipe, NotFoundError{name}
	}

	recipe = store.list[row]

	return recipe, err
}

func (store *MemoryStore) Create(recipe Recipe) error {
	store.list = append(store.list, recipe)
	store.index[recipe.Name] = store.nextRow
	store.nextRow++

	return nil
}
