package recipes

import "github.com/odpf/meteor/domain"

type Store interface {
	GetByName(string) (domain.Recipe, error)
	Create(domain.Recipe) error
}
