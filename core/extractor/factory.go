package extractor

<<<<<<< HEAD
import (
	"fmt"
	"github.com/odpf/meteor/core"
	"github.com/pkg/errors"
)

var (
	Catalog = NewFactory()
)

type Factory struct {
	fnStore          map[string]core.Extractor
=======
type TableFactoryFn func() TableExtractor
type TopicFactoryFn func() TopicExtractor
type DashboardFactoryFn func() DashboardExtractor
type UserFactoryFn func() UserExtractor
type BucketFactoryFn func() BucketExtractor

type Factory struct {
	tableFnStore     map[string]TableFactoryFn
	topicFnStore     map[string]TopicFactoryFn
	dashboardFnStore map[string]DashboardFactoryFn
	userFnStore      map[string]UserFactoryFn
	bucketFnStore    map[string]BucketFactoryFn
>>>>>>> d402bd3 (feat: Google Cloud Storage metadata extractor (#144))
}

func NewFactory() *Factory {
	return &Factory{
<<<<<<< HEAD
		fnStore: map[string]core.Extractor{},
=======
		tableFnStore:     make(map[string]TableFactoryFn),
		topicFnStore:     make(map[string]TopicFactoryFn),
		dashboardFnStore: make(map[string]DashboardFactoryFn),
		userFnStore:      make(map[string]UserFactoryFn),
		bucketFnStore:    make(map[string]BucketFactoryFn),
>>>>>>> d402bd3 (feat: Google Cloud Storage metadata extractor (#144))
	}
}

func (f *Factory) Get(name string) (core.Extractor, error) {
	if extractor, ok := f.fnStore[name]; ok {
		return extractor, nil
	}
<<<<<<< HEAD
=======

	userFn, ok := f.userFnStore[name]
	if ok {
		return userFn(), nil
  }
	bucketFn, ok := f.bucketFnStore[name]
	if ok {
		return bucketFn(), nil
	}

>>>>>>> d402bd3 (feat: Google Cloud Storage metadata extractor (#144))
	return nil, NotFoundError{name}
}

func (f *Factory) Register(name string, extractor core.Extractor) (err error) {
	if _, ok := f.fnStore[name]; ok {
		return errors.Errorf("duplicate extractor: %s", name)
	}
	f.fnStore[name] = extractor
	return nil
}

type NotFoundError struct {
	Name string
}

<<<<<<< HEAD
func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find extractor \"%s\"", err.Name)
}
=======
func (f *Factory) SetDashboardExtractor(name string, fn DashboardFactoryFn) {
	f.dashboardFnStore[name] = fn
}

func (f *Factory) SetUserExtractor(name string, fn UserFactoryFn) {
	f.userFnStore[name] = fn
}
func (f *Factory) SetBucketExtractor(name string, fn BucketFactoryFn) {
	f.bucketFnStore[name] = fn
}
>>>>>>> d402bd3 (feat: Google Cloud Storage metadata extractor (#144))
