package urlbuilder

import (
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// Source is used to create URLBuilder instances.
//
// The URLBuilder created using Source will include the base
// URL and query parameters present on the Source instance.
//
// Ideally, Source instance should be created only once for a
// given base URL.
type Source struct {
	base *url.URL
	qry  url.Values
}

// NewSource builds a Source instance by parsing the baseURL.
//
// The baseURL is expected to specify the host. If no scheme is
// specified, it defaults to http scheme.
func NewSource(baseURL string) (Source, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return Source{}, fmt.Errorf("new url builder: invalid input: %w", err)
	}

	if u.Scheme == "" {
		return NewSource("http://" + baseURL)
	}

	return Source{
		base: u,
		qry:  u.Query(),
	}, nil
}

// New creates a new instance of URLBuilder with the base URL
// and query parameters carried over from Source.
func (b Source) New() *URLBuilder {
	u := *b.base // create a copy
	return &URLBuilder{
		url:        &u,
		pathParams: make(map[string]string),
		qry:        urlValuesCopy(b.qry),
	}
}

// URLBuilder is used to build a URL.
//
// Source should be used to create an instance of URLBuilder.
//
//	b, err := httputil.NewSource("https://api.example.com/")
//	if err != nil {
//		// handle error
//	}
//
//	u := b.New().
//		Path("/users/{id}/posts").
//		PathParam("id", id).
//		QueryParam("limit", limit).
//		QueryParam("offset", offset).
//		URL()
//
//	// { id: 123, limit: 10, offset: 120 }
//	// https://api.example.com/users/123/posts?limit=10&offset=120
//
//	r, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u.String(), nil)
//	if err != nil {
//		// handle error
//	}
//
//	// send HTTP request.
type URLBuilder struct {
	url        *url.URL
	path       string
	pathParams map[string]string
	qry        url.Values
}

// Path sets the path template for the URL.
//
// Path parameters of the format "{paramName}" are supported and can be
// substituted using PathParam*.
func (u *URLBuilder) Path(p string) *URLBuilder {
	u.path = p
	return u
}

// PathParam sets the path parameter name and value which needs to be
// substituted in the path template. Substitution happens when the URL
// is built using URLBuilder.URL()
func (u *URLBuilder) PathParam(name, value string) *URLBuilder {
	u.pathParams[name] = value
	return u
}

// PathParamInt sets the path parameter name and value which needs to be
// substituted in the path template. Substitution happens when the URL
// is built using URLBuilder.URL()
func (u *URLBuilder) PathParamInt(name string, value int64) *URLBuilder {
	u.pathParams[name] = strconv.FormatInt(value, 10)
	return u
}

// PathParams sets the path parameter names and values which need to be
// substituted in the path template. Substitution happens when the URL
// is built using URLBuilder.URL()
func (u *URLBuilder) PathParams(params map[string]string) *URLBuilder {
	for name, value := range params {
		u.pathParams[name] = value
	}
	return u
}

// QueryParam sets the query parameter with the given values. If a value
// was previously set, it is replaced.
func (u *URLBuilder) QueryParam(key string, values ...string) *URLBuilder {
	u.qry.Del(key)
	for _, v := range values {
		u.qry.Add(key, v)
	}
	return u
}

// QueryParamInt sets the query parameter with the given values. If a
// value was previously set, it is replaced.
func (u *URLBuilder) QueryParamInt(key string, values ...int64) *URLBuilder {
	u.qry.Del(key)
	for _, v := range values {
		u.qry.Add(key, strconv.FormatInt(v, 10))
	}
	return u
}

// QueryParamBool sets the query parameter with the given value. If a
// value was previously set, it is replaced.
func (u *URLBuilder) QueryParamBool(key string, value bool) *URLBuilder {
	u.qry.Set(key, strconv.FormatBool(value))
	return u
}

// QueryParamFloat sets the query parameter with the given values. If a
// value was previously set, it is replaced.
func (u *URLBuilder) QueryParamFloat(key string, values ...float64) *URLBuilder {
	u.qry.Del(key)
	for _, v := range values {
		u.qry.Add(key, strconv.FormatFloat(v, 'f', -1, 64))
	}
	return u
}

// QueryParams sets the query parameters. If a value was previously set
// for any of the given parameters, it is replaced.
func (u *URLBuilder) QueryParams(params url.Values) *URLBuilder {
	for key, values := range params {
		u.qry.Del(key)
		for _, v := range values {
			u.qry.Add(key, v)
		}
	}
	return u
}

// URL constructs and returns an instance of URL.
//
// The constructed URL has the complete path and query parameters setup.
// The path parameters are substituted before being joined with the base
// URL.
func (u *URLBuilder) URL() *url.URL {
	urlv := u.url

	p := u.path
	for name, value := range u.pathParams {
		p = strings.Replace(p, "{"+name+"}", url.PathEscape(value), -1)
	}
	urlv.Path = path.Join(urlv.Path, p)

	urlv.RawQuery = u.qry.Encode()

	return urlv
}

func urlValuesCopy(src url.Values) url.Values {
	dst := make(url.Values, len(src))
	for key, values := range src {
		dst[key] = make([]string, 0, len(values))
		for _, v := range values {
			dst.Add(key, v)
		}
	}
	return dst
}
