package github

import (
	"context"

	"github.com/google/go-github/v37/github"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"golang.org/x/oauth2"
)

var (
	configInfo = ``
	inputInfo  = `
Input:
______________________________________________________________________
| Key    | Example         | Description                 |            |
|________|_________________|_________ ___________________|____________|
|"org"   | "odpf"          | Name of github organisation | *required* |
|"token" | "kdfljdfljoijj" | Github API access token     | *required* |
|__________________________|_____________________________|____________|
`
	outputInfo = `
Output:
______________________________________________
|Field         |Sample Value                  |
|_______ ______|______________________________|
|"resource.urn"|"https://github.com/ravisuhag"|
|"email"       | "suhag.ravi@gmail.com"       |
|"username"    | "ravisuhag"                  |
|"full_name"   | "Ravi Suhag"                 |
|"status"      | "active"                     |
|______________|______________________________|`
)

type Config struct {
	Org   string `mapstructure:"org" validate:"required"`
	Token string `mapstructure:"token" validate:"required"`
}

type Extractor struct {
	logger log.Logger
}

func (e *Extractor) GetDescription() string {
	return inputInfo + outputInfo
}

func (e *Extractor) GetSampleConfig() string {
	return configInfo
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {

	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: config.Token},
	)
	tc := oauth2.NewClient(ctx, ts)

	client := github.NewClient(tc)

	users, _, err := client.Organizations.ListMembers(context.Background(), config.Org, nil)

	if err != nil {
		return err
	}
	for _, user := range users {
		usr, _, err := client.Users.Get(ctx, *user.Login)
		if err != nil {
			continue
		}
		out <- assets.User{
			Resource: &common.Resource{
				Urn: usr.GetURL(),
			},
			Email:    usr.GetEmail(),
			Username: usr.GetLogin(),
			FullName: usr.GetName(),
			Status:   "active",
		}
	}

	return nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("github", func() plugins.Extractor {
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
