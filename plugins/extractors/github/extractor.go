package github

import (
	"context"

	"github.com/google/go-github/v37/github"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
	"golang.org/x/oauth2"
)

type Config struct {
	Org   string `mapstructure:"org" validate:"required"`
	Token string `mapstructure:"token" validate:"required"`
}

type Extractor struct {
	logger plugins.Logger
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {

	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return extractor.InvalidConfigError{}
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
		out <- meta.User{
			Urn:      usr.GetURL(),
			Email:    usr.GetEmail(),
			Username: usr.GetLogin(),
			FullName: usr.GetName(),
			IsActive: true,
		}
	}

	return nil
}

// Register the extractor to catalog
func init() {
	if err := extractor.Catalog.Register("github", &Extractor{
		logger: plugins.Log,
	}); err != nil {
		panic(err)
	}
}
