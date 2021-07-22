package github

import (
	"context"

	"github.com/google/go-github/v37/github"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
	"golang.org/x/oauth2"
)

type Config struct {
	Org   string `mapstructure:"org" validate:"required"`
	Token string `mapstructure:"token" validate:"required"`
}

type Extractor struct{}

func New() extractor.UserExtractor {
	return &Extractor{}
}

func (e *Extractor) Extract(c map[string]interface{}) (result []meta.User, err error) {

	var config Config
	err = utils.BuildConfig(c, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}

	ctx := context.Background()
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: config.Token},
	)
	tc := oauth2.NewClient(ctx, ts)

	client := github.NewClient(tc)

	users, _, err := client.Organizations.ListMembers(context.Background(), config.Org, nil)

	for _, user := range users {
		usr, _, err := client.Users.Get(context.Background(), *user.Login)
		if err != nil {
			continue
		}
		result = append(result, meta.User{
			Urn:      usr.GetURL(),
			Email:    usr.GetEmail(),
			Username: usr.GetLogin(),
			FullName: usr.GetName(),
			IsActive: true,
		})
	}

	return result, err
}
