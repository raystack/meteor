package gsuite

import (
	"context"

	"golang.org/x/oauth2/google"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type UsersServiceFactory interface {
	BuildUserService(ctx context.Context, email, serviceAccountJSON string) (UsersListCall, error)
}

type UsersListCall interface {
	Do(opts ...googleapi.CallOption) (*admin.Users, error)
}

type DefaultUsersServiceFactory struct{}

func (f *DefaultUsersServiceFactory) BuildUserService(ctx context.Context, email, serviceAccountJSON string) (UsersListCall, error) {
	jwtConfig, err := google.JWTConfigFromJSON([]byte(serviceAccountJSON), admin.AdminDirectoryUserScope)
	if err != nil {
		return nil, err
	}
	jwtConfig.Subject = email

	ts := jwtConfig.TokenSource(ctx)

	srv, err := admin.NewService(ctx, option.WithTokenSource(ts))
	if err != nil {
		return nil, err
	}

	return srv.Users.List().Customer("my_customer"), nil
}
