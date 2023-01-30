package redash

import "time"

type Results struct {
	Tags                    []interface{} `json:"tags"`
	IsArchived              bool          `json:"is_archived"`
	UpdatedAt               time.Time     `json:"updated_at"`
	IsFavorite              bool          `json:"is_favorite"`
	User                    User          `json:"user"`
	Layout                  []interface{} `json:"layout"`
	IsDraft                 bool          `json:"is_draft"`
	Id                      int           `json:"id"`
	UserId                  int           `json:"user_id"`
	Name                    string        `json:"name"`
	CreatedAt               time.Time     `json:"created_at"`
	Slug                    string        `json:"slug"`
	Version                 int           `json:"version"`
	Widgets                 interface{}   `json:"widgets"`
	DashboardFiltersEnabled bool          `json:"dashboard_filters_enabled"`
}

type User struct {
	AuthType            string      `json:"auth_type"`
	IsDisabled          bool        `json:"is_disabled"`
	UpdatedAt           time.Time   `json:"updated_at"`
	ProfileImageUrl     string      `json:"profile_image_url"`
	IsInvitationPending bool        `json:"is_invitation_pending"`
	Groups              []int       `json:"groups"`
	Id                  int         `json:"id"`
	Name                string      `json:"name"`
	CreatedAt           time.Time   `json:"created_at"`
	DisabledAt          interface{} `json:"disabled_at"`
	IsEmailVerified     bool        `json:"is_email_verified"`
	ActiveAt            time.Time   `json:"active_at"`
	Email               string      `json:"email"`
}
