//go:build plugins
// +build plugins

package gsuite_test

import (
	"context"
	"testing"

	"github.com/goto/meteor/models"
	assetsv1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/gsuite"
	"github.com/goto/meteor/test/mocks"
	"github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
)

var urnScope string = "test-gsuite"

func TestInit(t *testing.T) {
	t.Run("should return error for empty user email", func(t *testing.T) {
		err := gsuite.New(utils.Logger, new(mockUsersServiceFactory)).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"user_email":           "",
				"service_account_json": "{\"type\":\"service_account\",\"project_id\":\"gotocompany-meteor\",\"private_key_id\":\"3cb2103ef7883845a5fdcsvdefe6ff83d616757\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggdvdvdEAAoIBAQDF/cDQ++JnH9+9\\n3YBm4APqPbvfj6eHSdAUSjzKdbdfbdbYGgdxC7xPS1PVo+ENw+pBAH3NoRwQWYEin\\nHYj064sMvm8vbR5TcMQpnxYG86TGaPuIh30grz5dI39dtrUjttbdfbdvqRv0qu7I5\\nuELzp2OLUz509Q3AvuqvQVCZc7sDjNr2TPOsLeuCkpmcmBHbdfdi29bhoS+Ac\\n5ipT10yGF0FvT1f5KlJcHfsNoOGPJYePTaGxOW1zk680Z1Wdfbdf1xX9iw5/GUA3XM\\neon4p9X31ASgwbdbdplFZhwvcpoaYpxcuxyvefR44emnfveUY91h6wLvF/mPBElO\\npXOiVJ3lAgMBAAECggEbdbddYz8nSmTWFMW2OtyvojIq+ab864ZGPCpW4zfzF4BI\\n7o5TSIsNOMQMrawFUz0xZkgofJThfOscyXbbdbdbfbfT3wXI9JTWT8l275ssvFQVy1\\nVyAJI/Kize9ru5GnnEzV2sZoYEmOsB2xgqjvKXR90r5wNJ6wFp8Ubp9/+v2lTv1n\\nUCBBYPsPyVmUq677HfMVVa6ZpxCTWvbQga+/ZPaqppgGps5yLDqc434c3A/lDCKBtqk\\njaQXHqKjuYUsoiyl2vbPbwGxc34343c6gQfe7aeCouf8bI4GzCPmoyVPMRFpQJ6Ahp\\nMnCE96KfVVUARh1goxEEwMmSFyBPYFbmvXLPUGNfcQKBgQD3nrDHeWxW+0MjnaYD\\novXKvpnv1NiBCywOAEfc343535dJfgMZX0cfpnTDGXKPBI5ZbUywxk0sewu382JoArM\\n1w2wEIqH+73FGiMVpAuN2DpNX5mOC+z/zjFdOFZ28jkRUy8T+PTkajj7rkB7VDOr\\nIiCZwRrnbQFwhErWS1fZgg2PcQKBgQDMsRgDBfhgJX9sNRX3FHzIEZU94PP1KOc2\\nEUUzcwIV0cNOVzSyOUn2qrcYNg/hZZpGeRBBwyOcDGsqxmz5FAzk0OtbSCaMxybF\\n8NXFDh3ELmnfIyVBjvNBWPckcR1LCZcKGTqVLH/rhPiNhyzH3NQ0c3Gl15GPgzkD\\nboLfFN3jtQKBgG++blpmYkzScNb2wr9rX+5Rm1hOvjFl4EilOb+1rq/WPZ0ig5ZD\\nT5mdQ6ZC+5ppWp8AyjQsgsAYgUG1NoqAFg45OLrrERWMmP6gHBKz3IOkO8CNgzNh\\nUoeV7/cXkkdOObWSqLkXcoWpejHtqq905C9epIyBdZ/YI4mXU3c4343c4QRAoGBAK9F\\nMO9dzFjfouVP63f/Nf3GeIlctuiE1r5IOX4di3qNe/P33iqBvaCWe2Mi36Q78MdJ\\nYK8+3Z4AUD93WtZI4eWIMw+dj0zaNowldZZfSQO0Tnl/yaYCNq8M88pjhRa8pnVC\\nNxSG3x4XZREi3yhgIeCrvXOpS32celRC65MDdiBFAoGAHbURTEkQDZaWPAmVv+0q\\nYaT7x+UzQDGKy/By9QLGM/U2gvLGTw1vzmoeh99BTsQopPB/QuAfJNIHk9h0ohXJ\\nfA/X4T3F2LGhZ9+bujVyCQc0tTxuh41t2ipJPWtDP52rXk1AkCnIeWD+UHI0u5Ba\\nhI1dzLIxZKeq3bESrc/9tmM=\\n-----END PRIVATE KEY-----\\n\",\"client_email\":\"meteor-sa@gotocompany-meteor.iam.gserviceaccount.com\",\"client_id\":\"110059943435984635286\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\":\"https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40gotocompany-meteor.iam.gserviceaccount.com\"}",
			},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return error for invalid service account json", func(t *testing.T) {
		err := gsuite.New(utils.Logger, new(mockUsersServiceFactory)).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"user_email":           "user@example.com",
				"service_account_json": "",
			},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return error for invalid service account json", func(t *testing.T) {
		ctx := context.TODO()
		userEmail := "user@example.com"
		serviceAcc := "{\"type\":\"service_account\",\"project_id\":\"gotocompany-meteor\",\"private_key_id\":\"3cb2103ef7883845a5fdcsvdefe6ff83d616757\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggdvdvdEAAoIBAQDF/cDQ++JnH9+9\\n3YBm4APqPbvfj6eHSdAUSjzKdbdfbdbYGgdxC7xPS1PVo+ENw+pBAH3NoRwQWYEin\\nHYj064sMvm8vbR5TcMQpnxYG86TGaPuIh30grz5dI39dtrUjttbdfbdvqRv0qu7I5\\nuELzp2OLUz509Q3AvuqvQVCZc7sDjNr2TPOsLeuCkpmcmBHbdfdi29bhoS+Ac\\n5ipT10yGF0FvT1f5KlJcHfsNoOGPJYePTaGxOW1zk680Z1Wdfbdf1xX9iw5/GUA3XM\\neon4p9X31ASgwbdbdplFZhwvcpoaYpxcuxyvefR44emnfveUY91h6wLvF/mPBElO\\npXOiVJ3lAgMBAAECggEbdbddYz8nSmTWFMW2OtyvojIq+ab864ZGPCpW4zfzF4BI\\n7o5TSIsNOMQMrawFUz0xZkgofJThfOscyXbbdbdbfbfT3wXI9JTWT8l275ssvFQVy1\\nVyAJI/Kize9ru5GnnEzV2sZoYEmOsB2xgqjvKXR90r5wNJ6wFp8Ubp9/+v2lTv1n\\nUCBBYPsPyVmUq677HfMVVa6ZpxCTWvbQga+/ZPaqppgGps5yLDqc434c3A/lDCKBtqk\\njaQXHqKjuYUsoiyl2vbPbwGxc34343c6gQfe7aeCouf8bI4GzCPmoyVPMRFpQJ6Ahp\\nMnCE96KfVVUARh1goxEEwMmSFyBPYFbmvXLPUGNfcQKBgQD3nrDHeWxW+0MjnaYD\\novXKvpnv1NiBCywOAEfc343535dJfgMZX0cfpnTDGXKPBI5ZbUywxk0sewu382JoArM\\n1w2wEIqH+73FGiMVpAuN2DpNX5mOC+z/zjFdOFZ28jkRUy8T+PTkajj7rkB7VDOr\\nIiCZwRrnbQFwhErWS1fZgg2PcQKBgQDMsRgDBfhgJX9sNRX3FHzIEZU94PP1KOc2\\nEUUzcwIV0cNOVzSyOUn2qrcYNg/hZZpGeRBBwyOcDGsqxmz5FAzk0OtbSCaMxybF\\n8NXFDh3ELmnfIyVBjvNBWPckcR1LCZcKGTqVLH/rhPiNhyzH3NQ0c3Gl15GPgzkD\\nboLfFN3jtQKBgG++blpmYkzScNb2wr9rX+5Rm1hOvjFl4EilOb+1rq/WPZ0ig5ZD\\nT5mdQ6ZC+5ppWp8AyjQsgsAYgUG1NoqAFg45OLrrERWMmP6gHBKz3IOkO8CNgzNh\\nUoeV7/cXkkdOObWSqLkXcoWpejHtqq905C9epIyBdZ/YI4mXU3c4343c4QRAoGBAK9F\\nMO9dzFjfouVP63f/Nf3GeIlctuiE1r5IOX4di3qNe/P33iqBvaCWe2Mi36Q78MdJ\\nYK8+3Z4AUD93WtZI4eWIMw+dj0zaNowldZZfSQO0Tnl/yaYCNq8M88pjhRa8pnVC\\nNxSG3x4XZREi3yhgIeCrvXOpS32celRC65MDdiBFAoGAHbURTEkQDZaWPAmVv+0q\\nYaT7x+UzQDGKy/By9QLGM/U2gvLGTw1vzmoeh99BTsQopPB/QuAfJNIHk9h0ohXJ\\nfA/X4T3F2LGhZ9+bujVyCQc0tTxuh41t2ipJPWtDP52rXk1AkCnIeWD+UHI0u5Ba\\nhI1dzLIxZKeq3bESrc/9tmM=\\n-----END PRIVATE KEY-----\\n\",\"client_email\":\"meteor-sa@gotocompany-meteor.iam.gserviceaccount.com\",\"client_id\":\"110059943435984635286\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\":\"https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40gotocompany-meteor.iam.gserviceaccount.com\"}"
		factory := new(mockUsersServiceFactory)
		factory.On("BuildUserService", ctx, userEmail, serviceAcc).Return(new(mockUsersListCall), nil)

		err := gsuite.New(utils.Logger, factory).Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"user_email":           userEmail,
				"service_account_json": serviceAcc,
			},
		})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract user details from google workspace", func(t *testing.T) {
		adminUsers := []*admin.User{
			{
				Name:         &admin.UserName{FullName: "User1"},
				PrimaryEmail: "user1@test.com",
				Suspended:    true,
				Aliases:      []string{"alias1", "alias2"},
				Relations: []interface{}{
					map[string]interface{}{
						"type":  "manager1",
						"value": "manager1@test.com",
					},
				},
				Organizations: []interface{}{
					map[string]interface{}{
						"foo0": "bar0",
						"foo1": "bar1",
					},
				},
				OrgUnitPath: "/",
				CustomSchemas: map[string]googleapi.RawMessage{
					"foo0_customSchema": []byte("bar0_customSchema"),
					"foo1_customSchema": []byte("bar1_customSchema"),
				},
			},
			{
				Name:         &admin.UserName{FullName: "User2"},
				PrimaryEmail: "user2@test.com",
				Suspended:    false,
				Aliases:      []string{"alias3"},
				Relations: []interface{}{
					map[string]interface{}{
						"type":  "manager2",
						"value": "manager2@test.com",
					},
				},
				Organizations: []interface{}{
					map[string]interface{}{
						"foo20": "bar20",
						"foo21": "bar21",
					},
				},
				OrgUnitPath: "/test2",
				CustomSchemas: map[string]googleapi.RawMessage{
					"foo20_customSchema": []byte("bar20_customSchema"),
					"foo21_customSchema": []byte("bar21_customSchema"),
				},
			},
		}

		expectedData := []*assetsv1beta2.Asset{
			{
				Urn:     models.NewURN("gsuite", urnScope, "user", adminUsers[0].PrimaryEmail),
				Name:    adminUsers[0].Name.FullName,
				Service: "gsuite",
				Type:    "user",
				Data: utils.BuildAny(t, &assetsv1beta2.User{
					Email:    adminUsers[0].PrimaryEmail,
					FullName: adminUsers[0].Name.FullName,
					Status:   "suspended",
					Attributes: utils.BuildStruct(t, map[string]interface{}{
						"aliases":       "alias1,alias2",
						"org_unit_path": "/",
						"organizations": []interface{}{
							map[string]interface{}{
								"foo0": "bar0",
								"foo1": "bar1",
							},
						},
						"custom_schemas": map[string]interface{}{
							"foo0_customSchema": "bar0_customSchema",
							"foo1_customSchema": "bar1_customSchema",
						},
						"relations": []interface{}{
							map[string]interface{}{
								"type":  "manager1",
								"value": "manager1@test.com",
							},
						},
					}),
				}),
			},
			{
				Urn:     models.NewURN("gsuite", urnScope, "user", adminUsers[1].PrimaryEmail),
				Name:    adminUsers[1].Name.FullName,
				Service: "gsuite",
				Type:    "user",
				Data: utils.BuildAny(t, &assetsv1beta2.User{
					Email:    adminUsers[1].PrimaryEmail,
					FullName: adminUsers[1].Name.FullName,
					Status:   "",
					Attributes: utils.BuildStruct(t, map[string]interface{}{
						"aliases":       "alias3",
						"org_unit_path": "/test2",
						"organizations": []interface{}{
							map[string]interface{}{
								"foo20": "bar20",
								"foo21": "bar21",
							},
						},
						"custom_schemas": map[string]interface{}{
							"foo20_customSchema": "bar20_customSchema",
							"foo21_customSchema": "bar21_customSchema",
						},
						"relations": []interface{}{
							map[string]interface{}{
								"type":  "manager2",
								"value": "manager2@test.com",
							},
						},
					}),
				}),
			},
		}

		ctx := context.TODO()
		userEmail := "user@example.com"
		serviceAcc := "{\"type\":\"service_account\",\"project_id\":\"gotocompany-meteor\",\"private_key_id\":\"3cb2103ef7883845a5fdcsvdefe6ff83d616757\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggdvdvdEAAoIBAQDF/cDQ++JnH9+9\\n3YBm4APqPbvfj6eHSdAUSjzKdbdfbdbYGgdxC7xPS1PVo+ENw+pBAH3NoRwQWYEin\\nHYj064sMvm8vbR5TcMQpnxYG86TGaPuIh30grz5dI39dtrUjttbdfbdvqRv0qu7I5\\nuELzp2OLUz509Q3AvuqvQVCZc7sDjNr2TPOsLeuCkpmcmBHbdfdi29bhoS+Ac\\n5ipT10yGF0FvT1f5KlJcHfsNoOGPJYePTaGxOW1zk680Z1Wdfbdf1xX9iw5/GUA3XM\\neon4p9X31ASgwbdbdplFZhwvcpoaYpxcuxyvefR44emnfveUY91h6wLvF/mPBElO\\npXOiVJ3lAgMBAAECggEbdbddYz8nSmTWFMW2OtyvojIq+ab864ZGPCpW4zfzF4BI\\n7o5TSIsNOMQMrawFUz0xZkgofJThfOscyXbbdbdbfbfT3wXI9JTWT8l275ssvFQVy1\\nVyAJI/Kize9ru5GnnEzV2sZoYEmOsB2xgqjvKXR90r5wNJ6wFp8Ubp9/+v2lTv1n\\nUCBBYPsPyVmUq677HfMVVa6ZpxCTWvbQga+/ZPaqppgGps5yLDqc434c3A/lDCKBtqk\\njaQXHqKjuYUsoiyl2vbPbwGxc34343c6gQfe7aeCouf8bI4GzCPmoyVPMRFpQJ6Ahp\\nMnCE96KfVVUARh1goxEEwMmSFyBPYFbmvXLPUGNfcQKBgQD3nrDHeWxW+0MjnaYD\\novXKvpnv1NiBCywOAEfc343535dJfgMZX0cfpnTDGXKPBI5ZbUywxk0sewu382JoArM\\n1w2wEIqH+73FGiMVpAuN2DpNX5mOC+z/zjFdOFZ28jkRUy8T+PTkajj7rkB7VDOr\\nIiCZwRrnbQFwhErWS1fZgg2PcQKBgQDMsRgDBfhgJX9sNRX3FHzIEZU94PP1KOc2\\nEUUzcwIV0cNOVzSyOUn2qrcYNg/hZZpGeRBBwyOcDGsqxmz5FAzk0OtbSCaMxybF\\n8NXFDh3ELmnfIyVBjvNBWPckcR1LCZcKGTqVLH/rhPiNhyzH3NQ0c3Gl15GPgzkD\\nboLfFN3jtQKBgG++blpmYkzScNb2wr9rX+5Rm1hOvjFl4EilOb+1rq/WPZ0ig5ZD\\nT5mdQ6ZC+5ppWp8AyjQsgsAYgUG1NoqAFg45OLrrERWMmP6gHBKz3IOkO8CNgzNh\\nUoeV7/cXkkdOObWSqLkXcoWpejHtqq905C9epIyBdZ/YI4mXU3c4343c4QRAoGBAK9F\\nMO9dzFjfouVP63f/Nf3GeIlctuiE1r5IOX4di3qNe/P33iqBvaCWe2Mi36Q78MdJ\\nYK8+3Z4AUD93WtZI4eWIMw+dj0zaNowldZZfSQO0Tnl/yaYCNq8M88pjhRa8pnVC\\nNxSG3x4XZREi3yhgIeCrvXOpS32celRC65MDdiBFAoGAHbURTEkQDZaWPAmVv+0q\\nYaT7x+UzQDGKy/By9QLGM/U2gvLGTw1vzmoeh99BTsQopPB/QuAfJNIHk9h0ohXJ\\nfA/X4T3F2LGhZ9+bujVyCQc0tTxuh41t2ipJPWtDP52rXk1AkCnIeWD+UHI0u5Ba\\nhI1dzLIxZKeq3bESrc/9tmM=\\n-----END PRIVATE KEY-----\\n\",\"client_email\":\"meteor-sa@gotocompany-meteor.iam.gserviceaccount.com\",\"client_id\":\"110059943435984635286\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\":\"https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40gotocompany-meteor.iam.gserviceaccount.com\"}"

		userService := new(mockUsersListCall)
		userService.On("Do").Return(adminUsers).Once().Return(&admin.Users{Users: adminUsers}, nil)
		defer userService.AssertExpectations(t)

		factory := new(mockUsersServiceFactory)
		factory.On("BuildUserService", ctx, userEmail, serviceAcc).Return(userService, nil)
		defer factory.AssertExpectations(t)

		extr := gsuite.New(utils.Logger, factory)
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"user_email":           userEmail,
				"service_account_json": serviceAcc,
			},
		},
		)
		require.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		require.NoError(t, err)

		utils.AssertEqualProtos(t, expectedData, emitter.GetAllData())
	})
}

type mockUsersServiceFactory struct {
	mock.Mock
}

func (m *mockUsersServiceFactory) BuildUserService(ctx context.Context, email, serviceAccountJSON string) (gsuite.UsersListCall, error) {
	args := m.Called(ctx, email, serviceAccountJSON)
	return args.Get(0).(gsuite.UsersListCall), args.Error(1)
}

type mockUsersListCall struct {
	mock.Mock
}

func (m *mockUsersListCall) Do(opts ...googleapi.CallOption) (*admin.Users, error) {
	var args mock.Arguments
	if len(opts) > 0 {
		args = m.Called(opts)
	} else {
		args = m.Called()
	}
	return args.Get(0).(*admin.Users), args.Error(1)
}
