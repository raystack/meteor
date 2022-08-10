//go:build plugins
// +build plugins

package googleworkspace_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/googleworkspace"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	utilities "github.com/odpf/meteor/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var urnScope string = "test-googleworkspace"

func TestInit(t *testing.T) {
	t.Run("should return error for empty user email", func(t *testing.T) {
		err := googleworkspace.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"user_email":           "",
				"service_account_json": "{\"type\":\"service_account\",\"project_id\":\"odpf-meteor\",\"private_key_id\":\"3cb2103ef7883845a5fdcsvdefe6ff83d616757\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggdvdvdEAAoIBAQDF/cDQ++JnH9+9\\n3YBm4APqPbvfj6eHSdAUSjzKdbdfbdbYGgdxC7xPS1PVo+ENw+pBAH3NoRwQWYEin\\nHYj064sMvm8vbR5TcMQpnxYG86TGaPuIh30grz5dI39dtrUjttbdfbdvqRv0qu7I5\\nuELzp2OLUz509Q3AvuqvQVCZc7sDjNr2TPOsLeuCkpmcmBHbdfdi29bhoS+Ac\\n5ipT10yGF0FvT1f5KlJcHfsNoOGPJYePTaGxOW1zk680Z1Wdfbdf1xX9iw5/GUA3XM\\neon4p9X31ASgwbdbdplFZhwvcpoaYpxcuxyvefR44emnfveUY91h6wLvF/mPBElO\\npXOiVJ3lAgMBAAECggEbdbddYz8nSmTWFMW2OtyvojIq+ab864ZGPCpW4zfzF4BI\\n7o5TSIsNOMQMrawFUz0xZkgofJThfOscyXbbdbdbfbfT3wXI9JTWT8l275ssvFQVy1\\nVyAJI/Kize9ru5GnnEzV2sZoYEmOsB2xgqjvKXR90r5wNJ6wFp8Ubp9/+v2lTv1n\\nUCBBYPsPyVmUq677HfMVVa6ZpxCTWvbQga+/ZPaqppgGps5yLDqc434c3A/lDCKBtqk\\njaQXHqKjuYUsoiyl2vbPbwGxc34343c6gQfe7aeCouf8bI4GzCPmoyVPMRFpQJ6Ahp\\nMnCE96KfVVUARh1goxEEwMmSFyBPYFbmvXLPUGNfcQKBgQD3nrDHeWxW+0MjnaYD\\novXKvpnv1NiBCywOAEfc343535dJfgMZX0cfpnTDGXKPBI5ZbUywxk0sewu382JoArM\\n1w2wEIqH+73FGiMVpAuN2DpNX5mOC+z/zjFdOFZ28jkRUy8T+PTkajj7rkB7VDOr\\nIiCZwRrnbQFwhErWS1fZgg2PcQKBgQDMsRgDBfhgJX9sNRX3FHzIEZU94PP1KOc2\\nEUUzcwIV0cNOVzSyOUn2qrcYNg/hZZpGeRBBwyOcDGsqxmz5FAzk0OtbSCaMxybF\\n8NXFDh3ELmnfIyVBjvNBWPckcR1LCZcKGTqVLH/rhPiNhyzH3NQ0c3Gl15GPgzkD\\nboLfFN3jtQKBgG++blpmYkzScNb2wr9rX+5Rm1hOvjFl4EilOb+1rq/WPZ0ig5ZD\\nT5mdQ6ZC+5ppWp8AyjQsgsAYgUG1NoqAFg45OLrrERWMmP6gHBKz3IOkO8CNgzNh\\nUoeV7/cXkkdOObWSqLkXcoWpejHtqq905C9epIyBdZ/YI4mXU3c4343c4QRAoGBAK9F\\nMO9dzFjfouVP63f/Nf3GeIlctuiE1r5IOX4di3qNe/P33iqBvaCWe2Mi36Q78MdJ\\nYK8+3Z4AUD93WtZI4eWIMw+dj0zaNowldZZfSQO0Tnl/yaYCNq8M88pjhRa8pnVC\\nNxSG3x4XZREi3yhgIeCrvXOpS32celRC65MDdiBFAoGAHbURTEkQDZaWPAmVv+0q\\nYaT7x+UzQDGKy/By9QLGM/U2gvLGTw1vzmoeh99BTsQopPB/QuAfJNIHk9h0ohXJ\\nfA/X4T3F2LGhZ9+bujVyCQc0tTxuh41t2ipJPWtDP52rXk1AkCnIeWD+UHI0u5Ba\\nhI1dzLIxZKeq3bESrc/9tmM=\\n-----END PRIVATE KEY-----\\n\",\"client_email\":\"meteor-sa@odpf-meteor.iam.gserviceaccount.com\",\"client_id\":\"110059943435984635286\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\":\"https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40odpf-meteor.iam.gserviceaccount.com\"}",
			}})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return error for invalid service account json", func(t *testing.T) {
		err := googleworkspace.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"user_email":           "user@example.com",
				"service_account_json": "invalide json",
			}})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract user details from google workspace", func(t *testing.T) {

		expectedData := []models.Record{
			models.NewRecord(&assetsv1beta1.User{
				Resource: &commonv1beta1.Resource{
					Service: "google workspace",
					Name:    "odpf admin",
				},
				Email:    "admin@odpf.com",
				FullName: "John Doe",
				LastName: "Doe",
				Status:   "not suspended",
				Properties: &facetsv1beta1.Properties{
					Attributes: utilities.TryParseMapToProto(map[string]interface{}{
						"manager": "lorem@odpf.com",
					}),
				},
			}),
			models.NewRecord(&assetsv1beta1.User{
				Resource: &commonv1beta1.Resource{
					Service: "google workspace",
					Name:    "ipsum",
				},
				Email:    "ipsum@odpf.com",
				FullName: "Ipsum Lorum",
				LastName: "Lorum",
				Status:   "not suspended",
				Properties: &facetsv1beta1.Properties{
					Attributes: utilities.TryParseMapToProto(map[string]interface{}{
						"manager": "manager@odpf.com",
					}),
				},
			}),
		}

		ctx := context.TODO()

		emitter := mocks.NewEmitter()
		extractor := mocks.NewExtractor()
		extractor.On("Init", mock.AnythingOfType("*context.emptyCtx"), mock.AnythingOfType("plugins.Emit")).Return(nil)
		extractor.SetEmit(expectedData)
		extractor.On("Extract", mock.AnythingOfType("*context.emptyCtx"), mock.AnythingOfType("plugins.Emit")).Return(nil)

		err := extractor.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"user_email":           "test@odpf.com",
				"service_account_json": "{\"type\":\"service_account\",\"project_id\":\"odpf\",\"private_key_id\":\"3cb2103ef7883455a5f09712befe6ff83d616757\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBGDSNBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDF/cDQ++JnH9+9\\n3YBm4APqPbvfj6eHSdASSjzKden0lgYGgdxC7xPS1PVo+ENw+pBAH3NoRwQWYEin\\nHYj064sMvm8vbR5TcMQpnxYG8SAGaPuIh30grz5dI39dtrUjttWWvtvqRv0qu7I5\\nuELzp2OLUz509Q3AvuqvQVCZc7sDjNr2TPOsLeuCkpmcmFDyNdOa6vi29bhoS+Ac\\n5ipT10yGF0FvT1f5KlJcHfsNoOGPJYeTRaGxOW1zk680Z1WFyB1xX9iw5/GUA3XM\\neon4p9X31ASgw6WTqplGDhwvcpoaYpxcuxyvefR44emnfveUY91h6wLvF/mPBElO\\npXOiVJ3lAgMBAAECggEALZwVYz8nSmTWFMW2OtyvojIq+ab864BGHFpW4zfzF4BI\\n7o5TSIsNOMQMrawFUz0xZkgofJJHhfOscyXydDHjHXT3wXI9JTWT8l275ssvFQVy1\\nVyAJI/Kize9ru5GnnEzV2sZoTYmOsB2xgqjvKXR90r5wNJ6wFp8Ubp9/+v2lTv1n\\nUCBBYPsPyVmUq677HfMBBa6ZpxCTWvbQga+/ZPaqppgGps5yLDqcp1A/lDCKBtqk\\njaQXHqKjuYDSsoiyl2vbPbwGxIzYSv6gQfe7aeCouf8bI4GzCPmoyVPMRFpQJ6Ahp\\nMnCE96KfVVUARh1goxEEwMmSFyBPYFbmvXLPUGNfcQKBgQD3nrDHeWxW+0MjnaYD\\novXKvpnv1NiBCywOAEfIhxadJfgMZX0cfpnTDGXKPBI5ZbUywxk0sewu382JoArM\\n1w2wEIqH+73FGiMVpAuN2DpNX5mOC+z/zjFdOFZ28jkRUy8T+PTkajj7rkB7VDOr\\nIiCZwRrnbQFwhErWS1fZgg2PcQKBgQDMsRgDBfhgJX9sNRX3FHzIEZU94PP1KOc2\\nEUUzcwIV0cNOVzSyOUn2qrcYNg/hZZpGeRBBwyOcDGsqxmz5FAzk0OtbSCaMxybF\\n8NXFDh3ELmnfIyVBjvNBWPckcR1LCZcKGTqVLH/rhPiNhyzH3NQ0c3Gl15GPgzkD\\nboLfFN3jtQKBgG++blpmYkzScNb2wr9rX+5Rm1hOvjFl4EilOb+1rq/WPZ0ig5ZD\\nT5mdQ6ZC+5ppWp8AyjQsgsAYgUG1NoqAFg45OLrrERWMmP6gHBKz3IOkO8CNgzNh\\nUoeV7/cXkkdOObWSqLkXcoWpejHtqq905C9epIyBdZ/YI4mXUJq4hPQRAoGBAK9F\\nMO9dzFjfouVP63f/Nf3GeIlctuiE1r5IOX4di3qNe/P33iqBvaCWe2Mi36Q78MdJ\\nYK8+3Z4AUD93WtZI4eWIMw+dj0zaNowldZZfSQO0Tnl/yaYCNq8M88pjhRa8pnVC\\nNxSG3x4XZREi3yhgIeCrvXOpS32celRC65MDdiBFAoGAHbURTEkQDZaWPAmVv+0q\\nYaT7x+UzQDGKy/By9QLGM/U2gvLGTw1vzmoeh99BTsQopPB/QuAfJNIHk9h0ohXJ\\nfA/X4T3F2LGhZ9+bujVyCQc0tTxuh41t2ipJPWtDP52rXk1AkCnIeWD+UHI0u5Ba\\nhI1dzLIxZKeq3bESrc/9tmM=\\n-----END PRIVATE KEY-----\\n\",\"client_email\":\"meteor-sa@odpf.iam.gserviceaccount.com\",\"client_id\":\"110059957285984635286\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\":\"https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40odpf.iam.gserviceaccount.com\"}",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		err = extractor.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		assert.EqualValues(t, expectedData, emitter.Get())
	})
}
