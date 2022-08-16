# Google Workspace

## Usage

```yaml
source:
  scope: my-scope
  type: googleworkspace
  config:     
    service_account_json: "{\"type\":\"service_account\",\"project_id\":\"meteor-sa\",\"private_key_id\":\"3cb2103ef7883845a2befe6ff83d616757\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9wEFAASCBKcwggSjAgEAAoIBAQDF/cDQ++JnH9+9\\n3YBm4APqPbvfj6eHSdAUSjzKden0lgYGgdxC7xPS1PVo+ENw+pBAH3NoRwQWYn\\nHYj064sMvmR5TcMQpnxYG86TGaPuIh30grz5dI39dtrUjttWWvtvqRv0qu7I5\\nuEL2OLUz509Q3AvuqvQVCZc7sDjNr2TPOsLeuCkpmcmBHyNdOai29bhoS+Ac\\n5ipTGF0FvT1f5KlJcHfsNoOGPJYePTaGxOW1zk680Z1WFyB1xX9iw5/GUA3XM\\neon4p9X31ASgWTqplFZhwvcpoaYpxcuxyvefR44emnfveUY91h6wLvF/mPBElO\\npXOiVJ3lAgMBAAggEALZwVYz8nSmTWFMW2OtyvojIq+ab864ZGPCpW4zfzF4BI\\n7o5TSIsNOMQMrawFUz0xZkgofJThfOscyXydDHjHXT3wXI9JTWT8l275ssvFQVy1\\nVyAJI/Kize9ru5GnnEzV2sZoYEmOsB2xgqjvKXR9NJ6wFp8Ubp9/+v2lTv1n\\nUCBBYPsPyVmUq677HfMVVa6ZpxCTWvbQga+/ZPaqppgGps5yLDqcp1A/lDCKBtqk\\njaQXHqKjuYUsoiyl2vbPbwGxIzYSv6gQfe7aeCouf8bI4GzCPmoyVPMRFpQJ6Ahp\\nMnCE96KfVVUARh1goxEEwMmSFyBPYFbmvXLPUGNfcQKBgQD3nrDHeWxW+0MjnaYD\\novXKvpnv1NiBCywOhxadJfgMZX0cfpnTDGXKPBI5ZbUywxk0sewu382JoArM\\n1w2wEIqH+73FGiMVpAuN2DpNX5mOC+z/zjFdOFZ28jkRUy8T+PTkajj7rkB7VDOr\\nIiCZwbQFwhErWS1fZgg2PcQDMsRgDBfsNRX3FHzIEZU94PP1KOc2\\nEUUzcwIV0cNOVzSyOUn2qrcYNg/hZZpGeRBBwyOcDGsqxmz5FAzk0OtbSCaMxybF\\n8NXFDhmnfIyVBjvNBWPckcR1LCZcKGTqVLH/rhPiNhyzH3NQ0c3Gl15GPgzkD\\nboLfFN3jtQKBgG++blpmYkzScNb2wr9rX+5Rm1hOvjFl4EilOb+1rq/WPZ0ig5ZD\\nT5mdQ6ZC+5ppWp8AyjQsgsAYgUG1NoqAFg45OLrrERWMmP6gHBKz3IOkO8CNgzNh\\nUoeV7/cXkkdOObWSqLkXcoWpejHtqq905C9epIyBdZ/YI4mXUJq4hPQRAoGBAK9F\\nMO9dfouVP63f/Nf3GeIlE1r5IOX4di3qNe/PqBvaCWe2Mi36Q78MdJ\\nYK8+3Z4AUD93WtZI4eWIMw+dj0zaNowldZZfSQO0Tnl/yaYCNq8M88pjhRa8pnVC\\nNxSG3ZREi3yhgIeCrvXOpS32celRC65MDdiBFAoGAHbURTEkQDZaWPAmVv+0q\\nYaT7x+UzQDGKy/By9QLGM/U2gvLvzmoeh99BTsQopPB/QuAfJNIHk9h0ohXJ\\nfA/X4T3F2LGhZ9+bujVyCQc0tTxuh41t2ipJPWtDP52rXk1AkCnIeWD+UHI0u5Ba\\nhI1dzLIx3bESrc/9tmM=\\n-----END PRIVATE KEY-----\\n\",\"client_email\":\"meteor-sa@meteor-sa.iam.gserviceaccount.com\",\"client_id\":\"1100599984635286\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\":\"https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40meteor-sa.iam.gserviceaccount.com\"}"
    user_email: meteor@odpf.com
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `user_email` | `string` | `meteor@odpf.com` | User email authorized to access the APIs | *required* |
| `service_account_json` | `string` | `{
    "type": "service_account",
    "project_id": "odpf-project",
    "private_key_id": "3cb2saasa3ef788dvdvdvdvdvdssdvds57",
    "private_key": "-----BEGIN PRIVATE KEY-----\njbjabdjbajd\n-----END PRIVATE KEY-----\n",
    "client_email": "meteor-sa@odpf-project.iam.gserviceaccount.com",
    "client_id": "1100599572858548635286",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/meteor-sa%40odpf-project.iam.gserviceaccount.com"
}` | Service Account JSON object | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `john.doe@gmail.com` |
| `resource.name` | `John Doe` |
| `email` | `john.doe@gmail.com` |
| `full_name` | `John Doe` |
| `status` | `not suspended` |
| `properties` | `{"attributes":{"aliases":"doe.john@gmail.com,john.doe0@gmail.com","manager":"christian.aristika@gmail.com","org_unit_path":"/"}}`

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
