package tableau

// Credentials OUTPUT
/*
	{
	   "credentials": {
	      "token": "xfG6QjjNR5qc9-PMrXXp5w|CB8mCZWgmZY4ShQZuVXXjNDL9LVhKlZz",
	      "site": {
	         "id": "82f5eafd-04ca-473a-b5f8-9020c0382122",
	         "contentUrl": "testmetadatadev713856"
	      },
	      "user": {
	         "id": "fa25b76f-9c8d-4bb9-899f-c29ff0c6a89a"
	      }
	   }
	}
*/
// api/3.4/auth/signin
type Credentials struct {
	Token string `json:"token"`
	Site  []Site `json:"site"`
	User  []User `json:"user"`
}
type Site struct {
	Id         string `json:"id"`
	ContentUrl string `json:"contentUrl"`
}
type User struct {
	Id string `json:"id"`
}

type Pagination struct {
	PageNumber     string `json:"pageNumber"`
	PageSize       string `json:"pageSize"`
	TotalAvailable string `json:"totalAvailable"`
}

type Projects struct {
	Project []struct {
		Id                              string `json:"id"`
		Name                            string `json:"name"`
		Description                     string `json:"description"`
		TopLevelProject                 string `json:"topLevelProject"`
		Writeable                       string `json:"writeable"`
		ControllingPermissionsProjectId string `json:"controllingPermissionsProjectId"`
		CreatedAt                       string `json:"createdAt"`
		UpdatedAt                       string `json:"updatedAt"`
		ContentPermissions              string `json:"contentPermissions"`
		ParentProjectId                 string `json:"parentProjectId"`
		Owner                           struct {
			Email     string `json:"email"`
			FullName  string `json:"fullName"`
			Id        string `json:"id"`
			LastLogin string `json:"lastLogin"`
			Name      string `json:"name"`
			SiteRole  string `json:"siteRole"`
		} `json:"owner"`
		ContentCounts struct {
			ProjectCount    string `json:"projectCount"`
			WorkbookCount   string `json:"workbookCount"`
			ViewCount       string `json:"viewCount"`
			DatasourceCount string `json:"datasourceCount"`
		} `json:"contentCounts"`
	} `json:"project"`
	Text string `json:"#text"`
}
/*
// obtain through api/site/site-id/projects been splitted above into Pagination and Project
type T2 struct {
	Pagination struct {
		PageNumber     string `json:"pageNumber"`
		PageSize       string `json:"pageSize"`
		TotalAvailable string `json:"totalAvailable"`
	} `json:"pagination"`
	Projects struct {
		Project []struct {
			Id                              string `json:"id"`
			Name                            string `json:"name"`
			Description                     string `json:"description"`
			TopLevelProject                 string `json:"topLevelProject"`
			Writeable                       string `json:"writeable"`
			ControllingPermissionsProjectId string `json:"controllingPermissionsProjectId"`
			CreatedAt                       string `json:"createdAt"`
			UpdatedAt                       string `json:"updatedAt"`
			ContentPermissions              string `json:"contentPermissions"`
			ParentProjectId                 string `json:"parentProjectId"`
			Owner                           struct {
				Email     string `json:"email"`
				FullName  string `json:"fullName"`
				Id        string `json:"id"`
				LastLogin string `json:"lastLogin"`
				Name      string `json:"name"`
				SiteRole  string `json:"siteRole"`
			} `json:"owner"`
			ContentCounts struct {
				ProjectCount    string `json:"projectCount"`
				WorkbookCount   string `json:"workbookCount"`
				ViewCount       string `json:"viewCount"`
				DatasourceCount string `json:"datasourceCount"`
			} `json:"contentCounts"`
		} `json:"project"`
		Text string `json:"#text"`
	} `json:"projects"`
}
 */