package upstream

import (
	"regexp"
	"strings"
)

type QueryParser func(query string) []Resource

var (
	topLevelUpstreamsPattern = regexp.MustCompile("" +
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-\\*?]+)`?" +
		"|" +
		"(?i)(?:JOIN)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
		"|" +
		"(?i)(?:WITH)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?\\s+(?:AS)" +
		"|" +
		"(?i)(?:VIEW)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
		"|" +
		"(?i)(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`\\s*(?:AS)?")

	singleLineCommentsPattern = regexp.MustCompile(`(--.*)`)
	multiLineCommentsPattern  = regexp.MustCompile(`(((/\*)+?[\w\W]*?(\*/)+))`)
	specialCommentPattern     = regexp.MustCompile(`(/\*\s*(@[a-zA-Z0-9_-]+)\s*\*/)`)
)

func ParseTopLevelUpstreamsFromQuery(query string) []Resource {
	cleanedQuery := cleanQueryFromComment(query)

	resourcesFound := make(map[Resource]bool)
	pseudoResources := make(map[Resource]bool)

	matches := topLevelUpstreamsPattern.FindAllStringSubmatch(cleanedQuery, -1)

	for _, match := range matches {
		var projectIdx, datasetIdx, nameIdx, ignoreUpstreamIdx int
		tokens := strings.Fields(match[0])
		clause := strings.ToLower(tokens[0])

		switch clause {
		case "from":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 1, 2, 3, 4
		case "join":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 5, 6, 7, 8
		case "with":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 9, 10, 11, 12
		case "view":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 13, 14, 15, 16
		default:
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 17, 18, 19, 20
		}

		project := match[projectIdx]
		dataset := match[datasetIdx]
		name := match[nameIdx]

		if project == "" || dataset == "" || name == "" {
			continue
		}

		if strings.TrimSpace(match[ignoreUpstreamIdx]) == "@ignoreupstream" {
			continue
		}

		if clause == "view" {
			continue
		}

		resource := Resource{
			Project: project,
			Dataset: dataset,
			Name:    name,
		}

		if clause == "with" {
			pseudoResources[resource] = true
		} else {
			resourcesFound[resource] = true
		}
	}

	var output []Resource

	for resource := range resourcesFound {
		if pseudoResources[resource] {
			continue
		}
		output = append(output, resource)
	}

	return output
}

func cleanQueryFromComment(query string) string {
	cleanedQuery := singleLineCommentsPattern.ReplaceAllString(query, "")

	matches := multiLineCommentsPattern.FindAllString(query, -1)
	for _, match := range matches {
		if specialCommentPattern.MatchString(match) {
			continue
		}
		cleanedQuery = strings.ReplaceAll(cleanedQuery, match, "")
	}

	return cleanedQuery
}
