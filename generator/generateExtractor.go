package generator

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func check(e error) {
	if e != nil {
		// panic(e)
		fmt.Println("Error")
	}
}
func createEmptyFile(name string) {
	d := []byte("")
	check(ioutil.WriteFile(name, d, 0644))
}

func GenerateExtractor(extractorType string, extractorName string) {

	if extractorName == "" || extractorType == "" {
		fmt.Println("Flags missing")
		return
	}

	extractorType = strings.Title(extractorType)
	message := fmt.Sprintf("Creating your extractor %v of %v type", extractorName, extractorType)
	fmt.Println(message)

	newExtractorData, err := ioutil.ReadFile("generator/extractor.txt")
	if err != nil {
		fmt.Println("newExtractor File reading error", err)
		return
	}
	newExtractorDataString := string(newExtractorData)
	newExtractorDataString = strings.ReplaceAll(newExtractorDataString, "<extractor>", extractorName)
	newExtractorDataString = strings.ReplaceAll(newExtractorDataString, "<type>", extractorType)
	fmt.Println(newExtractorDataString)

	newExtractorTestData, err := ioutil.ReadFile("generator/extractor_test.txt")
	if err != nil {
		fmt.Println("newExtractorTest File reading error", err)
		return
	}
	newExtractorTestDataString := string(newExtractorTestData)
	newExtractorTestDataString = strings.ReplaceAll(newExtractorTestDataString, "<extractor>", extractorName)
	fmt.Println(newExtractorTestDataString)

	newExtractorReadmeData, err := ioutil.ReadFile("generator/README.txt")
	if err != nil {
		fmt.Println("newExtractorReadme File reading error", err)
		return
	}
	newExtractorReadmeDataString := string(newExtractorReadmeData)
	newExtractorReadmeDataString = strings.ReplaceAll(newExtractorReadmeDataString, "<extractor>", extractorName)

	fmt.Println(newExtractorReadmeDataString)

	err = os.Chdir("plugins/extractors/")
	check(err)
	err = os.Mkdir(extractorName, 0755)
	if err == nil {
		return
	}

	newExtractor := extractorName + "/extractor.go"
	createEmptyFile(newExtractor)

	newExtractorTest := extractorName + "/extractor_test.go"
	createEmptyFile(newExtractorTest)

	newExtractorReadme := extractorName + "/README.md"
	createEmptyFile(newExtractorReadme)
}
