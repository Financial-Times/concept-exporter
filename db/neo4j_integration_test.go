//go:build integration
// +build integration

package db

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Financial-Times/annotations-rw-neo4j/v4/annotations"
	"github.com/Financial-Times/base-ft-rw-app-go/v2/baseftrwapp"
	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/content-rw-neo4j/v3/content"
	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	contentUUID                 = "a435b4ec-b207-4dce-ac0a-f8e7bbef310b"
	brandParentUUID             = "dbb0bdae-1f0c-1a1a-b0cb-b2227cce2b54"
	brandChildUUID              = "ff691bf8-8d92-1a1a-8326-c273400bff0b"
	brandGrandChildUUID         = "ff691bf8-8d92-2a2a-8326-c273400bff0b"
	financialInstrumentUUID     = "77f613ad-1470-422c-bf7c-1dd4c3fd1693"
	companyUUID                 = "eac853f5-3859-4c08-8540-55e043719400"
	organisationUUID            = "5d1510f8-2779-4b74-adab-0a5eb138fca6"
	personUUID                  = "b2fa511e-a031-4d52-b37d-72fd290b39ce"
	personWithBrandUUID         = "9070a3f1-aa6d-48a7-9d97-f56a47513cef"
	industryClassificationUUID  = "49da878c-67ce-4343-9a09-a4a767e584a2"
	industryClassificationUUID2 = "38ee195d-ebdd-48a9-af4b-c8a322e7b04d"
)

var allUUIDs = []string{contentUUID, brandParentUUID, brandChildUUID, brandGrandChildUUID, financialInstrumentUUID, companyUUID, organisationUUID, personUUID, personWithBrandUUID, industryClassificationUUID, industryClassificationUUID2, "eac853f5-3859-4c08-8540-55e043719401", "eac853f5-3859-4c08-8540-55e043719402", "dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54", "a7b4786c-aae9-3e3e-93a0-2c82a6383534", "22a60434-a9d5-3a38-a337-fdd904e99f6f"}

func getNeo4jDriver(t *testing.T) *cmneo4j.Driver {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "bolt://localhost:7687"
	}

	log := logger.NewUPPLogger("concept-exporter-test", "PANIC")
	driver, err := cmneo4j.NewDefaultDriver(url, log)

	assert.NoError(t, err, "Creating new neo driver failed")

	return driver
}

func TestNeoService_ReadBrand(t *testing.T) {
	driver := getNeo4jDriver(t)

	log := logger.NewUPPLogger("concept-exporter-test", "PANIC")
	svc := concepts.NewConceptService(driver, log)
	assert.NoError(t, svc.Initialise())

	cleanDB(t, driver)
	writeBrands(t, &svc)
	writeContent(t, driver)
	writeAnnotation(t, driver, fmt.Sprintf("./fixtures/Annotations-%s.json", contentUUID), "v1")

	neoSvc := NewNeoService(driver, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Brand", conceptCh)

	assert.NoError(t, err, "Error reading from Neo")
	assert.True(t, found)
	assert.Equal(t, 1, count)
waitLoop:
	for {
		select {
		case c, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			assert.Equal(t, "ff691bf8-8d92-1a1a-8326-c273400bff0b", c.UUID)
			assert.Equal(t, "http://api.ft.com/things/ff691bf8-8d92-1a1a-8326-c273400bff0b", c.ID)
			assert.Equal(t, "http://api.ft.com/brands/ff691bf8-8d92-1a1a-8326-c273400bff0b", c.APIURL)
			assert.Equal(t, "Business School video", c.PrefLabel)
			assertListContainsAll(t, []string{"Thing", "Concept", "Brand", "Classification"}, c.Labels)
			assert.Empty(t, c.LeiCode)
			assert.Empty(t, c.FigiCodes)
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

func TestNeoService_DoNotReadBrokenConcepts(t *testing.T) {
	driver := getNeo4jDriver(t)

	log := logger.NewUPPLogger("concept-exporter-test", "PANIC")
	svc := concepts.NewConceptService(driver, log)
	assert.NoError(t, svc.Initialise())

	tests := []struct {
		name                string
		conceptType         string
		conceptFixture      string
		annotationsFixture  string
		annotationsPlatform string
		brokenConceptUUID   string
	}{
		{
			name:                "Brands",
			conceptType:         "Brand",
			conceptFixture:      fmt.Sprintf("./fixtures/Brand-%s-child.json", brandChildUUID),
			annotationsFixture:  fmt.Sprintf("./fixtures/Annotations-%s.json", contentUUID),
			annotationsPlatform: "v1",
			brokenConceptUUID:   brandChildUUID,
		},
		{
			name:                "Organisations",
			conceptType:         "Organisation",
			conceptFixture:      fmt.Sprintf("./fixtures/Organisation-Fakebook-%s.json", companyUUID),
			annotationsFixture:  fmt.Sprintf("./fixtures/Annotations-%s-org.json", contentUUID),
			annotationsPlatform: "v2",
			brokenConceptUUID:   companyUUID,
		},
		{
			name:                "People",
			conceptType:         "Person",
			conceptFixture:      fmt.Sprintf("./fixtures/Person-%s.json", personUUID),
			annotationsFixture:  fmt.Sprintf("./fixtures/Annotations-%s-person.json", contentUUID),
			annotationsPlatform: "pac",
			brokenConceptUUID:   personUUID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanDB(t, driver)
			writeJSONToConceptService(t, &svc, test.conceptFixture)
			writeContent(t, driver)
			writeAnnotation(t, driver, test.annotationsFixture, test.annotationsPlatform)

			// Delete canonical node so we can check that we are not returning broken concepts
			query := &cmneo4j.Query{
				Cypher: "MATCH (c:Concept{prefUUID:$uuid}) DETACH DELETE c",
				Params: map[string]interface{}{
					"uuid": test.brokenConceptUUID,
				},
			}
			err := driver.Write(query)
			if err != nil {
				t.Fatalf("Error deleting canonical node: %v", err)
			}

			neoSvc := NewNeoService(driver, "not-needed")

			conceptCh := make(chan Concept)
			count, found, err := neoSvc.Read(test.conceptType, conceptCh)

			assert.NoError(t, err, "Error reading from Neo")
			assert.False(t, found)
			assert.Equal(t, 0, count)
		})
	}
}

func TestNeoService_ReadHasBrand(t *testing.T) {
	driver := getNeo4jDriver(t)

	log := logger.NewUPPLogger("concept-exporter-test", "PANIC")
	svc := concepts.NewConceptService(driver, log)
	assert.NoError(t, svc.Initialise())

	cleanDB(t, driver)
	writeBrands(t, &svc)
	writeContent(t, driver)
	writeAnnotation(t, driver, fmt.Sprintf("./fixtures/Annotations-%s-hasBrand.json", contentUUID), "v1")

	neoSvc := NewNeoService(driver, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Brand", conceptCh)

	assert.NoError(t, err, "Error reading from Neo")
	assert.True(t, found)
	assert.Equal(t, 1, count)
waitLoop:
	for {
		select {
		case c, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			assert.Equal(t, "ff691bf8-8d92-1a1a-8326-c273400bff0b", c.UUID)
			assert.Equal(t, "http://api.ft.com/things/ff691bf8-8d92-1a1a-8326-c273400bff0b", c.ID)
			assert.Equal(t, "http://api.ft.com/brands/ff691bf8-8d92-1a1a-8326-c273400bff0b", c.APIURL)
			assert.Equal(t, "Business School video", c.PrefLabel)
			assertListContainsAll(t, []string{"Thing", "Concept", "Brand", "Classification"}, c.Labels)
			assert.Empty(t, c.LeiCode)
			assert.Empty(t, c.FigiCodes)
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

func TestNeoService_ReadOrganisation(t *testing.T) {
	driver := getNeo4jDriver(t)

	log := logger.NewUPPLogger("concept-exporter-test", "PANIC")
	svc := concepts.NewConceptService(driver, log)
	assert.NoError(t, svc.Initialise())

	tests := []struct {
		name               string
		fixture            string
		expectedFactsetIDs []string
	}{
		{
			name:               "Organisation with 0 Factset Sources",
			fixture:            fmt.Sprintf("./fixtures/Organisation-Fakebook-%s.json", companyUUID),
			expectedFactsetIDs: []string{},
		},
		{
			name:               "Organisation with 1 Factset Sources",
			fixture:            fmt.Sprintf("./fixtures/Organisation-Fakebook-%s-Factset.json", companyUUID),
			expectedFactsetIDs: []string{"FACTSET1"},
		},
		{
			name:               "Organisation with 2 Factset Sources",
			fixture:            fmt.Sprintf("./fixtures/Organisation-Fakebook-%s-Factset2.json", companyUUID),
			expectedFactsetIDs: []string{"FACTSET1", "FACTSET2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanDB(t, driver)
			writeJSONToConceptService(t, &svc, test.fixture)
			writeJSONToConceptService(t, &svc, fmt.Sprintf("./fixtures/FinancialInstrument-%s.json", financialInstrumentUUID))

			writeContent(t, driver)
			writeAnnotation(t, driver, fmt.Sprintf("./fixtures/Annotations-%s-org.json", contentUUID), "v2")
			neoSvc := NewNeoService(driver, "not-needed")

			conceptCh := make(chan Concept)
			count, found, err := neoSvc.Read("Organisation", conceptCh)

			assert.NoError(t, err, "Error reading from Neo")
			assert.True(t, found)
			assert.Equal(t, 1, count)
		waitLoop:
			for {
				select {
				case c, open := <-conceptCh:
					if !open {
						break waitLoop
					}
					assert.Equal(t, "eac853f5-3859-4c08-8540-55e043719400", c.UUID)
					assert.Equal(t, "http://api.ft.com/things/eac853f5-3859-4c08-8540-55e043719400", c.ID)
					assert.Equal(t, "http://api.ft.com/organisations/eac853f5-3859-4c08-8540-55e043719400", c.APIURL)
					assert.Equal(t, "Fakebook", c.PrefLabel)
					assertListContainsAll(t, []string{"Thing", "Concept", "Organisation", "PublicCompany", "Company"}, c.Labels)
					assert.Equal(t, "PBLD0EJDB5FWOLXP3B76", c.LeiCode)
					assert.Equal(t, []string{"BB8000C3P0-R2D2"}, c.FigiCodes)

					sort.Strings(test.expectedFactsetIDs)
					sort.Strings(c.FactsetIDs)
					assert.Equal(t, test.expectedFactsetIDs, c.FactsetIDs)
				case <-time.After(3 * time.Second):
					t.FailNow()
				}
			}
		})
	}
}

func TestNeoService_ReadOrganisationWithNAICS(t *testing.T) {
	driver := getNeo4jDriver(t)

	log := logger.NewUPPLogger("concept-exporter-test", "PANIC")
	svc := concepts.NewConceptService(driver, log)
	assert.NoError(t, svc.Initialise())

	tests := []struct {
		name          string
		knownUUIDs    []string
		expectedNAICS []NAICSIndustryClassification
	}{
		{
			name:       "Organisation with 1 known and 1 unknown NAICS industry classification",
			knownUUIDs: []string{industryClassificationUUID},
			expectedNAICS: []NAICSIndustryClassification{
				{IndustryIdentifier: "519130", Rank: 1},
			},
		},
		{
			name:       "Organisation with multiple known NAICS industry classifications",
			knownUUIDs: []string{industryClassificationUUID, industryClassificationUUID2},
			expectedNAICS: []NAICSIndustryClassification{
				{IndustryIdentifier: "519130", Rank: 1}, {IndustryIdentifier: "519131", Rank: 2},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanDB(t, driver)
			writeJSONToConceptService(t, &svc, fmt.Sprintf("./fixtures/Organisation-Fakebook-%s-Factset.json", companyUUID))
			for _, id := range test.knownUUIDs {
				writeJSONToConceptService(t, &svc, fmt.Sprintf("./fixtures/NAICS-Industry-Classification-%s.json", id))
			}

			writeContent(t, driver)
			writeAnnotation(t, driver, fmt.Sprintf("./fixtures/Annotations-%s-org.json", contentUUID), "v2")
			neoSvc := NewNeoService(driver, "not-needed")

			conceptCh := make(chan Concept)
			count, found, err := neoSvc.Read("Organisation", conceptCh)

			assert.NoError(t, err, "Error reading from Neo")
			assert.True(t, found)
			assert.Equal(t, 1, count)
		waitLoop:
			for {
				select {
				case c, open := <-conceptCh:
					if !open {
						break waitLoop
					}

					assert.Equal(t, test.expectedNAICS, c.NAICSIndustryClassifications)
				case <-time.After(3 * time.Second):
					t.FailNow()
				}
			}
		})
	}
}

func TestNeoService_ReadPerson(t *testing.T) {
	driver := getNeo4jDriver(t)

	log := logger.NewUPPLogger("concept-exporter-test", "PANIC")
	svc := concepts.NewConceptService(driver, log)
	assert.NoError(t, svc.Initialise())

	tests := []struct {
		name               string
		uuid               string
		conceptFixture     string
		annotationsFixture string
		expectedCount      int
		expectedPrefLabel  string
		readAs             string
	}{
		{
			name:               "Standard Person",
			uuid:               personUUID,
			conceptFixture:     fmt.Sprintf("./fixtures/Person-%s.json", personUUID),
			annotationsFixture: fmt.Sprintf("./fixtures/Annotations-%s-person.json", contentUUID),
			expectedCount:      1,
			expectedPrefLabel:  "Peter Foster",
			readAs:             "Person",
		},
		{
			name:               "Person with Brand Read As Person",
			uuid:               personWithBrandUUID,
			conceptFixture:     fmt.Sprintf("./fixtures/Person-%s-With-Brand.json", personWithBrandUUID),
			annotationsFixture: fmt.Sprintf("./fixtures/Annotations-%s-person-with-brand.json", contentUUID),
			expectedCount:      1,
			expectedPrefLabel:  "Jancis Robinson",
			readAs:             "Person",
		},
		{
			name:               "Person with Brand Not Read As Brand",
			uuid:               personWithBrandUUID,
			conceptFixture:     fmt.Sprintf("./fixtures/Person-%s-With-Brand.json", personWithBrandUUID),
			annotationsFixture: fmt.Sprintf("./fixtures/Annotations-%s-person-with-brand.json", contentUUID),
			expectedCount:      0,
			readAs:             "Brand",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanDB(t, driver)
			writeJSONToConceptService(t, &svc, test.conceptFixture)
			writeContent(t, driver)
			writeAnnotation(t, driver, test.annotationsFixture, "pac")
			neoSvc := NewNeoService(driver, "not-needed")

			conceptCh := make(chan Concept)
			count, found, err := neoSvc.Read(test.readAs, conceptCh)

			assert.NoError(t, err, "Error reading from Neo")
			assert.Equal(t, test.expectedCount, count)
			if test.expectedCount == 0 {
				assert.False(t, found)
			} else {
				assert.True(t, found)
			waitLoop:
				for {
					select {
					case c, open := <-conceptCh:
						if !open {
							break waitLoop
						}
						assert.Equal(t, test.uuid, c.UUID)
						assert.Equal(t, "http://api.ft.com/things/"+test.uuid, c.ID)
						assert.Equal(t, "http://api.ft.com/people/"+test.uuid, c.APIURL)
						assert.Equal(t, test.expectedPrefLabel, c.PrefLabel)
						assertListContainsAll(t, []string{"Thing", "Concept", "Person"}, c.Labels)
					case <-time.After(3 * time.Second):
						t.FailNow()
					}
				}
			}
		})
	}
}

func TestNeoService_ReadWithoutResult(t *testing.T) {
	driver := getNeo4jDriver(t)
	cleanDB(t, driver)
	neoSvc := NewNeoService(driver, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Brand", conceptCh)

	assert.NoError(t, err, "Error reading from Neo")
	assert.False(t, found)
	assert.Equal(t, 0, count)
waitLoop:
	for {
		select {
		case _, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			t.FailNow()
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

func TestNeoService_ReadWithError(t *testing.T) {
	driver := getNeo4jDriver(t)
	neoSvc := NewNeoService(driver, "not-needed")

	conceptCh := make(chan Concept)
	count, found, err := neoSvc.Read("Invalid Concept", conceptCh)

	assert.Error(t, err, "Expected an error when reading from Neo")
	assert.False(t, found)
	assert.Equal(t, 0, count)
waitLoop:
	for {
		select {
		case _, open := <-conceptCh:
			if !open {
				break waitLoop
			}
			t.FailNow()
		case <-time.After(3 * time.Second):
			t.FailNow()
		}
	}
}

func assertListContainsAll(t *testing.T, list interface{}, items ...interface{}) {
	if reflect.TypeOf(items[0]).Kind().String() == "slice" {
		expected := reflect.ValueOf(items[0])
		expectedLength := expected.Len()
		for i := 0; i < expectedLength; i++ {
			assert.Contains(t, list, expected.Index(i).Interface())
		}
	} else {
		for _, item := range items {
			assert.Contains(t, list, item)
		}
	}
}

func writeAnnotation(t *testing.T, driver *cmneo4j.Driver, pathToJSON, platform string) {
	annrw := annotations.NewCypherAnnotationsService(driver)
	assert.NoError(t, annrw.Initialise())
	writeJSONToAnnotationService(t, annrw, pathToJSON, contentUUID, platform)
}

func writeContent(t *testing.T, driver *cmneo4j.Driver) {
	contentRW := content.NewContentService(driver)
	require.NoError(t, contentRW.Initialise())
	writeJSONToContentService(t, contentRW, fmt.Sprintf("./fixtures/Content-%s.json", contentUUID))
}

func writeBrands(t *testing.T, service concepts.ConceptServicer) {
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-parent.json", brandParentUUID))
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-child.json", brandChildUUID))
	writeJSONToConceptService(t, service, fmt.Sprintf("./fixtures/Brand-%s-grand_child.json", brandGrandChildUUID))
}

func writeJSONToConceptService(t *testing.T, service concepts.ConceptServicer, pathToJsonFile string) {
	f, err := os.Open(pathToJsonFile)
	require.NoError(t, err)
	dec := json.NewDecoder(f)
	inst, _, err := service.DecodeJSON(dec)
	require.NoError(t, err)
	_, err = service.Write(inst, "trans_id")
	require.NoError(t, err)
	f.Close()
}

func writeJSONToContentService(t *testing.T, service baseftrwapp.Service, pathToJsonFile string) {
	f, err := os.Open(pathToJsonFile)
	require.NoError(t, err)
	dec := json.NewDecoder(f)
	inst, _, err := service.DecodeJSON(dec)
	require.NoError(t, err)
	require.NoError(t, service.Write(inst, "trans_id"))
	f.Close()
}

func writeJSONToAnnotationService(t *testing.T, service annotations.Service, pathToJsonFile, uuid, platform string) {
	f, err := os.Open(pathToJsonFile)
	require.NoError(t, err)
	dec := json.NewDecoder(f)
	inst, err := service.DecodeJSON(dec)
	require.NoError(t, err)
	require.NoError(t, service.Write(uuid, fmt.Sprintf("annotations-%s", platform), platform, "trans_id", inst))
	f.Close()
}

//DELETES ALL DATA! DO NOT USE IN PRODUCTION!!!
func cleanDB(t *testing.T, driver *cmneo4j.Driver) {
	qs := make([]*cmneo4j.Query, len(allUUIDs))
	for i, uuid := range allUUIDs {
		qs[i] = &cmneo4j.Query{
			Cypher: `MATCH (a:Thing{uuid:$uuid})
			OPTIONAL MATCH (a)-[:EQUIVALENT_TO]-(t:Thing)
			DETACH DELETE t, a`,
			Params: map[string]interface{}{
				"uuid": uuid,
			},
		}
	}
	err := driver.Write(qs...)
	assert.NoError(t, err)
}
