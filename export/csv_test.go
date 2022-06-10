package export

import (
	"testing"

	"github.com/Financial-Times/concept-exporter/db"
	"github.com/stretchr/testify/assert"
)

var supportedConceptTypes = []string{"Brand", "Topic", "Location", "Person", "Organisation"}

func TestGetHeader(t *testing.T) {
	for _, conceptType := range supportedConceptTypes {
		header := getHeader(conceptType)
		if conceptType == "Organisation" {
			assert.Equal(t, []string{"id", "prefLabel", "apiUrl", "leiCode", "factsetId", "FIGI", "NAICS"}, header)
		} else {
			assert.Equal(t, []string{"id", "prefLabel", "apiUrl"}, header)
		}
	}
}

func TestConceptToCSVRecord(t *testing.T) {
	tests := map[string]struct {
		concept     db.Concept
		conceptType string
		expected    []string
	}{
		"transform brand": {
			concept: db.Concept{
				ID:        "http://api.ft.com/things/dbb0bdae-1f0c-1a1a-b0cb-b2227cce2b54",
				PrefLabel: "Financial Times",
				APIURL:    "http://api.ft.com/brands/dbb0bdae-1f0c-1a1a-b0cb-b2227cce2b54",
			},
			conceptType: "Brand",
			expected: []string{
				"http://api.ft.com/things/dbb0bdae-1f0c-1a1a-b0cb-b2227cce2b54",
				"Financial Times",
				"http://api.ft.com/brands/dbb0bdae-1f0c-1a1a-b0cb-b2227cce2b54",
				"",
			},
		},
		"transform organisation": {
			concept: db.Concept{
				ID:                "http://api.ft.com/things/eac853f5-3859-4c08-8540-55e043719400",
				PrefLabel:         "Fakebook",
				APIURL:            "http://api.ft.com/organisations/eac853f5-3859-4c08-8540-55e043719400",
				LeiCode:           "PBLD0EJDB5FWOLXP3B76",
				FactsetIDs:        []string{"FACTSET1", "FACTSET2"},
				FigiCodes:         []string{"BB8000C3P0-R2D2"},
				AlternativeLabels: []string{"Fakebook Company"},
				NAICSIndustryClassifications: []db.NAICSIndustryClassification{
					{IndustryIdentifier: "519130", Rank: 1},
					{IndustryIdentifier: "519131", Rank: 2},
				},
			},
			conceptType: "Organisation",
			expected: []string{
				"http://api.ft.com/things/eac853f5-3859-4c08-8540-55e043719400",
				"Fakebook",
				"http://api.ft.com/organisations/eac853f5-3859-4c08-8540-55e043719400",
				"Fakebook Company",
				"PBLD0EJDB5FWOLXP3B76",
				"FACTSET1;FACTSET2",
				"BB8000C3P0-R2D2",
				"519130;519131",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result := conceptToCSVRecord(test.concept, test.conceptType)
			assert.Equal(t, test.expected, result)
		})
	}
}
