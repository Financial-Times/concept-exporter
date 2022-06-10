package db

import (
	"errors"
	"fmt"
	"sort"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
)

//Service reads from a data source and uses a channel to iterate on the retrieved values for the given concept type
type Service interface {
	Read(conceptType string, conceptCh chan Concept) (int, bool, error)
}

//NeoService is the implementation of Service for Neo4j
type NeoService struct {
	Driver *cmneo4j.Driver
	NeoURL string
}

//Returns a new NeoService
func NewNeoService(driver *cmneo4j.Driver, neoURL string) *NeoService {
	return &NeoService{Driver: driver, NeoURL: neoURL}
}

//Concept is the model for the data read from the data source
type Concept struct {
	ID                           string
	UUID                         string
	PrefLabel                    string
	APIURL                       string
	Labels                       []string
	LeiCode                      string
	FactsetIDs                   []string
	FigiCodes                    []string
	NAICSIndustryClassifications []NAICSIndustryClassification
	AlternativeLabels            []string
	// AlternativeLabels contains the values of:
	Aliases     []string
	FormerNames []string
	ProperName  string
	ShortName   string
	TradeNames  []string
}

type NAICSIndustryClassification struct {
	IndustryIdentifier string `json:"id,omitempty"`
	Rank               int    `json:"rank,omitempty"`
}

func (s *NeoService) Read(conceptType string, conceptCh chan Concept) (int, bool, error) {
	results := []Concept{}
	stmt := fmt.Sprintf(`
		MATCH (x:%s)<-[:EQUIVALENT_TO]-(:Concept)<-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR|HAS_BRAND]-(:Content)
		USING SCAN x:%[1]s
		RETURN DISTINCT x.prefUUID AS Uuid, x.prefLabel AS PrefLabel, labels(x) AS Labels,
			x.aliases as Aliases, x.formerNames as FormerNames,	x.properName as ProperName, x.shortName as ShortName
		`, conceptType)

	if conceptType == "Organisation" {
		stmt = `
		MATCH (:Content)-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]->()-[:EQUIVALENT_TO]->(x:Organisation)
		USING SCAN x:Organisation
		WITH DISTINCT x
		MATCH (x)<-[:EQUIVALENT_TO]-(concept)
		OPTIONAL MATCH (concept)<-[:ISSUED_BY]-(fi:FinancialInstrument)
		OPTIONAL MATCH (concept)-[hasICRel:HAS_INDUSTRY_CLASSIFICATION]->(:NAICSIndustryClassification)-[:EQUIVALENT_TO]->(naicsCanonical:NAICSIndustryClassification)
		WITH x, collect(DISTINCT CASE concept.authority WHEN 'FACTSET' THEN concept.authorityValue END) AS factsetIds,
			collect(DISTINCT fi.figiCode) as figiCodes, collect(DISTINCT {id: naicsCanonical.industryIdentifier, rank: hasICRel.rank}) as naicsIndustryClassifications 
		RETURN x.prefUUID AS Uuid, labels(x) AS Labels, x.prefLabel AS PrefLabel, x.leiCode AS leiCode,
			x.aliases as Aliases, x.formerNames as FormerNames,	x.properName as ProperName, x.shortName as ShortName,
			factsetIds,
			figiCodes,
			naicsIndustryClassifications
		`
	}
	if conceptType == "Person" {
		stmt = `
		MATCH (:Content)-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]->(:Concept)-[:EQUIVALENT_TO]->(x:Person)
		USING SCAN x:Person
		RETURN DISTINCT x.prefUUID as Uuid, x.prefLabel as PrefLabel, labels(x) as Labels,
			x.aliases as Aliases, x.formerNames as FormerNames,	x.properName as ProperName, x.shortName as ShortName
		`
	}

	query := &cmneo4j.Query{
		Cypher: stmt,
		Result: &results,
	}

	err := s.Driver.Read(query)

	if errors.Is(err, cmneo4j.ErrNoResultsFound) {
		close(conceptCh)
		return 0, false, nil
	}
	if err != nil {
		close(conceptCh)
		return 0, false, err
	}
	go func() {
		defer close(conceptCh)
		for _, c := range results {
			c.APIURL = mapper.APIURL(c.UUID, c.Labels, "")
			c.ID = mapper.IDURL(c.UUID)
			c.NAICSIndustryClassifications = cleanNAICS(c.NAICSIndustryClassifications)
			c.AlternativeLabels = ConsolidateAlternativeLabels(c.Aliases, c.FormerNames, c.ProperName, c.ShortName, c.TradeNames)
			conceptCh <- c
		}
	}()
	return len(results), true, nil
}

func ConsolidateAlternativeLabels(aliases []string, formerNames []string, properName, shortName string, tradeNames []string) []string {
	var res []string

	if len(aliases) > 0 {
		res = append(res, aliases...)
	}
	if len(formerNames) > 0 {
		res = append(res, formerNames...)
	}
	if len(properName) > 0 {
		res = append(res, properName)
	}
	if len(shortName) > 0 {
		res = append(res, shortName)
	}
	if len(tradeNames) > 0 {
		res = append(res, tradeNames...)
	}

	return res
}

func (s *NeoService) CheckConnectivity() (string, error) {
	err := s.Driver.VerifyConnectivity()
	if err != nil {
		return "Could not connect to Neo", err
	}
	return "Neo could be reached", nil
}

func cleanNAICS(naics []NAICSIndustryClassification) []NAICSIndustryClassification {
	var res []NAICSIndustryClassification
	for _, ic := range naics {
		if ic.IndustryIdentifier != "" {
			res = append(res, ic)
		}
	}

	sort.SliceStable(res, func(k, l int) bool {
		return res[k].Rank < res[l].Rank
	})

	return res
}
