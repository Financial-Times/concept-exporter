package db

import (
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"fmt"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
)

type Service interface {
	Read(conceptType string, conceptCh chan Concept) (int, bool, error)
}

type NeoService struct {
	NeoURL     string
	Connection neoutils.NeoConnection
}

func NewNeoService(conn neoutils.NeoConnection, neoURL string) *NeoService {
	return &NeoService{Connection: conn, NeoURL: neoURL}
}

type Concept struct {
	Id        string
	Uuid      string
	PrefLabel string
	ApiUrl    string
	Labels    []string
	LeiCode string
	FactsetId string
	FIGI string
}

func (s *NeoService) Read(conceptType string, conceptCh chan Concept) (int, bool, error) {
	results := []Concept{}
	stmt := `
			MATCH (c:%s)-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]-(cc:Content)
			MATCH (c)-[:EQUIVALENT_TO]->(x:Thing)
			RETURN DISTINCT x.prefUUID AS Uuid, x.prefLabel AS PrefLabel, labels(c) AS Labels
				`
	if conceptType == "Organisation" {
		stmt = `
			MATCH (c:%s)-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]-(cc:Content)
			OPTIONAL MATCH (factset:FactsetIdentifier)-[:IDENTIFIES]->(c)
	    	OPTIONAL MATCH (lei:LegalEntityIdentifier)-[:IDENTIFIES]->(c)
	    	OPTIONAL MATCH (c)<-[:ISSUED_BY]-(fi:FinancialInstrument)<-[:IDENTIFIES]-(figi:FIGIIdentifier)
			RETURN DISTINCT c.uuid AS Uuid, c.prefLabel AS PrefLabel, labels(c) AS Labels, factset.value as factsetId, lei.value as leiCode, figi.value as FIGI
				`
	}
	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(stmt, conceptType),
		Result:       &results,
	}

	err := s.Connection.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		close(conceptCh)
		return 0, false, err
	}
	if len(results) == 0 {
		close(conceptCh)
		return 0, false, nil
	}
	go func() {
		defer close(conceptCh)
		for _, c := range results {
			c.ApiUrl = mapper.APIURL(c.Uuid, c.Labels, "")
			c.Id = mapper.IDURL(c.Uuid)
			conceptCh <- c
		}
	}()
	return len(results), true, nil
}

func (s *NeoService) CheckConnectivity() (string, error) {
	err := neoutils.Check(s.Connection)
	if err != nil {
		return "Could not connect to Neo", err
	}
	return "Neo could be reached", nil
}
