package db

import (
	"fmt"

	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/v2/neoutils"
	"github.com/jmcvetta/neoism"
)

//Service reads from a data source and uses a channel to iterate on the retrieved values for the given concept type
type Service interface {
	Read(conceptType string, conceptCh chan Concept) (int, bool, error)
}

//NeoService is the implementation of Service for Neo4j
type NeoService struct {
	Connection neoutils.NeoConnection
	NeoURL     string
}

//Returns a new NeoService
func NewNeoService(conn neoutils.NeoConnection, neoURL string) *NeoService {
	return &NeoService{Connection: conn, NeoURL: neoURL}
}

//Concept is the model for the data read from the data source
type Concept struct {
	ID                      string
	UUID                    string
	PrefLabel               string
	APIURL                  string
	Labels                  []string
	LeiCode                 string
	FactsetID               string
	FIGI                    string
	IndustryClassifications string
}

func (s *NeoService) Read(conceptType string, conceptCh chan Concept) (int, bool, error) {
	results := []Concept{}
	stmt := fmt.Sprintf(`
		MATCH (x:%s)<-[:EQUIVALENT_TO]-(:Concept)<-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR|HAS_BRAND]-(:Content)
		USING SCAN x:%[1]s
		RETURN DISTINCT x.prefUUID AS Uuid, x.prefLabel AS PrefLabel, labels(x) AS Labels
		`, conceptType)

	if conceptType == "Organisation" {
		stmt = `
		MATCH (:Content)-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]->()-[:EQUIVALENT_TO]->(x:Organisation)
		USING SCAN x:Organisation
		WITH DISTINCT x
		MATCH (x)<-[:EQUIVALENT_TO]-(concept)
		OPTIONAL MATCH (concept)<-[:ISSUED_BY]-(fi:FinancialInstrument)
		WITH x, collect(DISTINCT CASE concept.authority WHEN 'FACTSET' THEN concept.authorityValue END) AS factsetIds,
			collect(DISTINCT fi.figiCode) as figiCodes 
		RETURN x.prefUUID AS Uuid, labels(x) AS Labels, x.prefLabel AS PrefLabel, x.leiCode AS leiCode,
			reduce(s=head(factsetIds), n IN tail(factsetIds) | s + ';' + n) AS factsetId,
			reduce(s=head(figiCodes), n IN tail(figiCodes) | s + ';' + n) AS FIGI
		`
	}
	if conceptType == "Person" {
		stmt = `
		MATCH (:Content)-[:MENTIONS|MAJOR_MENTIONS|ABOUT|IS_CLASSIFIED_BY|IS_PRIMARILY_CLASSIFIED_BY|HAS_AUTHOR]->(:Concept)-[:EQUIVALENT_TO]->(x:Person)
		USING SCAN x:Person
		RETURN DISTINCT x.prefUUID as Uuid, x.prefLabel as PrefLabel, labels(x) as Labels
		`
	}

	query := &neoism.CypherQuery{
		Statement: stmt,
		Result:    &results,
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
			c.APIURL = mapper.APIURL(c.UUID, c.Labels, "")
			c.ID = mapper.IDURL(c.UUID)
			conceptCh <- c
		}
	}()
	return len(results), true, nil
}

func (s *NeoService) CheckConnectivity(conn neoutils.NeoConnection) (string, error) {
	err := neoutils.Check(conn)
	if err != nil {
		return "Could not connect to Neo", err
	}
	return "Neo could be reached", nil
}
