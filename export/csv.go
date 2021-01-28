package export

import (
	"bytes"
	"encoding/csv"
	"strings"

	"github.com/Financial-Times/concept-exporter/db"
)

type CsvExporter struct {
	Writer map[string]*ConceptWriter
}

type ConceptWriter struct {
	Buffer *bytes.Buffer
	Writer *csv.Writer
}

func NewCsvExporter() *CsvExporter {
	return &CsvExporter{}
}

func (e *CsvExporter) GetBytes(conceptType string) []byte {
	e.Writer[conceptType].Writer.Flush()
	return e.Writer[conceptType].Buffer.Bytes()
}

func (e *CsvExporter) Prepare(conceptTypes []string) error {
	writer := make(map[string]*ConceptWriter, len(conceptTypes))
	for _, cType := range conceptTypes {
		buffer := new(bytes.Buffer)
		writer[cType] = &ConceptWriter{Buffer: buffer, Writer: csv.NewWriter(buffer)}
		err := writer[cType].Writer.Write(getHeader(cType))
		if err != nil {
			return err
		}
	}
	e.Writer = writer
	return nil
}

func (e *CsvExporter) Write(c db.Concept, conceptType, tid string) error {
	rec := conceptToCSVRecord(c, conceptType)
	return e.Writer[conceptType].Writer.Write(rec)
}

func (e *CsvExporter) GetFileName(conceptType string) string {
	return conceptType + ".csv"
}

func getHeader(conceptType string) []string {
	if conceptType == "Organisation" {
		return []string{"id", "prefLabel", "apiUrl", "leiCode", "factsetId", "FIGI", "NAICS"}
	}
	return []string{"id", "prefLabel", "apiUrl"}
}

func conceptToCSVRecord(c db.Concept, conceptType string) []string {
	var rec []string
	rec = append(rec, c.ID)
	rec = append(rec, c.PrefLabel)
	rec = append(rec, c.APIURL)
	if conceptType == "Organisation" {
		rec = append(rec, c.LeiCode)
		rec = append(rec, c.FactsetID)
		rec = append(rec, c.FIGI)

		var naics []string
		for _, ic := range c.NAICSIndustryClassifications {
			naics = append(naics, ic.IndustryIdentifier)
		}

		rec = append(rec, strings.Join(naics, ";"))
	}

	return rec
}
