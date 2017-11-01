package web

import (
	"encoding/json"
	"fmt"

	"github.com/Financial-Times/concept-exporter/export"
	"github.com/Financial-Times/transactionid-utils-go"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
)

type RequestHandler struct {
	Exporter *export.Service
	ConceptTypes []string
}

func NewRequestHandler(fullExporter *export.Service, conceptTypes []string) *RequestHandler {
	return &RequestHandler{
		Exporter:     fullExporter,
		ConceptTypes: conceptTypes,
	}
}

func (handler *RequestHandler) GetJob(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	writer.Header().Add("Content-Type", "application/json")

	job := handler.Exporter.GetCurrentJob()

	err := json.NewEncoder(writer).Encode(job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer: "%v"`, job.ID, err)
		log.Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func (handler *RequestHandler) Export(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()

	tid := transactionidutils.GetTransactionIDFromRequest(request)

	if handler.Exporter.IsRunningJob() {
		http.Error(writer, "There are already running export jobs. Please wait them to finish", http.StatusBadRequest)
		return
	}
	candidates, errMsg := handler.getCandidateConceptTypes(request, tid)
	if len(candidates) == 0 {
		http.Error(writer, "No valid candidate concept types in the request", http.StatusBadRequest)
		return
	}
	job := handler.Exporter.CreateJob(candidates, errMsg)
	go handler.Exporter.RunFullExport(tid)
	writer.WriteHeader(http.StatusAccepted)
	writer.Header().Add("Content-Type", "application/json")

	err := json.NewEncoder(writer).Encode(job)
	if err != nil {
		msg := fmt.Sprintf(`Failed to write job %v to response writer: "%v"`, job.ID, err)
		log.Warn(msg)
		fmt.Fprintf(writer, "{\"ID\": \"%v\"}", job.ID)
		return
	}
}

func (handler *RequestHandler) getCandidateConceptTypes(request *http.Request, tid string) (candidates []string, errMsg string) {
	candidates = extractCandidateConceptTypesFromRequest(request)
	if candidates != nil && len(candidates) != 0 {
		var unsupported []string
		for i, cand := range candidates {
			found := false
			for _, cType := range handler.ConceptTypes {
				if cand == cType {
					found = true
					break
				}
			}
			if !found {
				unsupported = append(unsupported, cand)
				candidates = append(candidates[:i], candidates[i+1:]...)
			}
		}
		if len(unsupported) != 0 {
			errMsg = fmt.Sprintf("There are unsupported concept types within the candidates: %v", unsupported)
		}
		if len(candidates) == 0 {
			return
		}
	}
	if candidates == nil || len(candidates) == 0 {
		log.WithField("transaction_id", tid).Infof("Content type candidates are empty. Using all supported ones: %v", handler.ConceptTypes)
		candidates = handler.ConceptTypes
	}
	return
}

func extractCandidateConceptTypesFromRequest(request *http.Request) (candidates []string) {
	var result map[string]interface{}
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		log.Debugf("No valid POST body found, thus no candidate concept types to export. Parsing error: %v", err)
		return
	}

	if err = json.Unmarshal(body, &result); err != nil {
		log.Debugf("No valid json body found, thus no candidate concept types to export. Parsing error: %v", err)
		return
	}
	log.Infof("DEBUG Parsing request body: %v", result)
	cTypes, ok := result["conceptTypes"]
	if !ok {
		log.Infof("No conceptTypes field found in json body, thus no candidate concept types to export.")
		return
	}
	cTypesString, ok := cTypes.(string)
	if ok {
		candidates = strings.Split(cTypesString, " ")
	} else {
		log.Infof("The conceptTypes field found in json body is not a string as expected.")
	}

	return
}
