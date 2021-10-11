package main

import (
	"fmt"
	"net"
	"net/http"
	"time"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/Financial-Times/concept-exporter/concept"
	"github.com/Financial-Times/concept-exporter/db"
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/service-status-go/gtg"
)

const healthPath = "/__health"

type healthService struct {
	config *healthConfig
	checks []health.Check
	client *http.Client
	driver *cmneo4j.Driver
}

type healthConfig struct {
	appSystemCode string
	appName       string
	port          string
	s3Uploader    *concept.S3Updater
	neoService    *db.NeoService
	log           *logger.UPPLogger
}

func newHealthService(config *healthConfig) *healthService {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 3,
		Dial: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: 3 * time.Second,
		IdleConnTimeout:       3 * time.Second,
	}
	svc := &healthService{config: config}
	svc.checks = []health.Check{
		svc.NeoCheck(),
		svc.S3WriterCheck(),
	}
	svc.client = &http.Client{
		Transport: tr,
		Timeout:   3 * time.Second,
	}

	return svc
}

func (service *healthService) NeoCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToNeo4j",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://runbooks.in.ft.com/concept-exporter",
		Severity:         2,
		TechnicalSummary: fmt.Sprintf("The service is unable to connect to Neo4j (%s). Export won't work because of this", service.config.neoService.NeoURL),
		Checker: func() (string, error) {
			return service.config.neoService.CheckConnectivity()
		},
	}
}

func (service *healthService) S3WriterCheck() health.Check {
	return health.Check{
		Name:             "CheckConnectivityToExportRWS3",
		BusinessImpact:   "No Business Impact.",
		PanicGuide:       "https://runbooks.in.ft.com/concept-exporter",
		Severity:         2,
		TechnicalSummary: "The service is unable to connect to Export-RW-S3. Export won't work because of this",
		Checker: func() (string, error) {
			return service.config.s3Uploader.CheckHealth(service.client)
		},
	}
}

func (service *healthService) GTG() gtg.Status {
	s3WriterCheck := func() gtg.Status {
		return service.gtgCheck(service.S3WriterCheck())
	}
	neoCheck := func() gtg.Status {
		return service.gtgCheck(service.NeoCheck())
	}

	return gtg.FailFastParallelCheck([]gtg.StatusChecker{
		s3WriterCheck,
		neoCheck,
	})()
}

func (service *healthService) gtgCheck(check health.Check) gtg.Status {
	if _, err := check.Checker(); err != nil {
		return gtg.Status{GoodToGo: false, Message: err.Error()}
	}
	return gtg.Status{GoodToGo: true}
}
