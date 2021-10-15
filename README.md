# concept-exporter

[![Circle CI](https://circleci.com/gh/Financial-Times/concept-exporter/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/concept-exporter/tree/master)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/concept-exporter)](https://goreportcard.com/report/github.com/Financial-Times/concept-exporter) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/concept-exporter/badge.svg)](https://coveralls.io/github/Financial-Times/concept-exporter)

## Introduction

The service is used for automated concept exports. The concepts are taken from Neo4j, they are bundled into csv files and sent to S3 via UPP Export S3 Writer.
There are 2 types of exports:
* A *FULL export* consists in inquiring all supported concepts from the DB
* A *TARGETED export* is similar to the FULL export but triggering only for specific concept types

## Running locally

1. Run the unit tests and install the binary:

        go get github.com/Financial-Times/concept-exporter
        cd $GOPATH/src/github.com/Financial-Times/concept-exporter
        go test -v -race ./...
        go install

2. Run unit and integration tests:

    In order to execute the integration tests you must provide GITHUB_USERNAME and GITHUB_TOKEN values, because the service is depending on internal repositories.

    ```
    GITHUB_USERNAME=<GITHUB_USERNAME> GITHUB_TOKEN=<TOKEN> \
    docker-compose -f docker-compose-tests.yml up -d --build && \
    docker logs -f test-runner && \
    docker-compose -f docker-compose-tests.yml down -v
    ```

3. Run the binary (using the `help` flag to see the available optional arguments):

        $GOPATH/bin/concept-exporter [--help]

Options:

        Usage: concept-exporter [OPTIONS]

        Exports concept from a data source (Neo4j) and sends it to S3

        Options:
          --app-system-code="concept-exporter"                                      System Code of the application ($APP_SYSTEM_CODE)
          --app-name="concept-exporter"                                             Application name ($APP_NAME)
          --port="8080"                                                             Port to listen on ($APP_PORT)
          --neo-url="bolt://localhost:7687"                                         Neo4j endpoint URL ($NEO_URL)
          --s3WriterBaseURL="http://localhost:8080"                                 Base URL to S3 writer endpoint ($S3_WRITER_BASE_URL)
          --s3WriterHealthURL="http://localhost:8080/__gtg"                         Health URL to S3 writer endpoint ($S3_WRITER_HEALTH_URL)
          --conceptTypes=["Brand", "Topic", "Location", "Person", "Organisation"]   Concept types to support ($CONCEPT_TYPES)
          --logLevel                                                                Logging level (DEBUG, INFO, WARN, ERROR) (env $LOG_LEVEL) (default "INFO")

4. Test:

         curl http://localhost:8080/__health

## Build and deployment

* Built by Docker Hub on merge to master: [coco/concept-exporter](https://hub.docker.com/r/coco/concept-exporter/)
* CI provided by CircleCI: [concept-exporter](https://circleci.com/gh/Financial-Times/concept-exporter)

## Service endpoints

### POST
* `/export` - Triggers an export. If `conceptTypes` is in the json body request, then a TARGETED export is triggered, otherwise a FULL export

e.g.
A FULL export:

    curl localhost:8080/__concept-exporter/export -XPOST
    {"ID":"job_753c6005-dcf0-4381-96b9-aeac0d0c01c8","Concepts":["Brand","Topic","Location","Person","Organisation"],"Status":"Starting"}

A TARGETED export:

    curl localhost:8080/__concept-exporter/export -XPOST -d '{"conceptTypes":"Brand Topic"}'
    {"ID":"job_d6706835-5f72-4585-ba97-c454ea62dba6","Concepts":["Brand","Topic"],"Status":"Starting"}

### GET
* `/job` - Returns the running job information

e.g.

    curl http://localhost:8080/job | jq ''`
    {
      "ConceptWorkers": [
        {
          "ConceptType": "Brand",
          "Count": 335,
          "Progress": 335,
          "Status": "Finished"
        },
        {
          "ConceptType": "Topic",
          "Count": 760,
          "Progress": 760,
          "Status": "Finished"
        },
        {
          "ConceptType": "Location",
          "Count": 13093,
          "Progress": 13093,
          "Status": "Finished"
        },
        {
          "ConceptType": "Person",
          "Count": 39668,
          "Progress": 39668,
          "Status": "Finished"
        },
        {
          "ConceptType": "Organisation",
          "Count": 80972,
          "Progress": 80972,
          "Status": "Finished"
        }
      ],
      "ID": "job_753c6005-dcf0-4381-96b9-aeac0d0c01c8",
      "Concepts": [
        "Brand",
        "Topic",
        "Location",
        "Person",
        "Organisation"
      ],
      "Progress": [
        "Brand",
        "Topic",
        "Location",
        "Person",
        "Organisation"
      ],
      "Status": "Finished"
    }

## Utility endpoints

## Healthchecks
Admin endpoints are:

`/__gtg`

`/__health`

`/__build-info`

There are several checks performed:

* Check that a connection can be made to Neo4j, using the Neo4j URL supplied as a parameter in service startup
* Check that the S3 Writer service is healthy

### Logging

* NOTE: `/__build-info` and `/__gtg` endpoints are not logged as they are called every second from varnish/vulcand and this information is not needed in logs/splunk.
