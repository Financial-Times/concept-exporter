# UPP Concept Exporter

Exports concepts by configured concept types from Neo4j and sends them to S3.

## Code

concept-exporter

## Primary URL

<https://upp-prod-delivery-glb.upp.ft.com/__concept-exporter/>

## Service Tier

Bronze

## Lifecycle Stage

Production

## Delivered By

content

## Supported By

content

## Known About By

- elitsa.pavlova
- kalin.arsov
- miroslav.gatsanoga
- ivan.nikolov
- marina.chompalova

## Host Platform

AWS

## Architecture

The service is used for automated concept exports. The concepts are taken from Neo4j, they are bundled into csv files and sent to S3 via UPP Exports RW S3 service.

## Contains Personal Data

No

## Contains Sensitive Data

No

## Dependencies

- upp-neo4j-cluster
- upp-exports-rw-s3

## Failover Architecture Type

ActiveActive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The service is deployed in both Delivery clusters. The failover guide for the cluster is located here:
<https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/delivery-cluster>

## Data Recovery Process Type

NotApplicable

## Data Recovery Details

The service does not store data, so it does not require any data recovery steps.

## Release Process Type

PartiallyAutomated

## Rollback Process Type

Manual

## Release Details

The release is triggered by making a Github release which is then picked up by a Jenkins multibranch pipeline. The Jenkins pipeline should be manually started in order for it to deploy the helm package to the Kubernetes clusters.

## Key Management Process Type

Manual

## Key Management Details

To access the service clients need to provide basic auth credentials.
To rotate credentials you need to login to a particular cluster and update varnish-auth secrets.

## Monitoring

Service in UPP K8S delivery clusters:

- Delivery-Prod-EU health: <https://upp-prod-delivery-eu.upp.ft.com/__health/__pods-health?service-name=concept-exporter>
- Delivery-Prod-US health: <https://upp-prod-delivery-us.upp.ft.com/__health/__pods-health?service-name=concept-exporter>

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.
