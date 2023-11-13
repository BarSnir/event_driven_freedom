# Description
Project that demonstrate how to migrate single source of truth to event-driven project.


# Project Architecture
https://drive.google.com/file/d/1xcJzDR0OytQTqq8ymZlrWXqOCxrBSMuW/view?usp=sharing

# TODO:

## Stream processing for full ads:
- Elasticsearch (search engine cercaria)
- Neo4j (recommendation system with impression)
- Salesforce customers & ad integration.
- Amazon S3 bucket for data platform.

## Connectors:
- Elasticsearch.
- Neo4j.
- Salesforce.
- Amazon S3 bucket for data platform.
- DruidDB for real time analytics.

## Infrastructure:
- Kafka connect cluster.
- Installing contextual connectors.
- Elasticsearch image.
- Neo4j Image.
- AWS Account.
- Salesforce account.
- Prometheus + Grafana image.
- JMX on Flink + Kafka Topics to track event flow.

## Analytics:
- Random impression producer.

## Frontend + Backend:
- Need to plan legacy stack and transformation to the new stack.