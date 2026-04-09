# Elastic Natural Disaster Response

An agentic disaster coordination demo built on Elastic. A simulated Category 4 hurricane triggers automatic detection, geospatial enrichment, AI-driven personnel relocation planning, and email notifications — all without human intervention.

> **Disclaimer**: This is an entirely fictional scenario built for demonstration purposes. Installation locations are based on real publicly available geographic data (the DoD MIRTA dataset), but all operational data — personnel counts, housing capacity, assets, contact emails, and mission profiles — is completely fabricated.

## Steps

1. Deploy Elasticstack

  ```bash
  make docker
  ```

2. Log into [Kibana](http://localhost:5601) Username: elastic, Password: password

3. Setup your own [AI connector](https://www.elastic.co/docs/reference/kibana/connectors-kibana/ai-connector) or use Elastic Inference Service via [Cloud Connect](https://www.elastic.co/docs/explore-analyze/elastic-inference/connect-self-managed-cluster-to-eis#set-up-eis-with-cloud-connect)

4. Deploy the scenario

  ```bash
  make run
  ```

5. Go back into Kibana review the output under Agents -> Conversations

6. Once done destroy the environment

  ```bash
  make clean
  ```
