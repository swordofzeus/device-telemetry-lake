ROOT_COMPOSE := -f ../docker-compose.yml -f docker-compose.yml

.PHONY: deps build seed-s3 init-lakehouse start stop logs query test clean

deps: build seed-s3 init-lakehouse ## Build images, seed S3, create tables + schemas. Run this before starting services.

build: ## Build latest Docker images for lakehouse-manager and device-ingestion
	docker compose $(ROOT_COMPOSE) build lakehouse-init device-ingestion

seed-s3: ## Upload reference data and contracts to S3 (requires garage running)
	docker compose $(ROOT_COMPOSE) run --rm \
		-v "$(CURDIR)/../reference-data:/data:ro" \
		s3-client s3 cp /data/inventory/v1/sites.csv s3://warehouse/dimension-tables/v1/sites.csv
	docker compose $(ROOT_COMPOSE) run --rm \
		-v "$(CURDIR)/../reference-data:/data:ro" \
		s3-client s3 cp /data/inventory/v1/devices.csv s3://warehouse/dimension-tables/v1/devices.csv
	docker compose $(ROOT_COMPOSE) run --rm \
		-v "$(CURDIR)/../reference-data:/data:ro" \
		s3-client s3 cp /data/inventory/v1/device_dependencies.csv s3://warehouse/dimension-tables/v1/device_dependencies.csv
	docker compose $(ROOT_COMPOSE) run --rm \
		-v "$(CURDIR)/../reference-data:/data:ro" \
		s3-client s3 cp /data/census/v1/census_block_groups.csv s3://warehouse/dimension-tables/v1/census_block_groups.csv
	docker compose $(ROOT_COMPOSE) run --rm \
		-v "$(CURDIR)/../contracts/events:/data:ro" \
		s3-client s3 cp --recursive /data/ s3://warehouse/contracts/events/v1/

init-lakehouse: ## Create Iceberg tables, load reference data, register Kafka schemas (requires nessie + schema-registry)
	docker compose $(ROOT_COMPOSE) run --rm lakehouse-init

start: ## Start the ingestion pipeline and solution services
	docker compose $(ROOT_COMPOSE) up -d

stop: ## Stop all containers without removing volumes (preserves data)
	docker compose $(ROOT_COMPOSE) stop

logs: ## Tail logs for all services (or pass SVC=<name> for one service)
	docker compose $(ROOT_COMPOSE) logs -f $(SVC)

query: ## Open a Trino CLI session (or pass SQL="<query>" to run one query)
ifdef SQL
	docker compose $(ROOT_COMPOSE) exec trino trino --execute "$(SQL)"
else
	docker compose $(ROOT_COMPOSE) exec trino trino
endif

test: ## Run unit tests for both packages
	cd ingestion/ready-lakehouse-manager && uv run pytest tests/ -v
	cd ingestion/device-ingestion && uv run pytest tests/ -v

clean: ## Stop all containers, delete all volumes, and remove solution images (full reset)
	docker compose $(ROOT_COMPOSE) down -v
	docker rmi ready-lakehouse-manager:latest device-ingestion:latest 2>/dev/null || true

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
