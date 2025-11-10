#!/usr/bin/make

.SILENT: clean
.PHONY: all
.DEFAULT_GOAL := help
.DEFAULT:
	@: # Do nothing for unknown targets

##@ Development resources

setup: ## Setup the project
	@make check-docker
	docker compose down --remove-orphans
	docker-compose build
	docker-compose up -d --force-recreate
	@echo "\033[1;32mSetup completed successfully.\033[0m"
	@echo "\033[1;34mRun benchmark  =======> make benchmark\033[0m"
	@echo "\033[1;34mAccess Redpanda =====> http://localhost:8660\033[0m"

benchmark: ## Run the benchmark
	docker-compose exec -it go-app bash -c "cd /app/cmd/benchmark && go run main.go"

container: ## Access the application container
	docker-compose exec -it go-app bash

check-docker: ## Check if Docker is installed
	@docker --version > /dev/null 2>&1 || (echo "Docker is not installed. Please install Docker and try again." && exit 1)

help: ## Show this help message
	@echo "Usage: make [command]"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
