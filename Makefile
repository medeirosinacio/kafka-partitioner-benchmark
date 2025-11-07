#!/usr/bin/make

.SILENT: clean
.PHONY: all
.DEFAULT_GOAL := help
.DEFAULT:
	@: # Do nothing for unknown targets

RUN = docker-compose run --rm -v $(PWD):/app -w /app --user $(id -u $USER):$(id -g $USER) --entrypoint
APP = $(RUN) bash go-app

##@ Development resources

setup: ## Setup the project
	@make check-docker
	docker compose down --remove-orphans
	docker-compose build
	docker-compose up -d --force-recreate
	@echo "\033[1;32mSetup concluído com sucesso.\033[0m"
	@echo "\033[1;34mRodar benchmark  =======> make benchmark\033[0m"
	@echo "\033[1;34mAcessar Redpanda =======> http://localhost:8660\033[0m"

container: ## Access the application container
	docker-compose exec -it  app bash

check-docker: ## Check if Docker is installed
	@docker --version > /dev/null 2>&1 || (echo "Docker is not installed. Please install Docker and try again." && exit 1)

help: ## Show this help message
	@echo "Usage: make [command]"
	@echo ""
	@echo "Commands available:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
