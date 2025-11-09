RELEASE_TAG ?= v0.1.0
COMPOSE := docker compose -f docker-compose.yml -f docker-compose.release.yml

.PHONY: pull-release up-release down-release logs set-tag health

pull-release:
	$(COMPOSE) pull ai dashboard

up-release:
	$(COMPOSE) up -d --force-recreate ai dashboard

down-release:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f ai dashboard

# Usage: make set-tag RELEASE_TAG=v0.2.0
set-tag:
	@grep -q '^RELEASE_TAG=' .env && sed -i '' "s/^RELEASE_TAG=.*/RELEASE_TAG=$(RELEASE_TAG)/" .env || echo "RELEASE_TAG=$(RELEASE_TAG)" >> .env
	$(MAKE) up-release

health:
	curl -s http://localhost:8000/healthz | jq .
