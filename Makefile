.PHONY: gold verify-gold

gold:
	docker compose build dbt
	docker compose run --rm dbt

verify-gold:
	bash scripts/check_gold.sh
