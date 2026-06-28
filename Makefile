# Atalhos. No Windows use os comandos `docker compose ...` do README se não tiver make.
.PHONY: help up down logs seed demo query psql nessie-refs clean

help:
	@echo "up         - sobe toda a stack (MinIO, Nessie, Trino, Dagster)"
	@echo "seed       - materializa os assets (popula main: sales, marketing, analytics)"
	@echo "demo       - roda a demo data-as-code (branch -> validar -> merge/rollback)"
	@echo "query      - abre a CLI do Trino"
	@echo "nessie-refs- lista os branches/tags do Nessie"
	@echo "down       - derruba a stack"
	@echo "clean      - derruba a stack e apaga volumes"

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f dagster

seed:
	docker compose exec dagster dagster asset materialize --select '*' -m data_mesh

demo:
	docker compose exec dagster python -m data_mesh.demo

query:
	docker compose exec -it trino trino

nessie-refs:
	curl -s http://localhost:19120/api/v2/trees | jq

clean:
	docker compose down -v
