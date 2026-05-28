.PHONY: help setup build up down logs shell init

help:
	@echo "Available targets:"
	@echo "  make setup   - create virtualenv and install Python dependencies"
	@echo "  make build   - build the Docker image"
	@echo "  make up      - build and start Docker Compose services"
	@echo "  make down    - stop Docker Compose services"
	@echo "  make logs    - follow Airflow webserver logs"
	@echo "  make shell   - open a shell in the airflow-webserver container"
	@echo "  make init    - initialize Airflow DB and create admin user"

setup:
	python -m venv .venv
	. .venv/bin/activate && python -m pip install --upgrade pip
	. .venv/bin/activate && pip install -r requirements.txt

build:
	docker compose build

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f airflow-webserver

shell:
	docker compose exec airflow-webserver bash

init:
	docker compose run --rm airflow-init
