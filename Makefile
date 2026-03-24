.PHONY: build up down logs-web logs-sh shell-sh clean-all help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## забилдить образы (парсер и ml)
	docker build -t flats-parser:2.0 -f parsers/Dockerfile .
	docker build -t catboost_train:latest -f ml/Dockerfile .

up: ## запустить проект
	docker-compose up -d --build

down: ## остановить и удалить контейнеры
	docker-compose down

all: build up ## билд и запуск

## Airflow
logs-web: ## логи вебсервера
	docker-compose logs -f airflow-webserver

logs-sh: ## логи планировщика
	docker-compose logs -f airflow-scheduler

shell-sh: ## зайти в контейнер планировщика
	docker exec -it --user airflow airflow_scheduler bash

clean-all: ## ВОТ ТУТ ОСТОРОЖНО: удалить контейнеры, все волумы и данные
	@echo "Точно удалить все? (y/N)" && read ans && [ $${ans:-N} = y ]
	docker-compose down -v --remove-orphans
	rm -rf ./minio_data ./metabase_data ./logs ./plugins ./config
	@echo "Данные очищены"