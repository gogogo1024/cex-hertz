DOCKER_COMPOSE_FILE=docker-compose-base.yaml

.PHONY: up down restart logs build

up:
	docker-compose -f $(DOCKER_COMPOSE_FILE) up -d

down:
	docker-compose -f $(DOCKER_COMPOSE_FILE) down

restart:
	make down
	make up

logs:
	docker-compose -f $(DOCKER_COMPOSE_FILE) logs -f --tail=200

build:
	docker-compose -f $(DOCKER_COMPOSE_FILE) build

  