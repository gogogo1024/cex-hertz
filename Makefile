# 设置 RocksDB 头文件和库路径环境变量
export CGO_CFLAGS=-I/usr/local/include
export CGO_LDFLAGS=-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd


DOCKER_COMPOSE_FILE=docker-compose-base.yaml
.PHONY: gobuild
gobuild:
	CGO_ENABLED=1 go build -o bin/cex-hertz main.go

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

.PHONY: benchmark-tsung
benchmark-tsung:
	tsung -f benchmark/tsung.xml start  -env ERL_MAX_PORTS 300000  -env ERL_MAX_PROCESSES 300000

PHONY: benchmark-tsung-report
benchmark-tsung-report:
	@logdir=`find ~/.tsung/log -type f -name tsung.log -exec dirname {} \; | sort | tail -1`; \
	if [ -z "$$logdir" ]; then echo "No tsung.log found!"; exit 1; fi; \
	/usr/local/Cellar/tsung/1.8.0/lib/tsung/bin/tsung_stats.pl $$logdir; \
	echo "Report generated. Open the HTML report in the latest log directory."
