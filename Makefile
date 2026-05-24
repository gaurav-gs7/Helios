GO ?= go
PYTHON ?= python3
DOCKER_COMPOSE ?= docker compose -f deploy/compose.yaml

DEPLOY_TARGET ?= docker-compose
DEPLOY_OVERLAY ?= dev
BENCHMARK_COUNTS ?= 100 500 1000

.PHONY: fmt lint test build-cli infra-up infra-down run-control-plane run-worker planner logs benchmark benchmark-quick pre-deploy-check post-deploy-check smoke deploy compose-deploy k8s-deploy-dev argocd-bootstrap-dev

fmt:
	$(GO)fmt ./...

lint:
	golangci-lint run
	ruff check .

test:
	$(GO) test ./...

build-cli:
	mkdir -p bin
	$(GO) build -o bin/heliosctl ./cmd/heliosctl

infra-up:
	$(DOCKER_COMPOSE) up -d

infra-down:
	$(DOCKER_COMPOSE) down -v

run-control-plane:
	$(GO) run ./cmd/control-plane

run-worker:
	$(GO) run ./cmd/worker

planner:
	cd planner && $(PYTHON) -m uvicorn main:app --host 0.0.0.0 --port 8090

logs:
	bash scripts/logs/tail_app_logs.sh

benchmark:
	BENCHMARK_COUNTS="$(BENCHMARK_COUNTS)" bash scripts/benchmark/run_benchmark.sh

benchmark-quick:
	COUNT=25 bash scripts/benchmark/run_benchmark.sh

pre-deploy-check:
	bash scripts/deploy/pre_deploy_check.sh

post-deploy-check:
	bash scripts/deploy/post_deploy_check.sh

smoke:
	bash scripts/deploy/smoke_test.sh

deploy:
	HELIOS_DEPLOY_TARGET=$(DEPLOY_TARGET) HELIOS_DEPLOY_OVERLAY=$(DEPLOY_OVERLAY) bash scripts/deploy/deploy.sh

compose-deploy:
	HELIOS_DEPLOY_TARGET=docker-compose HELIOS_DEPLOY_OVERLAY=dev bash scripts/deploy/deploy.sh

k8s-deploy-dev:
	HELIOS_DEPLOY_TARGET=kubernetes HELIOS_DEPLOY_OVERLAY=dev bash scripts/deploy/deploy.sh

argocd-bootstrap-dev:
	HELIOS_DEPLOY_TARGET=argocd HELIOS_DEPLOY_OVERLAY=dev bash scripts/deploy/deploy.sh
