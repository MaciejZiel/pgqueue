# Benchmark helpers. Needs Docker, uv and a Linux or macOS shell.
#
#   make bench          start Postgres and Valkey, run the full benchmark, draw the chart
#   make bench-quick    the same with tiny numbers, to check the setup works
#   make bench-chart    redraw docs/benchmarks.png from bench/results.json
#   make bench-down     stop the benchmark containers
#
# Override the ports if the defaults are taken, e.g. `make bench PG_PORT=55461`.
PG_PORT ?= 55432
REDIS_PORT ?= 56379
export PGQUEUE_BENCH_DSN ?= postgresql://pgqueue:pgqueue@localhost:$(PG_PORT)/pgqueue_bench
export BENCH_REDIS_URL ?= redis://localhost:$(REDIS_PORT)/0
export PGQUEUE_DSN ?= $(PGQUEUE_BENCH_DSN)
BENCH = uv run --group bench python -m bench.bench

.PHONY: bench bench-quick bench-up bench-down bench-chart

bench-up:
	PG_PORT=$(PG_PORT) REDIS_PORT=$(REDIS_PORT) docker compose --profile bench up -d --wait postgres valkey

bench-down:
	docker compose --profile bench stop postgres valkey

bench: bench-up
	$(BENCH) $(ARGS)
	$(MAKE) bench-chart

bench-quick: bench-up
	$(BENCH) --quick --out bench/results-quick.json $(ARGS)

bench-chart:
	uv run --group bench python -m bench.chart
