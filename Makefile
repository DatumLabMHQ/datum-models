# Same commands everywhere. `make hourly` on a laptop is exactly what GitHub runs every hour.
ENV ?= $(HOME)/.config/datum/.env
IMAGE ?= datum-models

.PHONY: image check hourly nightly shell build test
image:      ; docker build -t $(IMAGE) .
check:      ; docker run --rm $(IMAGE) "bin/hourly.sh --check"
hourly:     ; docker run --rm --env-file $(ENV) $(IMAGE) "bin/hourly.sh"
nightly:    ; docker run --rm --env-file $(ENV) $(IMAGE) "bin/nightly.sh"
shell:      ; docker run --rm -it --env-file $(ENV) $(IMAGE) bash
build:      ; docker run --rm --env-file $(ENV) -e DBT_TARGET=dev $(IMAGE) 'eval "$$(python scripts/pgenv.py)" && dbt build --profiles-dir . --target dev'
test:       ; docker run --rm --env-file $(ENV) -e DBT_TARGET=dev $(IMAGE) 'eval "$$(python scripts/pgenv.py)" && dbt test --profiles-dir . --target dev'
