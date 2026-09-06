# The platform runner. One image runs every scheduled job identically on a laptop, a GitHub
# runner, or any container host: Python 3.12 for the loaders and dbt, Node 22 for the Sui adapters.
# Build:  docker build -t datum-models .
# Run:    docker run --rm --env-file ~/.config/datum/.env datum-models bin/hourly.sh
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 DBT_TARGET=prod
RUN apt-get update && apt-get install -y --no-install-recommends git curl ca-certificates \
 && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY . .
RUN dbt deps --profiles-dir . >/dev/null

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["bin/hourly.sh"]
