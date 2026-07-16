FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libxml2-dev libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt


FROM python:3.12-slim

LABEL org.opencontainers.image.source=https://github.com/kevinauberson/GreenExtenderK8s


# Run as a non-root user
RUN groupadd -r app && useradd -r -g app -u 1000 app

WORKDIR /app

COPY --from=builder /root/.local /home/app/.local

COPY --chown=app:app cache.py electricity_maps.py \
                     metrics_server.py vsphere.py main.py ./
COPY --chown=app:app node_mapping.yaml ./

ENV PATH=/home/app/.local/bin:$PATH \
    PYTHONPATH=/home/app/.local/lib/python3.12/site-packages \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER app

CMD ["python", "main.py"]