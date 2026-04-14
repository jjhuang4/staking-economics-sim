FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    git \
    python3-venv \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/cadlabs-entrypoint.sh
RUN chmod +x /usr/local/bin/cadlabs-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/cadlabs-entrypoint.sh"]
CMD ["bash"]
