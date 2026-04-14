FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MPLBACKEND=Agg \
    PYTHONPATH=/app

RUN useradd --create-home --shell /bin/bash simuser

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY simulator/requirements.txt ./simulator-requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install -r simulator-requirements.txt

RUN mkdir -p /app/output /app/shared/output /app/shared/data && \
    chown -R simuser:simuser /app

COPY simulator ./simulator
COPY pool_tracker ./pool_tracker
COPY README.md pyproject.toml ./

USER simuser

EXPOSE 8501

CMD ["streamlit", "run", "simulator/live_dashboard.py", "--server.address=0.0.0.0", "--server.port=8501"]
