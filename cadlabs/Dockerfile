# FROM python:3.11-slim

# ENV PYTHONDONTWRITEBYTECODE=1 \
#     PYTHONUNBUFFERED=1 \
#     PIP_ROOT_USER_ACTION=ignore

# WORKDIR /cadlabs

# # Install system dependencies for building C extensions
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     gcc \
#     python3-dev \
#     && rm -rf /var/lib/apt/lists/*

# COPY requirements.txt .

# RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
#     if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; else echo "requirements.txt not found"; fi

# COPY . .

# CMD ["python"]


# DEPRECATED, uncomment if trying to copy ethereum economic model into local kernel
FROM python:3.11-slim

# 1. Environment variables to keep Python/Pip portable and quiet
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONPATH="/workspace/cadlabs-model"

# 2. Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential && \
    rm -rf /var/lib/apt/lists/*

# 3. Setup workspace and clone repo
WORKDIR /workspace
RUN git clone --depth 1 https://github.com/CADLabs/ethereum-economic-model.git /workspace/cadlabs-model

WORKDIR /workspace/cadlabs-model

# 4. Install Python dependencies
# We use the full path to python -m pip to ensure the right binary is used
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    if [ -f requirements.txt ]; then python -m pip install --no-cache-dir -r requirements.txt; fi && \
    python -m pip install --no-cache-dir jupyter notebook

# 5. Create directory for shared data
RUN mkdir -p /workspace/shared

# 6. Set the entrypoint to Python directly (Avoids the "cannot execute binary" shell trap)
# This makes the container start Jupyter by default, but allows it to be portable.
ENTRYPOINT ["python", "-m", "notebook"]

# 7. Default arguments for Jupyter (can be overridden by docker-compose)
CMD ["--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]