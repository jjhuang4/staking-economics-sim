#!/bin/bash

# Build the Docker image from the current directory (cadlabs/)
echo "Building Docker image..."
docker build -t cadlabs-test .

# Run the container
echo "Running Docker container..."
docker run --rm cadlabs-test