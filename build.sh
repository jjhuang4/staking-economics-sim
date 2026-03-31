# Build
docker build -t staking-sim .

# Runs and exits 0
docker run --rm staking-sim

# Plots land on the host
docker run --rm -v $(pwd)/output:/app/output staking-sim
ls output/

# Correct user
docker run --rm --entrypoint whoami staking-sim
# simuser