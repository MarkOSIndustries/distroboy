#!/bin/bash

# Get the reusable infrastructure we need going
docker-compose -p distroboy -f localdev/docker-compose.yml up -d

# Run the example job
exec docker-compose -p distroboy -f example/docker-compose.yml up --build
