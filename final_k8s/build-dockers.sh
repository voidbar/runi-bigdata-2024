#!/usr/bin/env bash

echo "Building Cassandra Custom Image..."
cd database
docker build -q -t cassandra-custom:latest .
cd ..

echo "Building Orchestrator Image..."
cd orchestrator
docker build -q -t orchestrator:latest .
cd ..

echo "Building CSV Worker Image..."
cd csv-worker
docker build -q -t csv-worker:latest .
cd ..

echo "Building Aggregator Image..."
cd aggregator
docker build -q -t aggregator:latest .
cd ..

echo "All images built successfully."
