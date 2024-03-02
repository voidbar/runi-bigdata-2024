#!/usr/bin/env bash

echo "Building Cassandra Custom Image..."
cd database
docker build -t cassandra-custom:latest .
cd ..

echo "Building DB Init Image..."
cd db-init
docker build -t db-init:latest .
cd ..

echo "Building CSV Worker Image..."
cd csv-worker
docker build -t csv-worker:latest .
cd ..

echo "Building Aggregator Image..."
cd aggregator
docker build -t aggregator:latest .
cd ..

echo "All images built successfully."
