#!/usr/bin/env bash

echo "Deleting all resources..."
kubectl delete all --all

echo "Applying all resources..."
kubectl apply -f ./k8s