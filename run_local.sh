#!/bin/bash

# Step 1 - build
# Puede usar --use-container
echo "Building app"
sam build

# Step 2 - Run
echo "Running"

sam local start-api --env-vars env.example.json

