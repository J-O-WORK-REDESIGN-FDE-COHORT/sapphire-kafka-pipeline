#!/bin/bash

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
  if curl -s http://kafka-connect:8083/ > /dev/null 2>&1; then
    echo "Kafka Connect is ready!"
    break
  fi
  attempt=$((attempt + 1))
  echo "Attempt $attempt/$max_attempts: Kafka Connect not ready yet, waiting..."
  sleep 5
done

if [ $attempt -eq $max_attempts ]; then
  echo "ERROR: Kafka Connect did not become ready in time"
  exit 1
fi

# Check if connector already exists
echo "Checking if postgres-sink-connector already exists..."
status_code=$(curl -s -o /dev/null -w "%{http_code}" http://kafka-connect:8083/connectors/postgres-sink-connector)

if [ "$status_code" = "200" ]; then
  echo "Connector already exists. Deleting it first..."
  curl -s -X DELETE http://kafka-connect:8083/connectors/postgres-sink-connector
  sleep 2
  echo "Existing connector deleted."
elif [ "$status_code" = "404" ]; then
  echo "Connector does not exist. Proceeding with deployment..."
else
  echo "WARNING: Unexpected status code: $status_code"
fi

# Function to deploy a connector
deploy_connector() {
  local connector_name=$1
  local config_file=$2
  
  echo ""
  echo "=========================================="
  echo "Deploying $connector_name..."
  echo "=========================================="
  
  # Check if connector already exists
  echo "Checking if $connector_name already exists..."
  status_code=$(curl -s -o /dev/null -w "%{http_code}" http://kafka-connect:8083/connectors/$connector_name)
  
  if [ "$status_code" = "200" ]; then
    echo "Connector already exists. Deleting it first..."
    curl -s -X DELETE http://kafka-connect:8083/connectors/$connector_name
    sleep 2
    echo "Existing connector deleted."
  elif [ "$status_code" = "404" ]; then
    echo "Connector does not exist. Proceeding with deployment..."
  else
    echo "WARNING: Unexpected status code: $status_code"
  fi
  
  # Deploy the connector
  response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    --data @/config/$config_file \
    http://kafka-connect:8083/connectors)
  
  # Extract HTTP status code
  http_status=$(echo "$response" | grep "HTTP_STATUS:" | cut -d: -f2)
  response_body=$(echo "$response" | sed '/HTTP_STATUS:/d')
  
  if [ "$http_status" = "201" ] || [ "$http_status" = "200" ]; then
    echo "SUCCESS: $connector_name deployed successfully!"
    echo "Response:"
    echo "$response_body"
  else
    echo "ERROR: Failed to deploy $connector_name (HTTP $http_status)"
    echo "Response:"
    echo "$response_body"
    return 1
  fi
  
  # Check connector status
  echo ""
  echo "Checking $connector_name status..."
  sleep 3
  status_response=$(curl -s http://kafka-connect:8083/connectors/$connector_name/status)
  echo "$status_response"
  
  # Check if connector is running
  if echo "$status_response" | grep -q '"state":"RUNNING"'; then
    echo ""
    echo "✓ $connector_name is RUNNING successfully!"
  else
    echo ""
    echo "⚠ $connector_name may not be running. Check the status above."
  fi
}

# Deploy both connectors
deploy_connector "postgres-device-sink-connector" "postgres-device-sink-connector.json"
deploy_connector "postgres-sink-connector" "postgres-sink-connector.json"

echo ""
echo "=========================================="
echo "All connectors deployment complete!"
echo "=========================================="
echo "Device connector status: http://localhost:8083/connectors/postgres-device-sink-connector/status"
echo "Metrics connector status: http://localhost:8083/connectors/postgres-sink-connector/status"

# Made with Bob
