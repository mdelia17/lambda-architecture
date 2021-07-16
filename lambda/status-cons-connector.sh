#!/bin/bash

echo 'connectors healthcheck'
curl -s localhost:9067/connector-plugins
echo 'consumer connector healthcheck'
curl -X GET http://localhost:9067/connectors/kafka-connect-cons-00