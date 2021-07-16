#!/bin/bash

echo 'starting connector'
curl -sX PUT http://localhost:8083/connectors/kafka-connect-prod-00/config -d @kafka-connect-prod-config.json --header "Content-Type: application/json"
