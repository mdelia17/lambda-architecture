#!/bin/bash

echo 'starting connector'
curl -sX PUT http://localhost:8083/connectors/connect-file-pulse-quickstart-csv/config -d @connect-file-pulse-quickstart-csv.json --header "Content-Type: application/json"
