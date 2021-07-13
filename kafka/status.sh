#!/bin/bash

echo 'plugin healthcheck'
curl -sX GET http://localhost:8083/connector-plugins | grep FilePulseSourceConnector
echo 'FilePulse healthcheck'
curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-csv
