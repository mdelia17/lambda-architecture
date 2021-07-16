#!/bin/bash

echo 'connectors healthcheck'
curl -sX GET http://localhost:8083/connector-plugins | grep FilePulseSourceConnector
echo 'producer connector healthcheck'
curl -X GET http://localhost:8083/connectors/kafka-connect-prod-00
