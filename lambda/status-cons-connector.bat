@ECHO OFF

echo connectors healthcheck
powershell -Command "Invoke-RestMethod -Method GET -Uri http://localhost:9067/connector-plugins"
echo consumer connector healthcheck
powershell -Command "Invoke-RestMethod -Method GET -Uri http://localhost:9067/connectors/kafka-connect-cons-00"