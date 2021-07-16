@ECHO OFF

echo connectors healthcheck
powershell -Command "Invoke-RestMethod -Method GET -Uri http://localhost:8083/connector-plugins"
echo producer connector healthcheck
powershell -Command "Invoke-RestMethod -Method GET -Uri http://localhost:8083/connectors/kafka-connect-prod-00"


