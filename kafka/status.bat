@ECHO OFF
powershell -Command "Invoke-RestMethod -Method GET -Uri http://localhost:8083/connector-plugins"
powershell -Command "Invoke-RestMethod -Method GET -Uri http://localhost:8083/connectors/connect-file-pulse-quickstart-csv"


