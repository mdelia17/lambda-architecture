@ECHO OFF
powershell -Command "Invoke-RestMethod -Method GET -Uri http://localhost:9067/connector-plugins"