@ECHO OFF
powershell -Command "Invoke-RestMethod -Method Put -Uri http://localhost:9067/connectors/sink-hdfs-network-data-00/config -ContentType "application/json" -Body $(get-content consumer-conf.json -raw)"