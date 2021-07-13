# Run
docker-compose up -d

# test per vedere se il plugin è up
curl -sX GET http://localhost:8083/connector-plugins | grep FilePulseSourceConnector

# Creazione di una istanza di connettore 
curl -sX PUT http://localhost:8083/connectors/connect-file-pulse-quickstart-csv/config -d @connect-file-pulse-quickstart-csv.json --header "Content-Type: application/json"

# Upload del csv 
docker exec -it connect mkdir -p /tmp/kafka-connect/examples
docker cp dataset/data0.csv connect://tmp/kafka-connect/examples/data0.csv

# Vedere lo stato del connettore
curl -X GET http://localhost:8083/connectors/connect-file-pulse-quickstart-csv

# Check for task completion
docker logs --tail="all" -f connect | grep "Orphan task detected"

# entrare nella shell di broker (fare "docker ps" per vedere l'id associato al container chiamato broker)
docker exec -i -t -u root broker /bin/bash 

# dentro la shell di broker fare i seguenti comandi
cd ; cd .. ; cd bin ; kafka-console-consumer --from-beginning --bootstrap-server localhost:29092 --topic=connect-file-pulse-quickstart-csv