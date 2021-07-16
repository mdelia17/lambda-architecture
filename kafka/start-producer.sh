#!/bin/bash

for i in $(seq 0 99);
do
	timestamp=$(date +%d-%m-%Y_%H-%M-%S)
	docker cp dataset/data$i.csv kafka-connect-prod://tmp/kafka-connect-prod/network-data/$timestamp.csv
	echo 'copied data'$i'.csv to '$timestamp'.csv'
	sleep 10
done
