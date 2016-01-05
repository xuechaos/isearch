#!/bin/sh

#siege -c50 -t1 -d1 'http://10.5.16.51:8080/producer/simpleModel POST < simple_model.json'
#siege -c1 -r1 'http://10.5.16.51:8080/producer/simpleModel POST < simple_model.json'
duration=${1:-0.1} #sleep duration, default 0.1s
host=${2:-10.108.14.62:8080}
while true ;
do
	id=`date +%N`
	desc=`cat /proc/sys/kernel/random/uuid`
	body="{\"id\":\"$id\",\"desc\": \"${desc}\"}"
	curl -H "Content-Type:application/json" -d "$body" "http://$host/producer/simpleModel" -# |xargs >> /tmp/isearch/produces.response.json
	sleep $duration
done

