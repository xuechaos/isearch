#!/bin/sh
reps=${1:-10}
concurrent=${2:-100} # concurrent user, default is 100 users
time=${3:-1} # executing time, default is 1 minutes
host=${4:-10.108.14.62:8080} #default is 10.108.14.62

tmp="/tmp/$$"
mkdir -p $tmp

i=1
while [ $i -le $reps ]
do
	id="date +%N"
	echo "http://$host/searcher/jsonq?id=$id" >> $tmp/urls.txt
	i=$(($i+1))
done

siege -c$concurrent -t$time "http://$host/searcher/jsonq?id=$sid"

rm -rf $tmp