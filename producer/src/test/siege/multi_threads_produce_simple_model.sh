#!/bin/sh

reps=${1:-10}
concurrent=${2:-10}
host=${3:-10.108.14.62:8080}
tmp="/tmp/$$"
mkdir -p $tmp

#i=1
#while [ $i -le $reps ]
#do
#	id=`date +%N`
#	desc=`cat /proc/sys/kernel/random/uuid`
#	#body="{\"id\":\"$id\",\"desc\": \"$desc\"}"
#	#echo $body > $tmp/$id
#	echo "http://$host/producer/simpleModel POST id=$id&desc=$desc" >> $tmp/urls.txt
#	i=$(($i+1))
#done

i=1
while [ $i -le $reps ]
do
	id=`date +%N`
	desc=`cat /proc/sys/kernel/random/uuid`
	body="{\"id\":\"$id\",\"desc\": \"$desc\"}"
	echo $body > $tmp/$id
	echo "http://$host/producer/simpleModel POST {\"id\":\"$id\",\"desc\": \"$desc\"}" >> $tmp/urls.txt
	i=$(($i+1))
done

num=`echo $reps/$concurrent| bc`

siege -H 'content-type:application/json' -c$concurrent -r$num -f $tmp/urls.txt

rm -rf $tmp
