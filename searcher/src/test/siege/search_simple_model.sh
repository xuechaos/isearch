#!/bin/sh

concurrent=${1:-100} # concurrent user, default is 100 users
time=${2:-1} # executing time, default is 1 minutes
sid=${3:-558398850} #simple model id, default is 558398850
host=${4:-10.108.14.62:8080} #default is 10.108.14.62
siege -c$concurrent -t$time "http://$host/searcher/jsonq?id=$sid"
