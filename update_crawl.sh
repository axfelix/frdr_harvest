#!/bin/bash

if [ -f "data/globus_oai.db" ]; then
	mv data/globus_oai.db data/old_oai.db
	python globus_harvester.py $1
	sqldiff --summary globus_oai.db old_oai.db > data/changes.sql
	# call another script here to make deletion requests to globus
else
	python globus_harvester.py $1
fi