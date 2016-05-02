#!/bin/bash

if [ -f "data/globus_oai.db" ]; then
	mv data/globus_oai.db data/old_oai.db

	python globus_harvester.py $1 --onlyharvest
	sqldiff globus_oai.db old_oai.db --primarykey > data/changes.sql
	sqlite3 changes.db "CREATE TABLE IF NOT EXISTS records (title TEXT, date TEXT, local_identifier TEXT, repository_url TEXT, PRIMARY KEY (local_identifier, repository_url))  WITHOUT ROWID;"
	sqlite3 changes.db < changes.sql

	python globus_harvester.py $1 --onlyexport

else
	python globus_harvester.py $1
fi