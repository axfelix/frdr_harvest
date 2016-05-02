#!/bin/bash

if [ -f "data/globus_oai.db" ]; then
	mv data/globus_oai.db data/old_oai.db

	python globus_harvester.py $1 --onlyharvest
	sqldiff data/globus_oai.db data/old_oai.db --primarykey > data/changes.sql
	grep "INSERT INTO" data/changes.sql > data/deleted.sql

	sqlite3 data/deleted.db "CREATE TABLE IF NOT EXISTS records (title TEXT, date TEXT, local_identifier TEXT, repository_url TEXT, PRIMARY KEY (local_identifier, repository_url)) WITHOUT ROWID;"
	sqlite3 data/deleted.db < data/deleted.sql

	python globus_harvester.py $1 --onlyexport
	rm data/deleted.sql
	rm data/deleted.db
	rm data/changes.sql

else
	python globus_harvester.py $1
fi