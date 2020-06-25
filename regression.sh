#!/bin/bash

# Clone the harvester and the (private) deployment, get repo config
# git clone https://github.com/axfelix/frdr_harvest.git
# git clone https://github.com/frdr-dfdr/frdr_deploy.git
cp frdr_deploy/roles/harvest_hosts/templates/repos.json frdr_harvest/conf/repos.json

# Remove limits on records updated per run
sed -Ei 's/2000/1000000/g' frdr_harvest/conf/harvester.conf
sed -Ei 's/5000/1000000/g' frdr_harvest/conf/repos.json

# Do a stock SQLite run (can take a couple hours)
cd frdr_harvest && python3 globus_harvester.py

# Compare it against the DB from the last run (is stdout OK?)
sqldiff data/globus_oai.db ../globus_oai.db

# Get rid of the git directories and keep this DB for the next run
mv data/globus_oai.db ../.
cd ..
rm -rf frdr_harvest
rm -rf frdr_deploy