**QUICKSTART FOR DEPLOYING HARVESTER ON LOCALHOST WITH POSTGRES AND DOCKER**

1. Install Docker: https://docs.docker.com/install/
2. Install local postgres client: https://wiki.postgresql.org/wiki/Detailed_installation_guides
3. Install generic postgres container:
``` shell
docker pull postgres
```
4. Start docker container with password of your choice:
``` shell
docker run --name harvest-pgsql -e POSTGRES_PASSWORD=$PASSWORD-p 5432:5432 \
  --mount source=pgsql-vol,destination=/var/lib/postgresql/data postgres
```
5. Test pgsql and create harvest database
``` shell
psql -U postgres -h localhost -p 5432
postgres=# CREATE DATABASE harvest;
```
6. Set configuration values in conf/harvester.conf
```shell
type = postgres
dbname = harvest
host = localhost
schema =
user = postgres
pass = $PASSWORD
```
7. Add additional repos (if any) to conf/ and run harvester
``` shell
python3 ./globus_harvester.py --onlyharvest
```
8. Run search web UI to access harvest results (localhost:8100):
``` shell
python3 admin/admin.py
```

