#!/usr/bin/python3

from flask import Flask, request, Response, got_request_exception
from flask_restful import reqparse, abort, Api, Resource
import configparser
import logging
import atexit
import time
import daemon
import os
from lockfile.pidlockfile import PIDLockFile

from harvester.DBInterface import DBInterface
from harvester.HarvestLogger import HarvestLogger
from harvester.ExporterDataverse import ExporterDataverse

app = Flask(__name__)

# Disable the default logging, it will not work in daemon mode
app.logger.disabled = True
log = logging.getLogger('werkzeug')
log.disabled = True

api = Api(app)

CACHE = {"repositories": {"count": 0, "repositories": [], "timestamp": 0}}
CONFIG = {"restapi": None, "db": None, "handles": {}}


def get_log():
    if "log" not in CONFIG["handles"]:
        CONFIG["handles"]["log"] = HarvestLogger(CONFIG["restapi"]["logging"])
        CONFIG["handles"]["log"].info(
            "Harvest REST API process starting on port {}".format(CONFIG["restapi"]["api"]["listen_port"]))
    return CONFIG["handles"]["log"]


def get_db():
    if "db" not in CONFIG["handles"]:
        CONFIG["handles"]["db"] = DBInterface(CONFIG['db'])
    return CONFIG["handles"]["db"]


def get_exporter():
    if "exporter" not in CONFIG["handles"]:
        CONFIG["export"]["destination"] = "stream"
        CONFIG["handles"]["exporter"] = ExporterDataverse(get_db(), get_log(), CONFIG["export"])
    return CONFIG["handles"]["exporter"]


def check_cache(objname):
    if objname in CACHE:
        if (int(time.time()) - CACHE[objname]["timestamp"] > int(CONFIG["restapi"]["api"]["max_cache_age"])):
            get_log().debug("Cache expired for {}; reloading".format(objname))
            if objname == "repositories":
                records = get_db().get_repositories()
                CACHE["repositories"]["repositories"][:] = []  # Purge existing list
                for record in records:
                    # Explicitly expose selected info here, so we do not accidentally leak internal data or something added in the future
                    this_repo = {
                        "repository_id": record["repository_id"],
                        "repository_name": record["repository_name"],
                        "repository_url": record["repository_url"],
                        "homepage_url": record["homepage_url"],
                        "repository_thumbnail": record["repository_thumbnail"],
                        "repository_type": record["repository_type"],
                        "item_count": record["item_count"]
                    }
                    CACHE["repositories"]["repositories"].append(this_repo)
                CACHE["repositories"]["count"] = len(records)
                CACHE["repositories"]["timestamp"] = int(time.time())


def log_shutdown():
    get_log().info("REST API process shutting down")


def get_config_ini(config_file):
    c = configparser.ConfigParser()
    try:
        c.read(config_file)
        return c
    except:
        return None


## API Methods

# Shows a single repo
class Repo(Resource):
    def get(self, repo_id):
        repo_id = int(repo_id)
        get_log().debug("{} GET /repos/{}".format(request.remote_addr, repo_id))
        check_cache("repositories")
        for repo in CACHE["repositories"]["repositories"]:
            if int(repo["repository_id"]) == repo_id:
                return repo
        abort(404, message="Repo {} doesn't exist".format(repo_id))


# Shows a list of all repos
class RepoList(Resource):
    def get(self):
        get_log().debug("{} GET /repos".format(request.remote_addr))
        check_cache("repositories")
        return CACHE["repositories"]


# Shows a single record
class Record(Resource):

    editable_fields = ["geodisy_harvested"]

    def get(self, record_id):
        record_id = int(record_id)
        get_log().debug("{} GET /records/{}".format(request.remote_addr, record_id))
        recs = get_db().get_multiple_records("records", "*", "record_id", record_id)
        
        if len(recs) == 0:
            abort(404, message="Record {} not found".format(record_id))
        else:
            record = recs[0]
            this_record = {}
            if int(record["deleted"]) == 1:
                this_record = {
                    "record_id": record["record_id"],
                    "deleted": record["deleted"]
                }
            else:
                this_record = {
                    "record_id": record["record_id"],
                    "repository_id": int(record["repository_id"]),
                    "title": record["title"],
                    "title_fr": record["title_fr"],
                    "pub_date": record["pub_date"],
                    "modified_timestamp": int(record["modified_timestamp"]),
                    "source_url": record["source_url"],
                    "deleted": int(record["deleted"]),
                    "local_identifier": record["local_identifier"],
                    "series": record["series"],
                    "item_url": record["item_url"],
                    "upstream_modified_timestamp": int(record["upstream_modified_timestamp"]),
                    "geodisy_harvested": int(record["geodisy_harvested"])
                }
            return this_record

    def put(self, record_id):
        record_id = int(record_id)
        body = request.json
        get_log().debug("{} PUT /records/{} - {}".format(request.remote_addr, record_id, request.json))

        # filter the json body passed in to only allow fields with keys in the 
        # self.editable_fields
        filtered_body = {k:v for k,v in body.items() if k in self.editable_fields}

        try:
            get_db().update_record(record_id, filtered_body)
        except Exception as e:
            get_log().debug(e)

        response = self.get(record_id)
        return response


class Exporter(Resource):
    def get(self):
        def _generate_resp():
            yield get_exporter()._generate(False)

        get_log().debug("{} GET /exporter".format(request.remote_addr))

        return Response(_generate_resp()) 
        

# Default response
class Default(Resource):
    def get(self):
        get_log().debug("{} GET /".format(request.remote_addr))
        return {"message": "Harvest REST API is running"}


# API resource routing

api.add_resource(RepoList, '/repos')
api.add_resource(Repo, '/repos/<repo_id>')
api.add_resource(Record, '/records/<record_id>')
api.add_resource(Exporter, '/exporter')
api.add_resource(Default, '/')

if __name__ == '__main__':
    CONFIG["restapi"] = get_config_ini("conf/restapi.conf")
    CONFIG["db"] = get_config_ini("conf/harvester.conf")["db"]
    CONFIG["export"] = get_config_ini("conf/harvester.conf")["export"]

    # For debugging use this line and comment out the daemon block below
    # and then run the API with 'python3 restapi.py', view at http://localhost:listen_port
    app.run(host='0.0.0.0', debug=True, port=int(CONFIG["restapi"]["api"]["listen_port"]))

#    with daemon.DaemonContext(pidfile=PIDLockFile(CONFIG["restapi"]["api"]["pidfile"]), working_directory=os.getcwd()):
#        atexit.register(log_shutdown)
#        get_log()
#        #app.run(host='0.0.0.0', debug=False, port=int(CONFIG["restapi"]["api"]["listen_port"]))
#        app.run(host='0.0.0.0', debug=True, port=int(CONFIG["restapi"]["api"]["listen_port"]))
