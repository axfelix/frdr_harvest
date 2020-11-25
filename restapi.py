#!/usr/bin/python3

from flask import Flask, request
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
    def get(self, record_id):
        record_id = int(record_id)
        get_log().debug("{} GET /records/{}".format(request.remote_addr, record_id))
        #check_cache("records")
       # for rec in CACHE["records"]["records"]:
       #     if int(rec["record_id"]) == record_id:
       #         return rec
        abort(404, message="Record {} doesn't exist".format(record_id))


# Default response
class Default(Resource):
    def get(self):
        get_log().debug("{} GET /".format(request.remote_addr))
        return {}


## API resource routing

api.add_resource(RepoList, '/repos')
api.add_resource(Repo, '/repos/<repo_id>')
#api.add_resource(RecordList, '/records')
api.add_resource(Record, '/records/<record_id>')
api.add_resource(Default, '/')

if __name__ == '__main__':
    CONFIG["restapi"] = get_config_ini("conf/restapi.conf")
    CONFIG["db"] = get_config_ini("conf/harvester.conf")["db"]

    with daemon.DaemonContext(pidfile=PIDLockFile(CONFIG["restapi"]["api"]["pidfile"]), working_directory=os.getcwd()):
        atexit.register(log_shutdown)
        get_log()
        app.run(host='0.0.0.0', debug=False, port=int(CONFIG["restapi"]["api"]["listen_port"]))
