#!/usr/bin/python3

from flask import Flask
from flask import request
from flask_restful import reqparse, abort, Api, Resource
import configparser
import logging
import atexit
import daemon
from lockfile.pidlockfile import PIDLockFile

from harvester.DBInterface import DBInterface
from harvester.HarvestLogger import HarvestLogger

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger('werkzeug')
log.disabled = True

api = Api(app)

LISTEN_PORT = 8101
PIDFILE = '/tmp/harvestapi.pid'
REPOS = {"count": 0, "repositories": []}
api_log = None

def log_shutdown():
    api_log.info("REST API process shutting down")

def get_config_ini(config_file="conf/restapi.conf"):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

## API Methods

# Shows a single repo
class Repo(Resource):
    def get(self, repo_id):
        repo_id = int(repo_id)
        api_log.debug("{} GET /repos/{}".format(request.remote_addr, repo_id))
        for repo in REPOS["repositories"]:
            if int(repo["repository_id"]) == repo_id:
                return repo
        abort(404, message="Repo {} doesn't exist".format(repo_id))


# Shows a list of all repos
class RepoList(Resource):
    def get(self):
        api_log.debug("{} GET /repos".format(request.remote_addr))
        return REPOS

# Default response
class Default(Resource):
    def get(self):
        api_log.debug("{} GET /".format(request.remote_addr))
        return {}

## API resource routing

api.add_resource(RepoList, '/repos')
api.add_resource(Repo, '/repos/<repo_id>')
api.add_resource(Default, '/')


if __name__ == '__main__':
    api_config = get_config_ini()
    harvester_config = get_config_ini("conf/harvester.conf")

    api_log = HarvestLogger(api_config['logging'])
    dbh = DBInterface(harvester_config['db'])
    dbh.setLogger(api_log)

    records = dbh.get_repositories()
    for record in records:
        # Explicitly expose selected info here, so we do not accidentally leak internal data or something added in the future
        this_repo = {
            "repository_id":        record["repository_id"],
            "repository_name":      record["repository_name"],
            "repository_url":       record["repository_url"],
            "homepage_url":         record["homepage_url"],
            "repository_thumbnail": record["repository_thumbnail"],
            "repository_type":      record["repository_type"],
            "item_count":           record["item_count"]
        }
        REPOS["repositories"].append(this_repo)
    REPOS["count"] = len(records)

    atexit.register(log_shutdown)
    api_log.info("REST API process starting on port {}".format(LISTEN_PORT))

    with daemon.DaemonContext(pidfile=PIDLockFile(PIDFILE)):
        app.run(host='0.0.0.0', port=LISTEN_PORT)
