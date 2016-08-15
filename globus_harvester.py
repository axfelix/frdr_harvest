"""Globus Harvester.

Usage:
  globus_harvester.py 
  globus_harvester.py [--onlyharvest | --onlyexport]

"""

from docopt import docopt
import sys
import json
import requests
import os
import time

from harvester.HarvestRepository import HarvestRepository
from harvester.OAIRepository import OAIRepository
from harvester.CKANRepository import CKANRepository
from harvester.DBInterface import DBInterface
from harvester.HarvestLogger import HarvestLogger
from harvester.TimeFormatter import TimeFormatter
from harvester.Lock import Lock


def get_config_json(repos_json="data/config.json"):
	configdict = {}

	with open(repos_json, 'r') as jsonfile:
		configdict = json.load(jsonfile)

	return configdict


if __name__ == "__main__":

	LOCK = Lock()
	tstart = time.time()
	arguments = docopt(__doc__)

	global configs
	configs = get_config_json()
	configs['error_count'] = 0
	configs['update_log_after_numitems'] = configs.get('update_log_after_numitems', 1000)
	configs['abort_after_numerrors'] = configs.get('abort_after_numerrors', 5)
	configs['record_refresh_days'] = configs.get('record_refresh_days', 30)
	configs['repo_refresh_days'] = configs.get('repo_refresh_days', 1)
	configs['temp_filepath'] = configs.get('temp_filepath', "data/temp.json")
	configs['gmeta_filepath'] = configs.get('gmeta_filepath', "data/gmeta.json")

	LOG = HarvestLogger(configs['logging'])
	LOG.info("Starting... (pid=%s)" % (os.getpid()) )

	DB = DBInterface(configs['db'])
	DB.setLogger(LOG)

	if arguments["--onlyexport"] == False:
		# Find any new information in the repositories
		for repoconfig in configs['repos']:
			if repoconfig['type'] == "oai":
				repo = OAIRepository(repoconfig)
			elif repoconfig['type'] == "ckan":
				repo = CKANRepository(repoconfig)
			repo.setDefaults(configs)
			repo.setLogger(LOG)
			repo.setDatabase(DB)
			repo.crawl()

		# Process all existing records that have not yet been fetched
		repo.update_stale_records()

	if arguments["--onlyharvest"] == True:
		raise SystemExit

	gmeta_filepath = configs['gmeta_filepath']
	temp_filepath = configs['temp_filepath']
	gmeta = repo.read_gmeta()

	try:
		with open(temp_filepath, "w") as tempfile:
			LOG.info("Writing gmeta file")
			tempfile.write(json.dumps({"_gmeta":gmeta}))
	except:
		LOG.error("Unable to write gmeta data to temporary file: %s" % (temp_filepath) )

	try:
		os.remove(gmeta_filepath)
	except:
		pass

	try:
		os.rename(temp_filepath, gmeta_filepath)
	except:
		LOG.error("Unable to move temp file %s into gmeta file location %s" % (temp_filepath, gmeta_filepath) )

	tdelta = time.time() - tstart
	TF = TimeFormatter()
	LOG.info("Done after %s" % (TF.humanize(tdelta)) )

	LOCK.unlock()