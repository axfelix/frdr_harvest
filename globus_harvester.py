"""Globus Harvester.

Usage:
  globus_harvester.py [--onlyharvest | --onlyexport] [--export-filepath=<file>] [--export-format=<format>]

Options:
  --onlyharvest             Just harvest new items, do not export anything.
  --onlyexport              Just export existing items, do not harvest anything.
  --export-filepath=<file>  The path to export the data to.
  --export-format=<format>  The export format (gmeta or rifcs).

"""

from docopt import docopt
import sys
import json
import requests
import os
import time

from harvester.OAIRepository import OAIRepository
from harvester.CKANRepository import CKANRepository
from harvester.DBInterface import DBInterface
from harvester.HarvestLogger import HarvestLogger
from harvester.TimeFormatter import TimeFormatter
from harvester.Lock import Lock
from harvester.Exporter import Exporter


def get_config_json(repos_json="data/config.json"):
	configdict = {}

	with open(repos_json, 'r') as jsonfile:
		configdict = json.load(jsonfile)

	return configdict


if __name__ == "__main__":

	instance_lock = Lock()
	tstart = time.time()
	arguments = docopt(__doc__)

	global configs
	configs = get_config_json()
	configs['update_log_after_numitems'] = configs.get('update_log_after_numitems', 1000)
	configs['abort_after_numerrors'] = configs.get('abort_after_numerrors', 5)
	configs['record_refresh_days'] = configs.get('record_refresh_days', 30)
	configs['repo_refresh_days'] = configs.get('repo_refresh_days', 1)
	configs['temp_filepath'] = configs.get('temp_filepath', "data/temp.json")
	configs['export_filepath'] = configs.get('export_filepath', "data/gmeta.json")
	configs['export_format'] = configs.get('export_format', "gmeta")

	main_log = HarvestLogger(configs['logging'])
	main_log.info("Starting... (pid=%s)" % (os.getpid()) )

	dbh = DBInterface(configs['db'])
	dbh.setLogger(main_log)

	if arguments["--onlyexport"] == False:
		# Find any new information in the repositories
		for repoconfig in configs['repos']:
			if repoconfig['type'] == "oai":
				repo = OAIRepository(repoconfig)
			elif repoconfig['type'] == "ckan":
				repo = CKANRepository(repoconfig)
			repo.setDefaults(configs)
			repo.setLogger(main_log)
			repo.setDatabase(dbh)
			repo.crawl()
			repo.update_stale_records()

	if arguments["--onlyharvest"] == True:
		raise SystemExit

	export_filepath = arguments.get("--export-filepath", configs['export_filepath'])
	export_format = arguments.get("--export-format", configs['export_format'])
	temp_filepath = configs['temp_filepath']
	exporter = Exporter(dbh, main_log)

	if export_format == "gmeta":
		output = json.dumps({"_gmeta":exporter.generate_gmeta()})

	if export_format == "rifcs":
		output = exporter.generate_rifcs()

	with open(temp_filepath, "w") as tempfile:
		main_log.info("Writing output file")
		tempfile.write(output.encode('utf-8') )

	try:
		pass

	except:
		main_log.error("Unable to write output data to temporary file: %s" % (temp_filepath) )

	try:
		os.remove(export_filepath)
	except:
		pass

	try:
		os.rename(temp_filepath, export_filepath)
	except:
		main_log.error("Unable to move temp file %s into output file location %s" % (temp_filepath, export_filepath) )

	formatter = TimeFormatter()
	main_log.info("Done after %s" % (formatter.humanize(time.time() - tstart)) )

	instance_lock.unlock()