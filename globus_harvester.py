"""Globus Harvester.

Usage:
  globus_harvester.py 
  globus_harvester.py [--onlyharvest | --onlyexport]

"""

from docopt import docopt
import sys
import signal
import fcntl
import json
import requests
import re
import csv
import os
from sickle import Sickle
import ckanapi
import time
import logging
import time
import threading
from functools import wraps
from logging.handlers import TimedRotatingFileHandler

def rate_limited(max_per_second):
    """
    Decorator that make functions not be called faster than a set rate
    """
    lock = threading.Lock()
    min_interval = 1.0 / float(max_per_second)

    def decorate(func):
        last_time_called = [0.0]

        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            lock.acquire()
            elapsed = time.clock() - last_time_called[0]
            left_to_wait = min_interval - elapsed

            if left_to_wait > 0:
                time.sleep(left_to_wait)

            lock.release()

            ret = func(*args, **kwargs)
            last_time_called[0] = time.clock()
            return ret

        return rate_limited_function

    return decorate

def get_config_json(repos_json="data/config.json"):
	configdict = {}

	with open(repos_json, 'r') as jsonfile:
		configdict = json.load(jsonfile)

	return configdict


def construct_local_url(record):
	# islandora
	if "/oai2" in record["repository_url"]:
		local_url = re.sub("\/oai2", "/islandora/object/", record["repository_url"]) + record["local_identifier"]
		return local_url

	# handle -- safer to catch these by URL regex, though slower
	#if "http://hdl.handle.net" in local_identifier:
	#	local_url = local_identifier
	#	return local_url

	# doi
	doi = re.search("(doi|DOI):\s?\S+", record["local_identifier"])
	if doi:
		doi = doi.group(0).rstrip('\.')
		local_url = re.sub("(doi|DOI):\s?", "http://dx.doi.org/", doi)
		return local_url

	# CKAN records
	if ('source_url' in record) and record['source_url']:
		return record['source_url']

	# URL is in the identifier
	local_url = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?", record["local_identifier"])
	if local_url: return local_url.group(0)

	local_url = None
	return local_url


def rest_insert(record):
	authheader = "bearer: \'" + access_token + "\'"
	headers = {'content-type': 'application/json', 'authentication': authheader}
	response = requests.post(configs['globus_rest_url'], data=json.dumps(record), headers=headers)
	return response


def initialize_database():
	if configs['db']['type'] == "sqlite":
		import sqlite3 as lite
		litecon = lite.connect(configs['db']['filename'])
		with litecon:
			litecur = litecon.cursor()
			litecur.execute("CREATE TABLE IF NOT EXISTS records (title TEXT, date TEXT, modified_timestamp NUMERIC, source_url TEXT, local_identifier TEXT, repository_url TEXT, PRIMARY KEY (local_identifier, repository_url)) WITHOUT ROWID")
			litecur.execute("CREATE TABLE IF NOT EXISTS creators (local_identifier TEXT, repository_url TEXT, creator TEXT, is_contributor INTEGER)")
			litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_creator ON creators (local_identifier, repository_url, creator)")
			litecur.execute("CREATE TABLE IF NOT EXISTS subjects (local_identifier TEXT, repository_url TEXT, subject TEXT)")
			litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_subject ON subjects (local_identifier, repository_url, subject)")
			litecur.execute("CREATE TABLE IF NOT EXISTS rights (local_identifier TEXT, repository_url TEXT, rights TEXT)")
			litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_rights ON rights (local_identifier, repository_url, rights)")
			litecur.execute("CREATE TABLE IF NOT EXISTS descriptions (local_identifier TEXT, repository_url TEXT, description TEXT)")
			litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_description ON descriptions (local_identifier, repository_url, description)")
			litecur.execute("CREATE TABLE IF NOT EXISTS repositories (repository_url TEXT, repository_name TEXT, repository_thumbnail TEXT, repository_type TEXT, last_crawl_timestamp NUMERIC, PRIMARY KEY (repository_url)) WITHOUT ROWID")


def sqlite_write_header(record_id, repository_url):
	import sqlite3 as lite

	litecon = lite.connect(configs['db']['filename'])
	with litecon:
		litecur = litecon.cursor()

		try:
			litecur.execute("INSERT INTO records (title, date, modified_timestamp, local_identifier, repository_url) VALUES(?,?,?,?,?)", ("", "", 0, record_id, repository_url))
		except lite.IntegrityError:
			# record already present in repo
			return None

	return record_id


@rate_limited(10)
def ckan_update_record(record):
	logger.debug("Updating record %s from repo at %s",record['local_identifier'],record['repository_url'])
	ckanrepo = ckanapi.RemoteCKAN(record['repository_url'])
	try:
		ckan_record = ckanrepo.action.package_show(id=record['local_identifier'])
		oai_record = format_ckan_to_oai(ckan_record,record['local_identifier'])
		sqlite_write_record(oai_record, record['repository_url'],"replace")
		return True
	except:
		configs['error_count'] = configs['error_count'] + 1
		if configs['error_count'] >= configs['abort_after_numerrors']:
			return False


def update_stale_records():
	record_count = 0
	tstart = time.time()
	logger.info("Looking for stale records to update")

	if configs['db']['type'] == "sqlite":
		import sqlite3 as lite

		stale_timestamp = int(time.time() - configs['record_refresh_days']*86400)
		recordset = []
		litecon = lite.connect(configs['db']['filename'])
		litecon.row_factory = lite.Row
		litecur = litecon.cursor()
		records = litecur.execute("""SELECT r1.title, r1.date, r1.modified_timestamp, r1.local_identifier, r1.repository_url, r2.repository_type
			FROM records r1, repositories r2 
			where r1.repository_url = r2.repository_url and r1.modified_timestamp < ?
			LIMIT ?""", (stale_timestamp,configs['max_records_updated_per_run'])).fetchall()

		for record in records:
			if record["repository_type"] == "ckan":
				status = ckan_update_record(record)
				if not status:
					logger.error("Aborting due to errors after %s items updated in %.1f seconds (%.1f items/sec)", record_count,(time.time() - tstart),record_count/(time.time() - tstart))
					break
			record_count = record_count + 1

	logger.info("Updated %s items in %.1f seconds (%.1f items/sec)", record_count,(time.time() - tstart),record_count/(time.time() - tstart))


def get_repo_last_crawl(repository):
	last_crawl_timestamp = 0

	if configs['db']['type'] == "sqlite":
		import sqlite3 as lite
		litecon = lite.connect(configs['db']['filename'])
		litecon.row_factory = lite.Row
		litecur = litecon.cursor()
		records = litecur.execute("select last_crawl_timestamp from repositories where repository_url = ?",[repository['url']]).fetchall()
		for record in records:
			last_crawl_timestamp = record['last_crawl_timestamp']

	return last_crawl_timestamp


def update_repo_last_crawl(repository):
	if configs['db']['type'] == "sqlite":
		import sqlite3 as lite
		litecon = lite.connect(configs['db']['filename'])
		litecur = litecon.cursor()
		litecur.execute("update repositories set last_crawl_timestamp = ? where repository_url = ?",(time.time(),repository['url']))


def sqlite_write_record(record, repository_url, mode = "insert"):
	import sqlite3 as lite

	litecon = lite.connect(configs['db']['filename'])
	with litecon:
		litecur = litecon.cursor()

		try:
			if mode == "replace":
				if 'dc:source' in record:
					litecur.execute("REPLACE INTO records (title, date, modified_timestamp, source_url, local_identifier, repository_url) VALUES(?,?,?,?,?,?)", (record["title"][0], record["date"][0], time.time(), record["dc:source"][0], record["identifier"][0], repository_url))				
				else:
					litecur.execute("REPLACE INTO records (title, date, modified_timestamp, local_identifier, repository_url) VALUES(?,?,?,?,?)", (record["title"][0], record["date"][0], time.time(), record["identifier"][0], repository_url))				
			else:
				litecur.execute("INSERT INTO records (title, date, modified_timestamp, local_identifier, repository_url) VALUES(?,?,?,?,?)", (record["title"][0], record["date"][0], time.time(), record["identifier"][0], repository_url))
		except lite.IntegrityError:
			# record already present in repo
			return None
	
		if "creator" in record:
			for creator in record["creator"]:
				try:
					litecur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"][0], repository_url, creator, 0))
				except lite.IntegrityError:
					pass

		if "contributor" in record:
			for contributor in record["contributor"]:
				try:
					litecur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"][0], repository_url, contributor, 1))
				except lite.IntegrityError:
					pass

		if "subject" in record:
			for subject in record["subject"]:
				try:
					litecur.execute("INSERT INTO subjects (local_identifier, repository_url, subject) VALUES (?,?,?)", (record["identifier"][0], repository_url, subject))
				except lite.IntegrityError:
					pass

		if "rights" in record:
			for rights in record["rights"]:
				try:
					litecur.execute("INSERT INTO rights (local_identifier, repository_url, rights) VALUES (?,?,?)", (record["identifier"][0], repository_url, rights))
				except lite.IntegrityError:
					pass

		if "description" in record:
			for description in record["description"]:
				try:
					litecur.execute("INSERT INTO descriptions (local_identifier, repository_url, description) VALUES (?,?,?)", (record["identifier"][0], repository_url, description))
				except lite.IntegrityError:
					pass

	return record["identifier"]


def sqlite_reader():
	import sqlite3 as lite

	# TODO: improve deleted record tracking
	deleted_records = []
	if os.path.isfile('data/deleted.db'):
		litecon = lite.connect('data/deleted.db')
		deleted_records = litecon.execute("SELECT local_identifier, repository_url FROM records").fetchall()

	litecon = lite.connect(configs['db']['filename'])
	gmeta = []

	# Only select records that have complete data
	records = litecon.execute("SELECT title, date, source_url, local_identifier, repository_url FROM records where modified_timestamp > 0")

	for record in records:
		record = dict(zip([tuple[0] for tuple in records.description], record))
		record["dc:source"] = construct_local_url(record)
		if record["dc:source"] is None:
			continue

		deleted_tuple = (record["local_identifier"], record["repository_url"])
		if deleted_tuple in deleted_records:
			gmeta_data = {record["dc:source"] : {"mimetype": "application/json", "content": None}}
			gmeta.append(gmeta_data)
			continue

		with litecon:
			litecon.row_factory = lambda cursor, row: row[0]
			litecur = litecon.cursor()

			# attach the other values to the dict
			# TODO: investigate doing this purely in SQL

			litecur.execute("SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=0", (record["local_identifier"], record["repository_url"]))
			record["dc:contributor.author"] = litecur.fetchall()

			litecur.execute("SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=1", (record["local_identifier"], record["repository_url"]))
			record["dc:contributor"] = litecur.fetchall()

			litecur.execute("SELECT subject FROM subjects WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
			record["dc:subject"] = litecur.fetchall()

			litecur.execute("SELECT rights FROM rights WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
			record["dc:rights"] = litecur.fetchall()

			litecur.execute("SELECT description FROM descriptions WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
			record["dc:description"] = litecur.fetchall()

			litecur.execute("SELECT repository_name FROM repositories WHERE repository_url=?", (record["repository_url"],))
			record["nrdr:origin.id"] = litecur.fetchall()

			litecur.execute("SELECT repository_thumbnail FROM repositories WHERE repository_url=?", (record["repository_url"],))
			record["nrdr:origin.icon"] = litecur.fetchall()

			record.pop("repository_url", None)
			record.pop("local_identifier", None)

		record["dc:title"] = record["title"]
		record.pop("title", None)
		record["dc:date"] = record["date"]
		record.pop("date", None)

		#api_response = rest_insert(record)

		record["@context"] = {"dc" : "http://dublincore.org/documents/dcmi-terms", "nrdr" : "http://nrdr-ednr.ca/schema/1.0/"}
		gmeta_data = {record["dc:source"] : {"mimetype": "application/json", "content": record}}
		gmeta.append(gmeta_data)

	return gmeta


def format_ckan_to_oai(ckan_record, local_identifier):
	record = {}

	if ('author' in ckan_record) and ckan_record['author']:
		record["creator"] = [ckan_record['author']]
	elif ('maintainer' in ckan_record) and ckan_record['maintainer']:
		record["creator"] = [ckan_record['maintainer']]
	else:
		record["creator"] = [ckan_record['organization']['title']]

	record["identifier"] = [local_identifier]
	record["title"] = [ckan_record['title']]
	record["description"] = [ckan_record['notes']]
	record["date"] = [ckan_record['date_published']]
	record["subject"] = ckan_record['subject']
	record["rights"] = [ckan_record['attribution']]
	record["dc:source"] = [ckan_record['url']]

	return record


def unpack_metadata(record, repository_url):
	if 'creator' not in record.keys():
		# if there's no author, probably not a valid record
		return None

	# if date is undefined, add an empty key
	if 'date' not in record.keys():
		record["date"] = ['']

	# if multiple dates, just grab the most recent (DSpace workaround)
	try:
		if record["date"][0][0]:
			record["date"] = record["date"][0]
	except:
		pass

	if configs['db']['type'] == "sqlite":
		sqlite_write_record(record, repository_url)


def sqlite_repo_writer(repository_url, repository_name, repository_type, repository_thumbnail=""):
	import sqlite3 as lite

	litecon = lite.connect(configs['db']['filename'])
	with litecon:
		litecur = litecon.cursor()

		try:
			litecur.execute("INSERT INTO repositories (repository_url, repository_name, repository_type, repository_thumbnail, last_crawl_timestamp) VALUES (?,?,?,?,?)", (repository_url, repository_name, repository_type, repository_thumbnail, time.time()))
		except lite.IntegrityError:
			# record already present in repo
			return None


def oai_harvest_with_thumbnails(repository):
	sickle = Sickle(repository["url"])
	records = []

	if not repository["set"]:
		try:
			records = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True)
		except:
			logger.info("No items were found")
	else:
		try:
			records = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True, set=repository["set"])
		except:
			logger.info("No items were found")

	if configs['db']['type'] == "sqlite":
		sqlite_repo_writer(repository["url"], repository["name"], "oai", repository["thumbnail"])

	item_count = 0
	log_update_interval = configs['update_log_after_numitems']
	if 'update_log_after_numitems' in repository:
		log_update_interval = repository['update_log_after_numitems']

	while records:
		try:
			record = records.next().metadata
			unpack_metadata(record, repository["url"])
			item_count = item_count + 1
			if (item_count % log_update_interval == 0):
				tdelta = time.time() - repository["tstart"]
				logger.info("Done %s items after %.1f seconds (%.1f items/sec)", item_count, tdelta, (item_count/tdelta))
		except AttributeError:
			# probably not a valid OAI record
			# Islandora throws this for non-object directories
			pass
		except StopIteration:
			break
	logger.info("Processed %s items in feed", item_count)

def ckan_get_package_list(repository):
	ckanrepo = ckanapi.RemoteCKAN(repository["url"])

	if configs['db']['type'] == "sqlite":
		sqlite_repo_writer(repository["url"], repository["name"], "ckan", repository["thumbnail"])

	records = ckanrepo.action.package_list()

	item_existing_count = 0
	item_new_count = 0
	log_update_interval = configs['update_log_after_numitems']
	if 'update_log_after_numitems' in repository:
		log_update_interval = repository['update_log_after_numitems']
	for record_id in records:
		result = sqlite_write_header(record_id, repository["url"])
		if result == None:
			item_existing_count = item_existing_count + 1
		else:
			item_new_count = item_new_count + 1
		if ((item_existing_count + item_new_count) % log_update_interval == 0):
			tdelta = time.time() - repository["tstart"]
			logger.info("Done %s item headers after %.1f seconds (%.1f items/sec)", (item_existing_count + item_new_count), tdelta, ((item_existing_count + item_new_count)/tdelta))

	logger.info("Found %s items in feed (%d existing, %d new)", (item_existing_count + item_new_count), item_existing_count, item_new_count)

if __name__ == "__main__":

	if os.name == 'posix':
		lockfile = open('lockfile','w')
		try:
			fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
		except (OSError, IOError):
			sys.stderr.write("ERROR: is harvester already running? (could not lock lockfile)\n")
			sys.exit(-1)

	tstart = time.time()
	arguments = docopt(__doc__)

	global configs 
	configs = get_config_json()
	if not 'update_log_after_numitems' in configs:
		configs['update_log_after_numitems'] = 1000
	configs['error_count'] = 0
	if not 'abort_after_numerrors' in configs:
		configs['abort_after_numerrors'] = 5
	if not 'record_refresh_days' in configs:
		configs['record_refresh_days'] = 30
	if not 'repo_refresh_days' in configs:
		configs['repo_refresh_days'] = 1

	logdir = os.path.dirname(configs['logging']['filename'])
	if not os.path.exists(logdir):
		os.makedirs(logdir)

	handler = TimedRotatingFileHandler(configs['logging']['filename'], when="D", interval=configs['logging']['daysperfile'], backupCount=configs['logging']['keep'])
	logFormatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	handler.setFormatter(logFormatter)
	logger = logging.getLogger("Rotating Log")
	logger.addHandler(handler)
	logger.setLevel(logging.DEBUG)
	if 'level' in configs['logging']:
		if (configs['logging']['level'].upper() == "INFO"):
			logger.setLevel(logging.INFO)
		if (configs['logging']['level'].upper() == "ERROR"):
			logger.setLevel(logging.ERROR)

	logger.info("Starting...")
	initialize_database()

	if arguments["--onlyexport"] == False:
		configs = get_config_json()

		# Find any new information in the repositories
		for repository in configs['repos']:
			logger.info("Repo: " + repository['name'])
			if (repository["enabled"]):
				repository["tstart"] = time.time()
				repo_refresh_days = configs['repo_refresh_days']
				if 'repo_refresh_days' in repository:
					repo_refresh_days = repository['repo_refresh_days']
				if (get_repo_last_crawl(repository) + repo_refresh_days*86400) < repository["tstart"]:
					if repository["type"] == "oai":
						oai_harvest_with_thumbnails(repository)
					elif repository["type"] == "ckan":
						ckan_get_package_list(repository)
					update_repo_last_crawl(repository)
				else:
					logger.info("This repo is not yet due to be harvested again")
			else:
				logger.info("This repo is not enabled for harvesting")

		# Process all existing records that have not yet been fetched
		update_stale_records()

	if arguments["--onlyharvest"] == True:
		raise SystemExit

#	global access_token
#	with open("data/token", "r") as tokenfile:
#		jsontoken = json.loads(tokenfile.read())
#		access_token = jsontoken['access_token'].encode()

	gmeta_filepath = configs['gmeta_filepath']
	if configs['db']['type'] == "sqlite":
		gmeta = sqlite_reader()

	with open(gmeta_filepath, "w") as gmetafile:
		logger.info("Writing gmeta file")
		gmetafile.write(json.dumps({"_gmeta":gmeta}))

	tdelta = time.time() - tstart
	logger.info("Done after %.1f seconds", tdelta)

	if os.name == 'posix':
		fcntl.flock(lockfile, fcntl.LOCK_UN)
		lockfile.close()
		os.unlink('lockfile')