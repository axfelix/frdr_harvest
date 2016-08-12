"""Globus Harvester.

Usage:
  globus_harvester.py 
  globus_harvester.py [--onlyharvest | --onlyexport]

"""

from docopt import docopt
import sys
import signal
import json
import requests
import re
import csv
import os
from sickle import Sickle
from sickle.oaiexceptions import BadArgument, CannotDisseminateFormat, IdDoesNotExist, NoSetHierarchy, BadResumptionToken, NoRecordsMatch, OAIError
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
	item_id = record["local_identifier"]

	for repository in configs['repos']:
		if repository['url'] == record['repository_url']:
			if 'item_id_to_url' in repository.keys():
				for step in repository['item_id_to_url']:
					if step['action'] == "replace":
						item_id = item_id.replace(step['data'][0], step['data'][1])
					if step['action'] == "prepend":
						item_id = "" + step['data'][0] + item_id
					if step['action'] == "append":
						item_id = "" + item_id + step['data'][0]

	# Check if the item_id has already been turned into a url
	if "http" in item_id.lower():
		return item_id

	# Check if the identifier is a DOI
	doi = re.search("(doi|DOI):\s?\S+", record["local_identifier"])
	if doi:
		doi = doi.group(0).rstrip('\.')
		local_url = re.sub("(doi|DOI):\s?", "http://dx.doi.org/", doi)
		return local_url

	# If the item has a source URL, use it
	if ('source_url' in record) and record['source_url']:
		return record['source_url']

	# URL is in the identifier
	local_url = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?", record["local_identifier"])
	if local_url: 
		return local_url.group(0)

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
			litecur.execute("CREATE TABLE IF NOT EXISTS records (title TEXT, date TEXT, modified_timestamp NUMERIC DEFAULT 0, source_url TEXT, deleted NUMERIC DEFAULT 0, local_identifier TEXT, repository_url TEXT, PRIMARY KEY (local_identifier, repository_url)) WITHOUT ROWID")
			litecur.execute("CREATE TABLE IF NOT EXISTS creators (local_identifier TEXT, repository_url TEXT, creator TEXT, is_contributor INTEGER)")
			litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_creator ON creators (local_identifier, repository_url, creator)")
			litecur.execute("CREATE TABLE IF NOT EXISTS subjects (local_identifier TEXT, repository_url TEXT, subject TEXT)")
			litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_subject ON subjects (local_identifier, repository_url, subject)")
			litecur.execute("CREATE TABLE IF NOT EXISTS rights (local_identifier TEXT, repository_url TEXT, rights TEXT)")
			litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_rights ON rights (local_identifier, repository_url, rights)")
			litecur.execute("CREATE TABLE IF NOT EXISTS descriptions (local_identifier TEXT, repository_url TEXT, description TEXT)")
			litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_description ON descriptions (local_identifier, repository_url, description)")
			litecur.execute("CREATE TABLE IF NOT EXISTS repositories (repository_url TEXT, repository_name TEXT, repository_thumbnail TEXT, repository_type TEXT, last_crawl_timestamp NUMERIC, PRIMARY KEY (repository_url)) WITHOUT ROWID")
		if os.name == "posix":
			try:
				os.chmod(configs['db']['filename'], 0o664 )
			except:
				pass


def sqlite_delete_record(record):
	import sqlite3 as lite

	litecon = lite.connect(configs['db']['filename'])
	with litecon:
		litecur = litecon.cursor()

		try:
			litecur.execute("UPDATE records set deleted = 1 where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
		except:
			logger.error("Unable to mark as deleted record %s in repository %s", record['local_identifier'], record['repository_url'] )
			return False

		try:
			litecur.execute("DELETE from creators where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
			litecur.execute("DELETE from subjects where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
			litecur.execute("DELETE from rights where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
			litecur.execute("DELETE from descriptions where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
		except:
			logger.error("Unable to delete related table rows for record %s in repository %s", record['local_identifier'], record['repository_url'] )
			return False

	logger.debug("Marked as deleted: record %s in repository %s", record['local_identifier'], record['repository_url'] )
	return True


def sqlite_touch_record(record):
	import sqlite3 as lite

	litecon = lite.connect(configs['db']['filename'])
	with litecon:
		litecur = litecon.cursor()
		try:
			litecur.execute("UPDATE records set modified_timestamp = ? where local_identifier = ? and repository_url = ?", (time.time(), record['local_identifier'], record['repository_url']))
		except:
			logger.error("Unable to update modified_timestamp for record %s in repository %s", record['local_identifier'], record['repository_url'] )
			return False

	return True


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


@rate_limited(5)
def ckan_update_record(record):
	logger.debug("Updating record %s from repo at %s",record['local_identifier'],record['repository_url'])

	try:
		ckanrepo = ckanapi.RemoteCKAN(record['repository_url'])
		ckan_record = ckanrepo.action.package_show(id=record['local_identifier'])
		oai_record = format_ckan_to_oai(ckan_record,record['local_identifier'])
		sqlite_write_record(oai_record, record['repository_url'],"replace")
		return True

	except ckanapi.errors.NotAuthorized:
		# Not authorized means that we currently do not have permission to access the data but we may in the future (embargo)
		sqlite_touch_record(record)

	except ckanapi.errors.NotFound:
		# Not found means this record was deleted
		sqlite_delete_record(record)

	except:
		if not 'error_count' in configs:
			configs['error_count'] = 0
		configs['error_count'] = configs['error_count'] + 1
		if configs['error_count'] >= configs['abort_after_numerrors']:
			return False


@rate_limited(5)
def oai_update_record(record):
	logger.debug("Updating record %s from repo at %s",record['local_identifier'],record['repository_url'])

	try:
		sickle = Sickle(record["repository_url"])
		single_record = sickle.GetRecord(identifier=record["local_identifier"],metadataPrefix="oai_dc")

		metadata = single_record.metadata
		if 'identifier' in metadata.keys() and isinstance(metadata['identifier'], list):
			if "http" in metadata['identifier'][0].lower():
				metadata['dc:source'] = metadata['identifier'][0]
		metadata['identifier'] = single_record.header.identifier
		oai_record = unpack_oai_metadata(metadata)
		sqlite_write_record(oai_record, record['repository_url'],"replace")
		return True

	except IdDoesNotExist:
		# Item no longer in this repo
		sqlite_delete_record(record)

	except:
		logger.error("Updating item failed")
		if not 'error_count' in configs:
			configs['error_count'] = 0
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
		with litecon:
			litecon.row_factory = lite.Row
			litecur = litecon.cursor()
			records = litecur.execute("""SELECT r1.title, r1.date, r1.modified_timestamp, r1.local_identifier, r1.repository_url, r2.repository_type
				FROM records r1, repositories r2 
				where r1.repository_url = r2.repository_url and r1.modified_timestamp < ?
				LIMIT ?""", (stale_timestamp,configs['max_records_updated_per_run'])).fetchall()

			for record in records:
				if record_count == 0:
					logger.info("Started processing for %d records", len(records))
				if record["repository_type"] == "ckan":
					status = ckan_update_record(record)
					if not status:
						logger.error("Aborting due to errors after %s items updated in %s (%.1f items/sec)", record_count, humanize_time(time.time() - tstart), record_count/(time.time() - tstart + 0.1))
						break
				if record["repository_type"] == "oai":
					status = oai_update_record(record)
					if not status:
						logger.error("Aborting due to errors after %s items updated in %s (%.1f items/sec)", record_count, humanize_time(time.time() - tstart), record_count/(time.time() - tstart + 0.1))
						break
				record_count = record_count + 1
				if (record_count % configs['update_log_after_numitems'] == 0):
					tdelta = time.time() - tstart + 0.1
					logger.info("Done %s items after %s (%.1f items/sec)", record_count, humanize_time(tdelta), (record_count/tdelta))

	logger.info("Updated %s items in %s (%.1f items/sec)", record_count, humanize_time(time.time() - tstart),record_count/(time.time() - tstart + 0.1))


def get_repo_data(repository, column):
	returnvalue = False

	if configs['db']['type'] == "sqlite":
		import sqlite3 as lite
		litecon = lite.connect(configs['db']['filename'])
		with litecon:
			litecon.row_factory = lite.Row
			litecur = litecon.cursor()
			records = litecur.execute("select " + column + " from repositories where repository_url = ?",[repository['url']]).fetchall()
			for record in records:
				returnvalue = record[column]

	return returnvalue


def update_repo_last_crawl(repository):
	if configs['db']['type'] == "sqlite":
		import sqlite3 as lite
		litecon = lite.connect(configs['db']['filename'])
		with litecon:
			litecur = litecon.cursor()
			litecur.execute("update repositories set last_crawl_timestamp = ? where repository_url = ?",(int(time.time()),repository['url']))


def sqlite_write_record(record, repository_url, mode = "insert"):
	import sqlite3 as lite

	if record == None:
		return None

	litecon = lite.connect(configs['db']['filename'])
	with litecon:
		litecur = litecon.cursor()
		verb = "INSERT"
		if mode == "replace":
			verb = "REPLACE"

		try:
			if 'dc:source' in record:
				litecur.execute(verb + " INTO records (title, date, modified_timestamp, source_url, deleted, local_identifier, repository_url) VALUES(?,?,?,?,?,?,?)", (record["title"], record["date"], time.time(), record["dc:source"], 0, record["identifier"], repository_url))				
			else:
				litecur.execute(verb + " INTO records (title, date, modified_timestamp, deleted, local_identifier, repository_url) VALUES(?,?,?,?,?,?)", (record["title"], record["date"], time.time(), 0, record["identifier"], repository_url))				
		except lite.IntegrityError:
			# record already present in repo
			return None
	
		if "creator" in record:
			if isinstance(record["creator"], list):
				for creator in record["creator"]:
					try:
						litecur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"], repository_url, creator, 0))
					except lite.IntegrityError:
						pass
			else:
				try:
					litecur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"], repository_url, record["creator"], 0))
				except lite.IntegrityError:
					pass

		if "contributor" in record:
			if isinstance(record["contributor"], list):
				for contributor in record["contributor"]:
					try:
						litecur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"], repository_url, contributor, 1))
					except lite.IntegrityError:
						pass
			else:
				try:
					litecur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"], repository_url, record["contributor"], 1))
				except lite.IntegrityError:
					pass

		if "subject" in record:
			if isinstance(record["subject"], list):
				for subject in record["subject"]:
					try:
						litecur.execute("INSERT INTO subjects (local_identifier, repository_url, subject) VALUES (?,?,?)", (record["identifier"], repository_url, subject))
					except lite.IntegrityError:
						pass
			else:
				try:
					litecur.execute("INSERT INTO subjects (local_identifier, repository_url, subject) VALUES (?,?,?)", (record["identifier"], repository_url, record["subject"]))
				except lite.IntegrityError:
					pass

		if "rights" in record:
			if isinstance(record["rights"], list):
				for rights in record["rights"]:
					try:
						litecur.execute("INSERT INTO rights (local_identifier, repository_url, rights) VALUES (?,?,?)", (record["identifier"], repository_url, rights))
					except lite.IntegrityError:
						pass
			else:
				try:
					litecur.execute("INSERT INTO rights (local_identifier, repository_url, rights) VALUES (?,?,?)", (record["identifier"], repository_url, record["rights"]))
				except lite.IntegrityError:
					pass

		if "description" in record:
			if isinstance(record["description"], list):
				for description in record["description"]:
					try:
						litecur.execute("INSERT INTO descriptions (local_identifier, repository_url, description) VALUES (?,?,?)", (record["identifier"], repository_url, description))
					except lite.IntegrityError:
						pass
			else:
				try:
					litecur.execute("INSERT INTO descriptions (local_identifier, repository_url, description) VALUES (?,?,?)", (record["identifier"], repository_url, record["description"]))
				except lite.IntegrityError:
					pass

	return record["identifier"]


def sqlite_reader():
	import sqlite3 as lite
	litecon = lite.connect(configs['db']['filename'])
	gmeta = []

	# Only select records that have complete data
	records = litecon.execute("""SELECT r1.title, r1.date, r1.source_url, r1.deleted, r1.local_identifier, r1.repository_url, r2.repository_name as "nrdr:origin.id", r2.repository_thumbnail as "nrdr:origin.icon"
			FROM records r1, repositories r2 
			WHERE r1.title != '' and r1.repository_url = r2.repository_url """)

	for record in records:
		record = dict(zip([tuple[0] for tuple in records.description], record))
		record["dc:source"] = construct_local_url(record)
		if record["dc:source"] is None:
			continue

		if record["deleted"] == 1:
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

			record.pop("repository_url", None)
			record.pop("local_identifier", None)

		record["dc:title"] = record["title"]
		record.pop("title", None)
		record["dc:date"] = record["date"]
		record.pop("date", None)
		record.pop("source_url", None)
		record.pop("deleted", None)

		record["@context"] = {"dc" : "http://dublincore.org/documents/dcmi-terms", "nrdr" : "http://nrdr-ednr.ca/schema/1.0/"}
		gmeta_data = {record["dc:source"] : {"mimetype": "application/json", "content": record}}
		gmeta.append(gmeta_data)

	return gmeta


def format_ckan_to_oai(ckan_record, local_identifier):
	record = {}

	if ('author' in ckan_record) and ckan_record['author']:
		record["creator"] = ckan_record['author']
	elif ('maintainer' in ckan_record) and ckan_record['maintainer']:
		record["creator"] = ckan_record['maintainer']
	else:
		record["creator"] = ckan_record['organization']['title']

	record["identifier"] = local_identifier
	record["title"] = ckan_record['title']
	record["description"] = ckan_record['notes']
	record["date"] = ckan_record['date_published']
	record["subject"] = ckan_record['subject']
	record["rights"] = ckan_record['attribution']
	record["dc:source"] = ckan_record['url']

	return record


def unpack_oai_metadata(record):
	if 'identifier' not in record.keys():
		return None

	# If there are multiple identifiers, and one of them contains a link, then prefer it
	# Otherwise just take the first one
	if isinstance(record["identifier"], list):
		valid_id = record["identifier"][0] 
		for idstring in record["identifier"]:
			if "http" in idstring.lower():
				valid_id = idstring
		record["identifier"] = valid_id

	if 'creator' not in record.keys():
		logger.debug("Item %s is missing creator - will not be added", record["identifier"])
		return None

	# If date is undefined add an empty key
	if 'date' not in record.keys():
		record["date"] = ""

	# If there are multiple dates choose the longest one (likely the most specific)
	if isinstance(record["date"], list):
		valid_date = record["date"][0]
		for datestring in record["date"]:
			if len(datestring) > len(valid_date):
				valid_date = datestring
		record["date"] = valid_date

	# Convert long dates into YYYY-MM-DD
	datestring = re.search("(\d{4}[-/]\d{2}[-/]\d{2})", record["date"])
	if datestring:
		record["date"] = datestring.group(0).replace("/","-")
		
	if isinstance(record["title"], list):
		record["title"] = record["title"][0]

	return record

def sqlite_create_repo(repository_url, repository_name, repository_type, repository_thumbnail=""):
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
		sqlite_create_repo(repository["url"], repository["name"], "oai", repository["thumbnail"])

	item_count = 0
	log_update_interval = configs['update_log_after_numitems']
	if 'update_log_after_numitems' in repository:
		log_update_interval = repository['update_log_after_numitems']

	while records:
		try:
			record = records.next()
			metadata = record.metadata
			if 'identifier' in metadata.keys() and isinstance(metadata['identifier'], list):
				if "http" in metadata['identifier'][0].lower():
					metadata['dc:source'] = metadata['identifier'][0]
			metadata['identifier'] = record.header.identifier
			oai_record = unpack_oai_metadata(metadata)
			sqlite_write_record(oai_record, repository["url"])
			item_count = item_count + 1
			if (item_count % log_update_interval == 0):
				tdelta = time.time() - repository["tstart"] + 0.1
				logger.info("Done %s items after %s (%.1f items/sec)", item_count, humanize_time(tdelta), (item_count/tdelta))
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
		sqlite_create_repo(repository["url"], repository["name"], "ckan", repository["thumbnail"])

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
			tdelta = time.time() - repository["tstart"] + 0.1
			logger.info("Done %s item headers after %s (%.1f items/sec)", (item_existing_count + item_new_count), humanize_time(tdelta), ((item_existing_count + item_new_count)/tdelta))

	logger.info("Found %s items in feed (%d existing, %d new)", (item_existing_count + item_new_count), item_existing_count, item_new_count)


def humanize_time(amount):

	INTERVALS = [ 1, 60, 3600, 86400, 604800, 2629800, 31557600 ]
	NAMES = [('second', 'seconds'),	('minute', 'minutes'), ('hour', 'hours'),
		('day', 'days'), ('week', 'weeks'), ('month', 'months'), ('year', 'years')]
	result = ""
	amount = int(amount)

	for i in range(len(NAMES)-1, -1, -1):
		a = amount // INTERVALS[i]
		if a > 0: 
			result = result + str(a) + " " + str(NAMES[i][1 % a]) + " "
			amount -= a * INTERVALS[i]

	result = str.strip(result)
	if result == "":
		result = "0 seconds"
	return result


if __name__ == "__main__":

	if os.name == 'posix':
		import fcntl
		lockfile = open('lockfile','w')
		try:
			os.chmod('lockfile', 0o664 )
		except:
			pass
		try:
			fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
		except (OSError, IOError):
			sys.stderr.write("ERROR: is harvester already running? (could not lock lockfile)\n")
			raise SystemExit

	tstart = time.time()
	arguments = docopt(__doc__)

	global configs
	configs = get_config_json()
	configs['error_count'] = 0
	if not 'update_log_after_numitems' in configs:
		configs['update_log_after_numitems'] = 1000
	if not 'abort_after_numerrors' in configs:
		configs['abort_after_numerrors'] = 5
	if not 'record_refresh_days' in configs:
		configs['record_refresh_days'] = 30
	if not 'repo_refresh_days' in configs:
		configs['repo_refresh_days'] = 1
	if not 'temp_filepath' in configs:
		configs['temp_filepath'] = "data/temp.json"
	if not 'gmeta_filepath' in configs:
		configs['gmeta_filepath'] = "data/gmeta.json"

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

	logger.info("Starting... (pid=%s)",os.getpid())
	initialize_database()

	if arguments["--onlyexport"] == False:
		# Find any new information in the repositories
		for repository in configs['repos']:
			repository["tstart"] = time.time()
			repository["last_crawl"] = get_repo_data(repository, "last_crawl_timestamp")
			if repository["last_crawl"] == 0:
				logger.info("Repo: " + repository['name'] + " (last harvested: never)" )
			else:
				logger.info("Repo: " + repository['name'] + " (last harvested: %s ago)",humanize_time(repository["tstart"] - repository["last_crawl"]) )
			if (repository["enabled"]):
				repo_refresh_days = configs['repo_refresh_days']
				if 'repo_refresh_days' in repository:
					repo_refresh_days = repository['repo_refresh_days']
				if (repository["last_crawl"] + repo_refresh_days*86400) < repository["tstart"]:
					if repository["type"] == "oai":
						oai_harvest_with_thumbnails(repository)
					elif repository["type"] == "ckan":
						ckan_get_package_list(repository)
					update_repo_last_crawl(repository)
				else:
					logger.info("This repo is not yet due to be harvested")
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
	temp_filepath = configs['temp_filepath']
	if configs['db']['type'] == "sqlite":
		gmeta = sqlite_reader()

	try:
		with open(temp_filepath, "w") as tempfile:
			logger.info("Writing gmeta file")
			tempfile.write(json.dumps({"_gmeta":gmeta}))
	except:
		logger.error("Unable to write gmeta data to temporary file: %s", temp_filepath)

	try:
		os.remove(gmeta_filepath)
	except:
		pass

	try:
		os.rename(temp_filepath, gmeta_filepath)
	except:
		logger.error("Unable to move temp file %s into gmeta file location %s", temp_filepath, gmeta_filepath)

	tdelta = time.time() - tstart
	logger.info("Done after %s", humanize_time(tdelta))

	if os.name == 'posix':
		fcntl.flock(lockfile, fcntl.LOCK_UN)
		lockfile.close()
		os.unlink('lockfile')