"""Globus Harvester.

Usage:
  globus_harvester.py <dbtype>...
  globus_harvester.py <dbtype> [--onlyharvest | --onlyexport]

"""

from docopt import docopt
import sys
import json
import requests
import re
import csv
import os
from sickle import Sickle


globus_endpoint_api_url = "https://rdmdev1.computecanada.ca/v1/api/collections/25"


def get_repositories(repos_csv="data/repos.csv"):
	repositories = {}

	with open(repos_csv, 'r') as csvfile:
		reader = csv.reader(csvfile)
		for row in reader:
			if len(row)==1 or not row[1]:
				repositories[row[0]] = None
			else:
				repositories[row[0]] = row[1]

	return repositories


def get_repositories_with_thumbnails(repos_csv="data/repos.csv"):
	repositories = []

	with open(repos_csv, 'r') as csvfile:
		reader = csv.DictReader(csvfile, ["Name", "URL", "Set", "Thumbnail"])

		for row in reader:
			repositories.append(row)

	return repositories


def construct_local_url(repository_url, local_identifier):
	# islandora
	if "/oai2" in repository_url:
		local_url = re.sub("\/oai2", "/islandora/object/", repository_url) + local_identifier
		return local_url

	# handle -- safer to catch these by URL regex, though slower
	#if "http://hdl.handle.net" in local_identifier:
	#	local_url = local_identifier
	#	return local_url

	# doi
	doi = re.search("(doi|DOI):\s?\S+", local_identifier)
	if doi:
		doi = doi.group(0).rstrip('\.')
		local_url = re.sub("(doi|DOI):\s?", "http://dx.doi.org/", doi)
		return local_url

	# errant URL
	local_url = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?", local_identifier)
	if local_url: return local_url.group(0)

	local_url = None
	return local_url


def rest_insert(record):
	authheader = "bearer: \'" + access_token + "\'"
	headers = {'content-type': 'application/json', 'authentication': authheader}
	response = requests.post(globus_endpoint_api_url, data=json.dumps(record), headers=headers)
	return response


def sqlite_writer(record, repository_url):
	import sqlite3 as lite

	litecon = lite.connect('data/globus_oai.db')
	with litecon:
		litecur = litecon.cursor()

		litecur.execute("CREATE TABLE IF NOT EXISTS records (title TEXT, date TEXT, local_identifier TEXT, repository_url TEXT, PRIMARY KEY (local_identifier, repository_url))  WITHOUT ROWID")
		litecur.execute("CREATE TABLE IF NOT EXISTS creators (local_identifier TEXT, repository_url TEXT, creator TEXT, is_contributor INTEGER)")
		litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_creator ON creators (local_identifier, repository_url, creator)")
		litecur.execute("CREATE TABLE IF NOT EXISTS subjects (local_identifier TEXT, repository_url TEXT, subject TEXT)")
		litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_subject ON subjects (local_identifier, repository_url, subject)")
		litecur.execute("CREATE TABLE IF NOT EXISTS rights (local_identifier TEXT, repository_url TEXT, rights TEXT)")
		litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_rights ON rights (local_identifier, repository_url, rights)")
		litecur.execute("CREATE TABLE IF NOT EXISTS descriptions (local_identifier TEXT, repository_url TEXT, description TEXT)")
		litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_description ON descriptions (local_identifier, repository_url, description)")

		try:
			litecur.execute("INSERT INTO records (title, date, local_identifier, repository_url) VALUES(?,?,?,?)", (record["title"][0], record["date"][0], record["identifier"][0], repository_url))

			for creator in record["creator"]:
				litecur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"][0], repository_url, creator, 0))

			if "contributor" in record:
				for contributor in record["contributor"]:
					litecur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"][0], repository_url, contributor, 1))

			if "subject" in record:
				for subject in record["subject"]:
					litecur.execute("INSERT INTO subjects (local_identifier, repository_url, subject) VALUES (?,?,?)", (record["identifier"][0], repository_url, subject))

			if "rights" in record:
				for rights in record["rights"]:
					litecur.execute("INSERT INTO rights (local_identifier, repository_url, rights) VALUES (?,?,?)", (record["identifier"][0], repository_url, rights))

			if "description" in record:
				for description in record["description"]:
					litecur.execute("INSERT INTO descriptions (local_identifier, repository_url, description) VALUES (?,?,?)", (record["identifier"][0], repository_url, description))

			return record["identifier"]

		except lite.IntegrityError:
			# record already present in repo
			return None


def sqlite_reader(gmeta_filepath):
	import sqlite3 as lite

	# this sucks but I can't think of a better solution right now
	deleted_records = []
	if os.path.isfile('data/deleted.db'):
		litecon = lite.connect('data/deleted.db')
		deleted_records = litecon.execute("SELECT local_identifier, repository_url FROM records").fetchall()

	litecon = lite.connect('data/globus_oai.db')
	gmeta = []

	records = litecon.execute("SELECT title, date, local_identifier, repository_url FROM records")

	for record in records:
		record = dict(zip([tuple[0] for tuple in records.description], record))
		record["dc:source"] = construct_local_url(record["repository_url"], record["local_identifier"])
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
			# I really should've done this purely in SQL
			# but group_concat() was making me angry

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


def unpack_metadata(record, repository_url):
	if 'creator' not in record.keys():
		# if there's no author, probably not a valid record
		return None

	# if date is undefined, add an empty key
	if 'date' not in record.keys():
		record["date"] = ['']

	# if multiple dates, just grab the most recent (DSpace workaround)
	if record["date"][0][0]:
		record["date"] = record["date"][0]

	if dbtype == "sqlite":
		sqlite_writer(record, repository_url)
	# elif other dbtypes


def sqlite_repo_writer(repository_url, repository_name, repository_thumbnail=""):
	import sqlite3 as lite

	litecon = lite.connect('data/globus_oai.db')
	with litecon:
		litecur = litecon.cursor()

		litecur.execute("CREATE TABLE IF NOT EXISTS repositories (repository_url TEXT, repository_name TEXT, repository_thumbnail TEXT, PRIMARY KEY (repository_url)) WITHOUT ROWID")
		litecur.execute("INSERT INTO repositories (repository_url, repository_name, repository_thumbnail) VALUES (?,?,?)", (repository_url, repository_name, repository_thumbnail))


def oai_harvest(repository_url, record_set):
	sickle = Sickle(repository_url)

	repository_name = sickle.Identify().repositoryName

	if record_set is not None:
		records = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True, set=record_set)
	else:
		records = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True)

	if dbtype == "sqlite":
		sqlite_repo_writer(repository_url, repository_name)

	while records:
		try:
			record = records.next().metadata
			unpack_metadata(record, repository_url)
		except AttributeError:
			# probably not a valid OAI record
			# Islandora throws this for non-object directories
			pass
		except StopIteration:
			break


def oai_harvest_with_thumbnails(repository):
	sickle = Sickle(repository["URL"])

	if not repository["Set"]:
		records = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True)
	else:
		records = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True, set=repository["Set"])

	if dbtype == "sqlite":
		sqlite_repo_writer(repository["URL"], repository["Name"], repository["Thumbnail"])

	while records:
		try:
			record = records.next().metadata
			unpack_metadata(record, repository["URL"])
		except AttributeError:
			# probably not a valid OAI record
			# Islandora throws this for non-object directories
			pass
		except StopIteration:
			break


if __name__ == "__main__":

	arguments = docopt(__doc__)

	global dbtype
	dbtype = arguments["<dbtype>"][0]

#	repositories = get_repositories()

#	if sys.version_info[0] == 2 and arguments["--onlyexport"] == False:
#		for repository_url, record_set in repositories.iteritems():
#			oai_harvest(repository_url, record_set)
#	elif arguments["--onlyexport"] == False:
#		for repository_url, record_set in repositories.items():
#			oai_harvest(repository_url, record_set)

	if arguments["--onlyexport"] == False:
		repositories = get_repositories_with_thumbnails()
		for repository in repositories:
				oai_harvest_with_thumbnails(repository)

	if arguments["--onlyharvest"] == True:
		raise SystemExit

#	global access_token
#	with open("data/token", "r") as tokenfile:
#		jsontoken = json.loads(tokenfile.read())
#		access_token = jsontoken['access_token'].encode()

	gmeta_filepath = "data/gmeta.json"
	if dbtype == "sqlite":
		gmeta = sqlite_reader(gmeta_filepath)

	with open(gmeta_filepath, "w") as gmetafile:
		gmetafile.write(json.dumps({"_gmeta":gmeta}))
