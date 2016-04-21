import sys
import json
import requests
from sickle import Sickle


# TODO: make these dynamic
repositories = {'http://researchdata.sfu.ca/oai2':None, 'http://dataverse.scholarsportal.info/dvn/OAIHandler':'ugrdr', 'http://circle.library.ubc.ca/oai/request':'com_2429_622', 'http://www.polardata.ca/oai/provider':None}
globus_endpoint_api_url = "https://rdmdev1.computecanada.ca/v1/api/collections/25"


def construct_local_url(repository_url, local_identifier):
	local_url = repository_url + " " + local_identifier
	# add rules to derive URLs for dataverse, dspace, islandora, etc.
	return local_url


def sqlite_writer(record, repository_url):
	import sqlite3 as lite

	litecon = lite.connect('data/globus_oai.db')
	with litecon:
		litecur = litecon.cursor()

		litecur.execute("CREATE TABLE IF NOT EXISTS records (title TEXT, date TEXT, local_identifier TEXT, repository_url TEXT)")
		litecur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_repository ON records (local_identifier, repository_url)")
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


def sqlite_reader():
	import sqlite3 as lite
	litecon = lite.connect('data/globus_oai.db')

	# serialize records as JSON
	# might have to switch this to a cursor if it gets too big

	records = litecon.execute("SELECT title, date, local_identifier, repository_url FROM records")

	for record in records:
		record = dict(zip([tuple[0] for tuple in records.description], record))

		with litecon:
			litecon.row_factory = lambda cursor, row: row[0]
			litecur = litecon.cursor()

			# attach the other values to the dict
			# I really should've done this purely in SQL
			# but group_concat() was making me angry

			litecur.execute("SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=0", (record["local_identifier"], record["repository_url"]))
			record["dc.contributor.author"] = litecur.fetchall()

			litecur.execute("SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=1", (record["local_identifier"], record["repository_url"]))
			record["dc.contributor"] = litecur.fetchall()

			litecur.execute("SELECT subject FROM subjects WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
			record["dc.subject"] = litecur.fetchall()

			litecur.execute("SELECT rights FROM rights WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
			record["dc.rights"] = litecur.fetchall()

			litecur.execute("SELECT description FROM descriptions WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
			record["dc.description"] = litecur.fetchall()

			record["dc.source"] = construct_local_url(record["repository_url"], record["local_identifier"])
			record.pop("repository_url", None)
			record.pop("local_identifier", None)

			record["dc.title"] = record["title"]
			record.pop("title", None)
			record["dc.date"] = record["date"]
			record.pop("date", None)


			authheader = "bearer: \'" + access_token + "\'"
			headers = {'content-type': 'application/json', 'authentication': authheader}
			response = requests.post(globus_endpoint_api_url, data=json.dumps(record), headers=headers)

			# did it work?


def unpack_metadata(record, repository_url):
	if 'creator' not in record.keys():
		# if there's no author, probably not a valid record
		return None

	# if date is undefined, as they can be optional in some repos, add an empty key
	if 'date' not in record.keys():
		record["date"] = ['']

	if dbtype == "sqlite":
		sqlite_writer(record, repository_url)
	# elif other dbtypes


def oai_harvest(repository_url, record_set):
	sickle = Sickle(repository_url)

	if record_set is not None:
		records = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True, set=record_set)

	else:
		records = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True)

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


if __name__ == "__main__":

	if len(sys.argv) != 2:
		print("Please specify a database backend as an argument to the script. Currently only sqlite is supported.")
		raise SystemExit

	global dbtype
	dbtype = sys.argv[1]

	if sys.version_info[0] == 2:
		for repository_url, record_set in repositories.iteritems():
			oai_harvest(repository_url, record_set)
	else:
		for repository_url, record_set in repositories.items():
			oai_harvest(repository_url, record_set)

	global access_token
	with open("data/token", "r") as tokenfile:
		jsontoken = json.loads(token.read())
		access_token = jsontoken['access_token'].encode()

	if dbtype == "sqlite":
		sqlite_reader()