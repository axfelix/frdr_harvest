import sys
from sickle import Sickle
import sqlite3 as lite


repositories = {'http://researchdata.sfu.ca/oai2':None, 'http://dataverse.scholarsportal.info/dvn/OAIHandler':'ugrdr', 'http://circle.library.ubc.ca/oai/request':'com_2429_622'}


def sqlite_writer(record, repository_url):

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
			# need to be able to check for deleted records, right now easiest way to do that is just dropping the DB before each harvest :)
			return None	


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

	global dbtype
	dbtype = sys.argv[1]

	for repository_url, record_set in repositories.iteritems():
		oai_harvest(repository_url, record_set)