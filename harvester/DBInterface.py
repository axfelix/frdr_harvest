import os
import time

class DBInterface:
	def __init__(self, params):
		self.dbtype = params.get('type', None)
		self.filename = params.get('filename', None)
		self.host = params.get('host', None)
		self.schema = params.get('schema', None)
		self.user = params.get('user', None)
		self.password = params.get('pass', None)
		self.connection = None
		self.logger = None
		if self.dbtype == "sqlite":
			self.sqlite3 = __import__('sqlite3')

			con = self.getConnection()
			with con:
				cur = con.cursor()
				cur.execute("CREATE TABLE IF NOT EXISTS records (title TEXT, date TEXT, contact TEXT, series TEXT, modified_timestamp NUMERIC DEFAULT 0, source_url TEXT, deleted NUMERIC DEFAULT 0, local_identifier TEXT, repository_url TEXT, PRIMARY KEY (local_identifier, repository_url)) WITHOUT ROWID")

				cur.execute("CREATE TABLE IF NOT EXISTS creators (local_identifier TEXT, repository_url TEXT, creator TEXT, is_contributor INTEGER)")
				cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_creator ON creators (local_identifier, repository_url, creator)")

				cur.execute("CREATE TABLE IF NOT EXISTS subjects (local_identifier TEXT, repository_url TEXT, subject TEXT)")
				cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_subject ON subjects (local_identifier, repository_url, subject)")

				cur.execute("CREATE TABLE IF NOT EXISTS rights (local_identifier TEXT, repository_url TEXT, rights TEXT)")
				cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_rights ON rights (local_identifier, repository_url, rights)")

				cur.execute("CREATE TABLE IF NOT EXISTS descriptions (local_identifier TEXT, repository_url TEXT, description TEXT)")
				cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_description ON descriptions (local_identifier, repository_url, description)")

				cur.execute("CREATE TABLE IF NOT EXISTS fra_descriptions (local_identifier TEXT, repository_url TEXT, description TEXT)")
				cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_description ON fra_descriptions (local_identifier, repository_url, description)")

				cur.execute("CREATE TABLE IF NOT EXISTS tags (local_identifier TEXT, repository_url TEXT, tag TEXT)")
				cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_tag ON tags (local_identifier, repository_url, tag)")

				cur.execute("CREATE TABLE IF NOT EXISTS geospatial (local_identifier TEXT, repository_url TEXT, coordinate_type TEXT, lat NUMERIC, lon NUMERIC)")
				cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS identifier_plus_lat_lon ON geospatial (local_identifier, repository_url, lat, lon)")

				cur.execute("CREATE TABLE IF NOT EXISTS repositories (repository_url TEXT, repository_name TEXT, repository_thumbnail TEXT, repository_type TEXT, last_crawl_timestamp NUMERIC, item_url_pattern TEXT, PRIMARY KEY (repository_url)) WITHOUT ROWID")
			if os.name == "posix":
				try:
					os.chmod(configs['db']['filename'], 0o664 )
				except:
					pass

	def setLogger(self, l):
		self.logger = l

	def getConnection(self):
		if self.connection == None:
			if self.dbtype == "sqlite":
				self.connection = self.sqlite3.connect(self.filename)
				return self.connection
		else:
			return self.connection


	def getRow(self):
		if self.dbtype == "sqlite":
			return self.sqlite3.Row


	def create_repo(self, repository_url, repository_name, repository_type, repository_thumbnail="", item_url_pattern=""):
		con = self.getConnection()
		with con:
			cur = con.cursor()

			try:
				cur.execute("INSERT OR REPLACE INTO repositories (repository_url, repository_name, repository_type, repository_thumbnail, last_crawl_timestamp, item_url_pattern) VALUES (?,?,?,?,?,?)", (repository_url, repository_name, repository_type, repository_thumbnail, time.time(), item_url_pattern))
			except self.sqlite3.IntegrityError:
				# record already present in repo
				return None


	def get_repo_last_crawl(self, repo_url):
		returnvalue = False
		con = self.getConnection()
		with con:
			con.row_factory = self.getRow()
			litecur = con.cursor()
			records = litecur.execute("select last_crawl_timestamp from repositories where repository_url = ?",(repo_url,) ).fetchall()
			for record in records:
				returnvalue = record['last_crawl_timestamp']
		return returnvalue


	def update_last_crawl(self, repo_url):
		con = self.getConnection()
		with con:
			cur = con.cursor()
			cur.execute("update repositories set last_crawl_timestamp = ? where repository_url = ?",(int(time.time()),repo_url))


	def delete_record(self, record):
		con = self.getConnection()
		with con:
			cur = con.cursor()

			try:
				cur.execute("UPDATE records set deleted = 1, modified_timestamp = ? where local_identifier = ? and repository_url = ?", (time.time(), record['local_identifier'], record['repository_url']))
			except:
				self.logger.error("Unable to mark as deleted record %s in repository %s" % (record['local_identifier'], record['repository_url'] ) )
				return False

			try:
				cur.execute("DELETE from creators where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
				cur.execute("DELETE from subjects where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
				cur.execute("DELETE from rights where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
				cur.execute("DELETE from descriptions where local_identifier = ? and repository_url = ?", (record['local_identifier'], record['repository_url']))
			except:
				self.logger.error("Unable to delete related table rows for record %s in repository %s" % (record['local_identifier'], record['repository_url'] ) )
				return False

		self.logger.debug("Marked as deleted: record %s in repository %s" % (record['local_identifier'], record['repository_url'] ) )
		return True


	def write_record(self, record, repository_url, mode = "insert"):
		if record == None:
			return None

		con = self.getConnection()
		with con:
			cur = con.cursor()
			verb = "INSERT"
			if mode == "replace":
				verb = "REPLACE"

			try:
				if 'dc:source' in record:
					cur.execute(verb + " INTO records (title, date, contact, series, modified_timestamp, source_url, deleted, local_identifier, repository_url) VALUES(?,?,?,?,?,?,?,?,?)", (record["title"], record["date"], record["contact"], record["series"], time.time(), record["dc:source"], 0, record["identifier"], repository_url))				
				else:
					cur.execute(verb + " INTO records (title, date, contact, series, modified_timestamp, deleted, local_identifier, repository_url) VALUES(?,?,?,?,?,?,?,?)", (record["title"], record["date"], record["contact"], record["series"], time.time(), 0, record["identifier"], repository_url))				
			except self.sqlite3.IntegrityError:
				# record already present in repo
				return None
		
			if "creator" in record:
				if not isinstance(record["creator"], list):
					record["creator"] = [record["creator"]]
				for creator in record["creator"]:
					try:
						cur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"], repository_url, creator, 0))
					except self.sqlite3.IntegrityError:
						pass

			if "contributor" in record:
				if not isinstance(record["contributor"], list):
					record["contributor"] = [record["contributor"]]
				for contributor in record["contributor"]:
					try:
						cur.execute("INSERT INTO creators (local_identifier, repository_url, creator, is_contributor) VALUES (?,?,?,?)", (record["identifier"], repository_url, contributor, 1))
					except self.sqlite3.IntegrityError:
						pass

			if "subject" in record:
				if not isinstance(record["subject"], list):
					record["subject"] = [record["subject"]]
				for subject in record["subject"]:
					try:
						if len(subject) > 0:
							cur.execute("INSERT INTO subjects (local_identifier, repository_url, subject) VALUES (?,?,?)", (record["identifier"], repository_url, subject))
					except self.sqlite3.IntegrityError:
						pass

			if "rights" in record:
				if not isinstance(record["rights"], list):
					record["rights"] = [record["rights"]]
				for rights in record["rights"]:
					try:
						cur.execute("INSERT INTO rights (local_identifier, repository_url, rights) VALUES (?,?,?)", (record["identifier"], repository_url, rights))
					except self.sqlite3.IntegrityError:
						pass

			if "description" in record:
				if not isinstance(record["description"], list):
					record["description"] = [record["description"]]
				for description in record["description"]:
					try:
						cur.execute("INSERT INTO descriptions (local_identifier, repository_url, description) VALUES (?,?,?)", (record["identifier"], repository_url, description))
					except self.sqlite3.IntegrityError:
						pass

			if "fra_description" in record:
				if not isinstance(record["fra_description"], list):
					record["fra_description"] = [record["fra_description"]]
				for fra_description in record["fra_description"]:
					try:
						cur.execute("INSERT INTO fra_descriptions (local_identifier, repository_url, description) VALUES (?,?,?)", (record["identifier"], repository_url, fra_description))
					except self.sqlite3.IntegrityError:
						pass

			if "tags" in record:
				if not isinstance(record["tags"], list):
					record["tags"] = [record["tags"]]
				for tag in record["tags"]:
					try:
						cur.execute("INSERT INTO tags (local_identifier, repository_url, tag) VALUES (?,?,?)", (record["identifier"], repository_url, tag))
					except self.sqlite3.IntegrityError:
						pass

			if "geospatial" in record:
				for coordinates in record["geospatial"]["coordinates"][0]:
					try:
						cur.execute("INSERT INTO geospatial (local_identifier, repository_url, coordinate_type, lat, lon) VALUES (?,?,?,?,?)", (record["identifier"], repository_url, record["geospatial"]["type"], coordinates[0], coordinates[1]))
					except self.sqlite3.IntegrityError:
						pass

		return record["identifier"]


	def touch_record(self, record):
		con = self.getConnection()
		with con:
			cur = con.cursor()
			try:
				cur.execute("UPDATE records set modified_timestamp = ? where local_identifier = ? and repository_url = ?", (time.time(), record['local_identifier'], record['repository_url']))
			except:
				self.logger.error("Unable to update modified_timestamp for record %s in repository %s" % (record['local_identifier'], record['repository_url'] ) )
				return False

		return True


	def write_header(self, record_id, repository_url):
		con = self.getConnection()
		with con:
			cur = con.cursor()

			try:
				cur.execute("INSERT INTO records (title, date, contact, series, modified_timestamp, local_identifier, repository_url) VALUES(?,?,?,?,?,?,?)", ("", "", "", "", 0, record_id, repository_url))
			except self.sqlite3.IntegrityError:
				# record already present in repo
				return None

		return record_id

