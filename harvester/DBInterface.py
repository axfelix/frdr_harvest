import os
import time

class DBInterface:
	def __init__(self, params):
		self.dbtype = params.get('type', None)
		self.dbname = params.get('dbname', None)
		self.host = params.get('host', None)
		self.schema = params.get('schema', None)
		self.user = params.get('user', None)
		self.password = params.get('pass', None)
		self.connection = None
		self.logger = None
		if self.dbtype == "sqlite":
			self.dblayer = __import__('sqlite3')
			con = self.getConnection()
			if os.name == "posix":
				try:
					os.chmod(configs['db']['dbname'], 0o664 )
				except:
					pass

		elif self.dbtype == "postgres":
			self.dblayer = __import__('psycopg2')
			con = self.getConnection()

		with con:
			cur = con.cursor()
			cur.execute("create table if not exists creators (creator_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, creator TEXT, is_contributor INTEGER)")
			cur.execute("create table if not exists descriptions (description_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, description TEXT, language TEXT)")
			cur.execute("create table if not exists domain_metadata (metadata_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, field_name TEXT, field_value TEXT)")
			cur.execute("create table if not exists geospatial (geospatial_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, coordinate_type TEXT, lat NUMERIC, lon NUMERIC)")
			cur.execute("""create table if not exists records (record_id INTEGER PRIMARY KEY NOT NULL,repository_id INTEGER NOT NULL,title TEXT,pub_date TEXT,modified_timestamp INTEGER DEFAULT 0,
				source_url TEXT,deleted NUMERIC DEFAULT 0,local_identifier TEXT,series TEXT,contact TEXT)""")
			cur.execute("""create table if not exists repositories (repository_id INTEGER PRIMARY KEY NOT NULL,repository_set TEXT NOT NULL DEFAULT '',repository_url TEXT,repository_name TEXT,
				repository_thumbnail TEXT,repository_type TEXT,last_crawl_timestamp INTEGER,item_url_pattern TEXT,abort_after_numerrors INTEGER,max_records_updated_per_run INTEGER,
				update_log_after_numitems INTEGER,record_refresh_days INTEGER,repo_refresh_days INTEGER,enabled TEXT)""")
			cur.execute("create table if not exists rights (rights_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, rights TEXT)")
			cur.execute("create table if not exists subjects (subject_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, subject TEXT)")
			cur.execute("create table if not exists tags (tag_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, tag TEXT, language TEXT)")
			cur.execute("create index if not exists creators_by_record on creators(record_id)")
			cur.execute("create index if not exists descriptions_by_record on descriptions(record_id,language)")
			cur.execute("create index if not exists tags_by_record on tags(record_id,language)")
			cur.execute("create index if not exists subjects_by_record on subjects(record_id)")
			cur.execute("create index if not exists rights_by_record on rights(record_id)")
			cur.execute("create index if not exists geospatial_by_record on geospatial(record_id)")
			cur.execute("create index if not exists domain_metadata_by_record on domain_metadata(record_id)")
			cur.execute("create unique index if not exists records_by_repository on records (repository_id, local_identifier)")

	def setLogger(self, l):
		self.logger = l

	def getConnection(self):
		if self.connection == None:
			if self.dbtype == "sqlite":
				self.connection = self.dblayer.connect(self.dbname)
			elif self.dbtype == "postgres":
				self.connection = self.dblayer.connect("dbname='%s' user='%s' password='%s' host='%s'" % (self.dbname, self.user, self.password, self.host))
				self.connection.autocommit = True
		return self.connection


	def getRow(self):
		return self.dblayer.Row

	def _prep(self, statement):
		if (self.dbtype == "postgres"):
			return statement.replace('?', '%s')
		return statement


	def update_repo(self,repo_id,repo_url,repo_set,repo_name,repo_type,enabled,repo_thumbnail,item_url_pattern,abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days):
		con = self.getConnection()
		records=[]
		with con:
			if self.dbtype == "sqlite":
				con.row_factory = self.getRow()
				cur = con.cursor()
			if self.dbtype == "postgres":
				from psycopg2.extras import RealDictCursor
				cur = con.cursor(cursor_factory = RealDictCursor)
			if repo_id > 0:
				# Existing repo
				try:
					self.logger.debug("This repo already exists in the database; updating")
					cur.execute(self._prep("""UPDATE repositories 
						set repository_url=?, repository_set=?, repository_name=?, repository_type=?, repository_thumbnail=?, last_crawl_timestamp=?, item_url_pattern=?,enabled=?,
						abort_after_numerrors=?,max_records_updated_per_run=?,update_log_after_numitems=?,record_refresh_days=?,repo_refresh_days=?
						WHERE repository_id=?"""), (repo_url, repo_set, repo_name, repo_type, repo_thumbnail, time.time(), item_url_pattern, enabled, 
						abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days,repo_id) )
				except self.dblayer.IntegrityError:
					# record already present in repo
					return repo_id
			else:
				# Create new repo record
				try:
					self.logger.debug("This repo did not exist in the database; adding")
					if self.dbtype == "postgres":
						cur.execute(self._prep("""INSERT INTO repositories 
							(repository_url, repository_set, repository_name, repository_type, repository_thumbnail, last_crawl_timestamp, item_url_pattern, enabled,
							abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days) 
							VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING repository_id"""), (repo_url, repo_set, repo_name, repo_type, repo_thumbnail, time.time(), item_url_pattern, enabled,
							abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days))
						res = cur.fetchone()
						repo_id = int(res[0])

					if self.dbtype == "sqlite":
						cur.execute(self._prep("""INSERT INTO repositories 
							(repository_url, repository_set, repository_name, repository_type, repository_thumbnail, last_crawl_timestamp, item_url_pattern, enabled,
							abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days) 
							VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"""), (repo_url, repo_set, repo_name, repo_type, repo_thumbnail, time.time(), item_url_pattern, enabled,
							abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days))
						repo_id = int(cur.lastrowid)

				except self.dblayer.IntegrityError:
					# record already present in repo
					return repo_id

		return repo_id

	def get_repo_id(self, repo_url, repo_set):
		returnvalue = 0
		con = self.getConnection()
		with con:
			if self.dbtype == "sqlite":
				con.row_factory = self.getRow()
			cur = con.cursor()
			if self.dbtype == "postgres":
				from psycopg2.extras import RealDictCursor
				cur = con.cursor(cursor_factory = RealDictCursor)
			cur.execute(self._prep("select repository_id from repositories where repository_url = ? and repository_set = ?"), (repo_url,repo_set) )
			if cur is not None:
				records = cur.fetchall()
			else:
				return 0
			for record in records:
				returnvalue = int(record['repository_id'])
		return returnvalue

	def get_repo_last_crawl(self, repo_id):
		returnvalue = 0
		if repo_id == 0 or repo_id is None:
			return 0
		con = self.getConnection()
		with con:
			if self.dbtype == "sqlite":
				con.row_factory = self.getRow()
			cur = con.cursor()
			if self.dbtype == "postgres":
				from psycopg2.extras import RealDictCursor
				cur = con.cursor(cursor_factory = RealDictCursor)
			cur.execute(self._prep("select last_crawl_timestamp from repositories where repository_id = ?"), (repo_id,) )
			if cur is not None:
				records = cur.fetchall()
			else:
				return 0
			for record in records:
				returnvalue = int(record['last_crawl_timestamp'])
		self.logger.debug("Last crawl ts for repo_id %s is %s" % (repo_id, returnvalue))
		return returnvalue


	def update_last_crawl(self, repo_id):
		con = self.getConnection()
		with con:
			cur = con.cursor()
			cur.execute(self._prep("update repositories set last_crawl_timestamp = ? where repository_id = ?"), (int(time.time()),repo_id) )


	def delete_record(self, record):
		con = self.getConnection()
		if record['record_id'] == 0:
			return False
		with con:
			cur = con.cursor()

			try:
				cur.execute(self._prep("UPDATE records set deleted = 1, modified_timestamp = ? where record_id=?"), (time.time(), record['record_id']) )
			except:
				self.logger.error("Unable to mark as deleted record %s" % (record['local_identifier'],) )
				return False

			try:
				cur.execute(self._prep("DELETE from creators where record_id=?"), (record['record_id'],))
				cur.execute(self._prep("DELETE from subjects where record_id=?"), (record['record_id'],))
				cur.execute(self._prep("DELETE from rights where record_id=?"), (record['record_id'],))
				cur.execute(self._prep("DELETE from descriptions where record_id=?"), (record['record_id'],))
				cur.execute(self._prep("DELETE from tags where record_id=?"), (record['record_id'],))
				cur.execute(self._prep("DELETE from geospatial where record_id=?"), (record['record_id'],))
				cur.execute(self._prep("DELETE from domain_metadata where record_id=?"), (record['record_id'],))
			except:
				self.logger.error("Unable to delete related table rows for record %s" % (record['local_identifier'],) )
				return False

		self.logger.debug("Marked as deleted: record %s" % (record['local_identifier'],) )
		return True

	def get_record_id(self, repo_id, local_identifier):
		returnvalue = None
		con = self.getConnection()
		with con:
			if self.dbtype == "sqlite":
				con.row_factory = self.getRow()
			cur = con.cursor()
			if self.dbtype == "postgres":
				from psycopg2.extras import RealDictCursor
				cur = con.cursor(cursor_factory = RealDictCursor)
			cur.execute(self._prep("select record_id from records where local_identifier=? and repository_id = ?"),(local_identifier, repo_id))
			if cur is not None:
				records = cur.fetchall()
			else:
				return None
			for record in records:
				returnvalue = int(record['record_id'])
		return returnvalue		

	def write_record(self, record, repo_id, metadata_prefix, domain_metadata, mode = "insert"):
		if record == None:
			return None
		record["record_id"] = self.get_record_id(repo_id, record["identifier"])

		con = self.getConnection()
		with con:
			cur = con.cursor()
			source_url = ""
			if 'dc:source' in record:
				source_url = record["dc:source"]
			if record["record_id"] is None:
				try:
					if self.dbtype == "postgres":
						cur.execute(self._prep("INSERT INTO records (title, pub_date, contact, series, modified_timestamp, source_url, deleted, local_identifier, repository_id) VALUES(?,?,?,?,?,?,?,?,?) RETURNING record_id"), 
							(record["title"], record["pub_date"], record["contact"], record["series"], time.time(), source_url, 0, record["identifier"], repo_id))
						res = cur.fetchone()
						record["record_id"] = int(res[0])
					if self.dbtype == "sqlite":
						cur.execute(self._prep("INSERT INTO records (title, pub_date, contact, series, modified_timestamp, source_url, deleted, local_identifier, repository_id) VALUES(?,?,?,?,?,?,?,?,?)"), 
							(record["title"], record["pub_date"], record["contact"], record["series"], time.time(), source_url, 0, record["identifier"], repo_id))
						record["record_id"] = int(cur.lastrowid)
				except self.dblayer.IntegrityError:
					return None
			else:
				cur.execute(self._prep("UPDATE records set title=?, pub_date=?, contact=?, series=?, modified_timestamp=?, source_url=?, deleted=?, local_identifier=? WHERE record_id = ?"),
					(record["title"], record["pub_date"], record["contact"], record["series"], time.time(), source_url, 0, record["identifier"], record["record_id"]) )

			if record["record_id"] is None:
				return None

			if "creator" in record:
				if not isinstance(record["creator"], list):
					record["creator"] = [record["creator"]]
				for creator in record["creator"]:
					try:
						cur.execute(self._prep("DELETE from creators where record_id = ? and is_contributor=0"), (record["record_id"],))
						cur.execute(self._prep("INSERT INTO creators (record_id, creator, is_contributor) VALUES (?,?,?)"), (record["record_id"], creator, 0))
					except self.dblayer.IntegrityError:
						pass

			if "contributor" in record:
				if not isinstance(record["contributor"], list):
					record["contributor"] = [record["contributor"]]
				for creator in record["contributor"]:
					try:
						cur.execute(self._prep("DELETE from creators where record_id = ? and is_contributor=1"), (record["record_id"],))
						cur.execute(self._prep("INSERT INTO creators (record_id, creator, is_contributor) VALUES (?,?,?)"), (record["record_id"], creator, 1))
					except self.dblayer.IntegrityError:
						pass

			if "subject" in record:
				if not isinstance(record["subject"], list):
					record["subject"] = [record["subject"]]
				for subject in record["subject"]:
					try:
						cur.execute(self._prep("DELETE from subjects where record_id = ?"), (record["record_id"],))
						if subject is not None and len(subject) > 0:
							cur.execute(self._prep("INSERT INTO subjects (record_id, subject) VALUES (?,?)"), (record["record_id"], subject))
					except self.dblayer.IntegrityError:
						pass

			if "rights" in record:
				if not isinstance(record["rights"], list):
					record["rights"] = [record["rights"]]
				for rights in record["rights"]:
					try:
						cur.execute(self._prep("DELETE from rights where record_id = ?"), (record["record_id"],))
						cur.execute(self._prep("INSERT INTO rights (record_id, rights) VALUES (?,?)"), (record["record_id"], rights))
					except self.dblayer.IntegrityError:
						pass

			if "description" in record:
				if not isinstance(record["description"], list):
					record["description"] = [record["description"]]
				for description in record["description"]:
					try:
						cur.execute(self._prep("DELETE from descriptions where record_id = ? and language='en' "), (record["record_id"],))
						cur.execute(self._prep("INSERT INTO descriptions (record_id, description, language) VALUES (?,?,?)"), (record["record_id"], description, 'en'))
					except self.dblayer.IntegrityError:
						pass

			if "description_fr" in record:
				if not isinstance(record["description_fr"], list):
					record["description_fr"] = [record["description_fr"]]
				for description_fr in record["description_fr"]:
					try:
						cur.execute(self._prep("DELETE from descriptions where record_id = ? and language='fr' "), (record["record_id"],))
						cur.execute(self._prep("INSERT INTO descriptions (record_id, description, language) VALUES (?,?,?)"), (record["record_id"], description_fr, 'fr'))
					except self.dblayer.IntegrityError:
						pass

			if "tags" in record:
				if not isinstance(record["tags"], list):
					record["tags"] = [record["tags"]]
				for tag in record["tags"]:
					try:
						cur.execute(self._prep("DELETE from tags where record_id = ? and language='en' "), (record["record_id"],))
						cur.execute(self._prep("INSERT INTO tags (record_id, tag, language) VALUES (?,?,?)"), (record["record_id"], tag, "en"))
					except self.dblayer.IntegrityError:
						pass

			if "tags_fr" in record:
				if not isinstance(record["tags_fr"], list):
					record["tags_fr"] = [record["tags_fr"]]
				for tag_fr in record["tags_fr"]:
					try:
						cur.execute(self._prep("DELETE from tags where record_id = ? and language='fr' "), (record["record_id"],))
						cur.execute(self._prep("INSERT INTO tags (record_id, tag, language) VALUES (?,?,?)"), (record["record_id"], tag_fr, "fr"))
					except self.dblayer.IntegrityError:
						pass

			if "geospatial" in record:
				for coordinates in record["geospatial"]["coordinates"][0]:
					try:
						cur.execute(self._prep("DELETE from geospatial where record_id = ?"), (record["record_id"],))
						cur.execute(self._prep("INSERT INTO geospatial (record_id, coordinate_type, lat, lon) VALUES (?,?,?,?)"), (record["record_id"], record["geospatial"]["type"], coordinates[0], coordinates[1]))
					except self.dblayer.IntegrityError:
						pass

			for domain_metadata_element in domain_metadata:
				if domain_metadata_element in record:
					if not isinstance(record[domain_metadata_element], list):
						record[domain_metadata_element] = [record[domain_metadata_element]]
					namespaced_element = metadata_prefix + ":" + domain_metadata_element
					for domain_metadata_element_value in record[domain_metadata_element]:
						try:
							cur.execute(self._prep("INSERT INTO domain_metadata (record_id, field_name, field_value) VALUES (?,?,?)"), (record["record_id"], namespaced_element, domain_metadata_element_value))
						except self.dblayer.IntegrityError:
							pass

		return None

	def get_stale_records(self,stale_timestamp,repo_id, max_records_updated_per_run):
		con = self.getConnection()
		records = []
		with con:
			if self.dbtype == "sqlite":
				con.row_factory = self.getRow()
				cur = con.cursor()
			if self.dbtype == "postgres":
				from psycopg2.extras import RealDictCursor
				cur = con.cursor(cursor_factory = RealDictCursor)
			cur.execute(self._prep("""SELECT recs.record_id, recs.title, recs.pub_date, recs.contact, recs.series, recs.modified_timestamp, recs.local_identifier, 
				repos.repository_id, repos.repository_type
				FROM records recs, repositories repos
				where recs.repository_id = repos.repository_id and recs.modified_timestamp < ? and repos.repository_id = ?
				LIMIT ?"""), (stale_timestamp,repo_id, max_records_updated_per_run))
			if cur is not None:
				records = cur.fetchall()
		return records


	def touch_record(self, record):
		con = self.getConnection()
		with con:
			cur = con.cursor()
			try:
				cur.execute(self._prep("UPDATE records set modified_timestamp = ? where record_id = ?"), (time.time(), record['record_id']) )
			except:
				self.logger.error("Unable to update modified_timestamp for record id %s" % (record['record_id'],) )
				return False

		return True


	def write_header(self, local_identifier, repo_id):
		con = self.getConnection()
		with con:
			cur = con.cursor()

			try:
				cur.execute(self._prep("INSERT INTO records (title, pub_date, contact, series, modified_timestamp, local_identifier, repository_id) VALUES(?,?,?,?,?,?,?)"), ("", "", "", "", 0, local_identifier, repo_id))
			except self.dblayer.IntegrityError:
				# record already present in repo
				return None

		return None
