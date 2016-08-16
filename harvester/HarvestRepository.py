import time
import re
from harvester.TimeFormatter import TimeFormatter

class HarvestRepository:
	""" Top level representation of a repository """

	def __init__(self, params):
		self.url = params.get('url', None)
		self.type = params.get('type', None)
		self.name = params.get('name', None)
		self.set = params.get('set', None)
		self.thumbnail = params.get('thumbnail', None)
		self.update_log_after_numitems = params.get('update_log_after_numitems', None)
		self.item_url_pattern = params.get('item_url_pattern', None)
		self.enabled = params.get('enabled', True)
		self.formatter = TimeFormatter()
		self.error_count = 0
		self.db = None
		self.logger = None

	def setDefaults(self, defaults):
		""" Inherit global configs, but they do not overwrite local configs """
		for k,v in defaults.items():
			if isinstance(k, str) or isinstance(k, unicode):
				if k in self.__dict__:
					if not self.__dict__[k]:
						self.__dict__[k] = v
				else:
					self.__dict__[k] = v

	def setLogger(self, l):
		self.logger = l

	def setDatabase(self, d):
		self.db = d

	def setFormatter(self, f):
		self.formatter = f

	def crawl(self):
		self.tstart = time.time()
		self.last_crawl = self.db.get_repo_last_crawl(self.url)

		if self.last_crawl == 0:
			self.logger.info("Repo: %s, type: %s, (last harvested: never)" % (self.name, self.type ) )
		else:
			self.logger.info("Repo: %s, type: %s, (last harvested: %s ago)" % (self.name, self.type, self.formatter.humanize(self.tstart - self.last_crawl) ) )

		if (self.enabled):
			if (self.last_crawl + self.repo_refresh_days*86400) < self.tstart:
				self._crawl()
				self.db.update_last_crawl(self.url)
			else:
				self.logger.info("This repo is not yet due to be harvested")
		else:
			self.logger.info("This repo is not enabled for harvesting")


	def construct_local_url(self, record):
		# Check if the local_identifier has already been turned into a url
		if "http" in record["local_identifier"].lower():
			return record["local_identifier"]

		# Check for OAI format of identifier (oai:domain:id)
		oai_id = None
		oai_search = re.search("oai:(.+):(.+)", record["local_identifier"])
		if oai_search:
			oai_id = oai_search.group(2)
			# TODO: determine if this is needed for all repos, or just SFU?
			oai_id = oai_id.replace("_",":")

		if self.item_url_pattern:
			if oai_id:
				local_url = re.sub("(\%id\%)", oai_id, self.item_url_pattern)
			else:
				local_url = re.sub("(\%id\%)", record["local_identifier"], self.item_url_pattern)
			return local_url

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


	def read_gmeta(self):
		con = self.db.getConnection()
		gmeta = []

		# Only select records that have complete data
		records = con.execute("""SELECT recs.title, recs.date, recs.contact, recs.series, recs.source_url, recs.deleted, recs.local_identifier, recs.repository_url, repos.repository_name as "nrdr:origin.id", repos.repository_thumbnail as "nrdr:origin.icon"
				FROM records recs, repositories repos 
				WHERE recs.title != '' and recs.repository_url = repos.repository_url """)

		for record in records:
			record = dict(zip([tuple[0] for tuple in records.description], record))
			record["dc:source"] = self.construct_local_url(record)
			if record["dc:source"] is None:
				continue

			if record["deleted"] == 1:
				gmeta_data = {record["dc:source"] : {"mimetype": "application/json", "content": None}}
				gmeta.append(gmeta_data)
				continue

			with con:
				con.row_factory = lambda cursor, row: row[0]
				litecur = con.cursor()

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

				litecur.execute("SELECT description FROM fra_descriptions WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["nrdr:fra_description"] = litecur.fetchall()

				litecur.execute("SELECT tag FROM tags WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["nrdr:tags"] = litecur.fetchall()

				litecur.execute("SELECT coordinate_type, lat, lon FROM geospatial WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["nrdr:geospatial"] = litecur.fetchall()

				record.pop("repository_url", None)
				record.pop("local_identifier", None)

			record["dc:title"] = record["title"]
			record.pop("title", None)
			record["dc:date"] = record["date"]
			record.pop("date", None)
			record["nrdr:contact"] = record["contact"]
			record.pop("contact", None)
			record["nrdr:series"] = record["series"]
			record.pop("series", None)
			record.pop("source_url", None)
			record.pop("deleted", None)

			record["@context"] = {"dc" : "http://dublincore.org/documents/dcmi-terms", "nrdr" : "http://nrdr-ednr.ca/schema/1.0/"}
			gmeta_data = {record["dc:source"] : {"mimetype": "application/json", "content": record}}
			gmeta.append(gmeta_data)

		return gmeta

	def _update_record(self, record):
		""" This method to be overridden """
		return True

	def update_stale_records(self):
		""" This method will be called by a child class only, so that it uses its own _update_record() method """
		record_count = 0
		tstart = time.time()
		self.logger.info("Looking for stale records to update")
		stale_timestamp = int(time.time() - self.record_refresh_days*86400)

		recordset = []
		con = self.db.getConnection()
		with con:
			con.row_factory = self.db.getRow()
			cur = con.cursor()
			records = cur.execute("""SELECT recs.title, recs.date, recs.contact, recs.series, recs.modified_timestamp, recs.local_identifier, recs.repository_url, repos.repository_type
				FROM records recs, repositories repos 
				where recs.repository_url = repos.repository_url and recs.modified_timestamp < ? and repos.repository_url = ?
				LIMIT ?""", (stale_timestamp,self.url, self.max_records_updated_per_run)).fetchall()

			for record in records:
				if record_count == 0:
					self.logger.info("Started processing for %d records" % (len(records)) )

				status = self._update_record(record)
				if not status:
					self.logger.error("Aborting due to errors after %s items updated in %s (%.1f items/sec)" % (record_count, self.formatter.humanize(time.time() - tstart), record_count/(time.time() - tstart + 0.1)))
					break

				record_count = record_count + 1
				if (record_count % self.update_log_after_numitems == 0):
					tdelta = time.time() - tstart + 0.1
					self.logger.info("Done %s items after %s (%.1f items/sec)" % (record_count, self.formatter.humanize(tdelta), (record_count/tdelta)))

		self.logger.info("Updated %s items in %s (%.1f items/sec)" % (record_count, self.formatter.humanize(time.time() - tstart),record_count/(time.time() - tstart + 0.1)))

