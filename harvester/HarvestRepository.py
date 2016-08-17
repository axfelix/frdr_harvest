import time
import re
from harvester.TimeFormatter import TimeFormatter

class HarvestRepository(object):
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

