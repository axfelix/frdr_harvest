import time
import re
from harvester.TimeFormatter import TimeFormatter

class HarvestRepository(object):
	""" Top level representation of a repository """

	def __init__(self, globalParams):
		self.__dict__.update({
			'url': None,
			'type': None,
			'name': None,
			'set': None,
			'thumbnail': None,
			'abort_after_numerrors': 5,
			'max_records_updated_per_run': 100,
			'update_log_after_numitems': 100,
			'record_refresh_days': 30,
			'repo_refresh_days': 7,
			'item_url_pattern': None,
			'enabled': False,
			'formatter': TimeFormatter(),
			'error_count': 0,
			'db': None,
			'logger': None
		})

		# Inherit global config
		self.__dict__.update(globalParams)

	def setRepoParams(self, repoParams):
		""" Set local repo params and let them override the global config """
		self.__dict__.update(repoParams)

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
			self.logger.info("*** Repo: %s, type: %s, (last harvested: never)" % (self.name, self.type ) )
		else:
			self.logger.info("*** Repo: %s, type: %s, (last harvested: %s ago)" % (self.name, self.type, self.formatter.humanize(self.tstart - self.last_crawl) ) )

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


	def update_stale_records(self, dbparams):
		""" This method will be called by a child class only, so that it uses its own _update_record() method """
		if self.enabled != True:
			return True
		if self.db == None:
			self.logger.error("Database configuration is not complete")
			return False
		record_count = 0
		tstart = time.time()
		self.logger.info("Looking for stale records to update")
		stale_timestamp = int(time.time() - self.record_refresh_days*86400)
		self.dbtype = dbparams.get('type', None)

		records = self.db.get_stale_records(stale_timestamp,self.url, self.max_records_updated_per_run)
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
