from harvester.HarvestRepository import HarvestRepository
from functools import wraps
from owslib.csw import CatalogueServiceWeb
import time
import json
import re
import os.path


class CSWRepository(HarvestRepository):
	""" CSW Repository """

	def setRepoParams(self, repoParams):
		self.metadataprefix = "csw"
		super(CSWRepository, self).setRepoParams(repoParams)
		self.cswrepo = CatalogueServiceWeb(self.url)
		self.domain_metadata = []


	def _crawl(self):
		kwargs = {
			"repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name, "repo_type": "csw", 
			"enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
			"abort_after_numerrors": self.abort_after_numerrors, "max_records_updated_per_run": self.max_records_updated_per_run,
			"update_log_after_numitems": self.update_log_after_numitems, "record_refresh_days": self.record_refresh_days,
			"repo_refresh_days": self.repo_refresh_days
		}
		self.repository_id = self.db.update_repo(**kwargs)
		self.cswrepo.getrecords2()

		item_count = 0
		for rec in csw.records:
			result = self.db.write_header(csw.records[rec].identifier, self.repository_id)
			item_count = item_count + 1
			if (item_count % self.update_log_after_numitems == 0):
				tdelta = time.time() - self.tstart + 0.1
				self.logger.info("Done %s item headers after %s (%.1f items/sec)" % (item_count, self.formatter.humanize(tdelta), item_count/tdelta) )
			if item_count % csw.results['returned'] and csw.results['nextrecord'] != 0:
				csw.getrecords2(startposition=csw.results['nextrecord'])

		self.logger.info("Found %s items in feed" % (item_count) )

	def format_csw_to_oai(self, csw_record, local_identifier):
		record = {}

		record["title"] = csw_record.title
		record["description"] = csw_record.abstract
		record["tags"] = csw_record.subjects
		record["identifier"] = local_identifier
		record["creator"] = self.name
		record["contact"] = self.contact

		return record

	def _rate_limited(max_per_second):
		""" Decorator that make functions not be called faster than a set rate """
		threading = __import__('threading')
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

	@_rate_limited(5)
	def _update_record(self,record):

		self.cswrepo.action.getrecordbyid(id=[record['local_identifier']])
		if csw.records:
			csw_record = csw.records[record['local_identifier']]
			oai_record = self.format_csw_to_oai(csw_record,record['local_identifier'])
			csw.getrecordbyid(id=[record['local_identifier']], outputschema="http://www.isotc211.org/2005/gmd")
			oai_record["pub_date"] = csw.records[record['local_identifier']].datestamp
			if oai_record:
				self.db.write_record(oai_record, self.repository_id, self.metadataprefix.lower(), self.domain_metadata)
			return True

		else:
			# This record was deleted
			self.db.delete_record(record)
			return True

		return False

