from harvester.HarvestRepository import HarvestRepository
from functools import wraps
import ckanapi
import time
import json
import re

class CKANRepository(HarvestRepository):
	""" CKAN Repository """

	def __init__(self, params):
		super(CKANRepository, self).__init__(params)
		self.ckanrepo = ckanapi.RemoteCKAN(self.url)
		
	def _crawl(self):
		self.db.create_repo(self.url, self.name, "ckan", self.thumbnail, self.item_url_pattern)
		records = self.ckanrepo.action.package_list()

		item_existing_count = 0
		item_new_count = 0
		for record_id in records:
			result = self.db.write_header(record_id, self.url)
			if result == None:
				item_existing_count = item_existing_count + 1
			else:
				item_new_count = item_new_count + 1
			if ((item_existing_count + item_new_count) % self.update_log_after_numitems == 0):
				tdelta = time.time() - self.tstart + 0.1
				self.logger.info("Done %s item headers after %s (%.1f items/sec)" % ((item_existing_count + item_new_count), self.formatter.humanize(tdelta), ((item_existing_count + item_new_count)/tdelta)) )

		self.logger.info("Found %s items in feed (%d existing, %d new)" % ((item_existing_count + item_new_count), item_existing_count, item_new_count) )

	def format_ckan_to_oai(self,ckan_record, local_identifier):
		record = {}

		if not 'date_published' in ckan_record:
			return None

		if ('author' in ckan_record) and ckan_record['author']:
			record["creator"] = ckan_record['author']
		elif ('maintainer' in ckan_record) and ckan_record['maintainer']:
			record["creator"] = ckan_record['maintainer']
		else:
			record["creator"] = ckan_record['organization']['title']

		record["identifier"] = local_identifier
		record["title"] = ckan_record.get("title", "")
		record["description"] = ckan_record.get("notes", "")
		record["fra_description"] = ckan_record.get("notes_fra", "")
		record["subject"] = ckan_record['subject']

		# Open Data Canada API now returns a mangled unicode-escaped-keyed-dict-as-string; regex is the only solution
		record["dc:source"] = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?", ckan_record["url"]).group(0)

		record["rights"] = [ckan_record['license_title']]
		record["rights"].append(ckan_record.get("license_url", ""))
		record["rights"].append(ckan_record.get("attribution", ""))
		record["rights"] = " - ".join(record["rights"])

		# Some CKAN records have a trailing null timestamp after date
		record["date"] = re.sub(" 00:00:00", "", ckan_record['date_published'])

		record["contact"] = ckan_record.get("author_email", ckan_record.get("maintainer_email", ""))

		try: 
			record["series"] = ckan_record["data_series_name"]["en"]
		except:
			record["series"] = ckan_record.get("data_series_name", "")

		record["tags"] = []
		for tag in ckan_record["tags"]:
			record["tags"].append(tag["display_name"])

		if ('geometry' in ckan_record) and ckan_record['geometry']:
			record["geospatial"] = ckan_record['geometry']
		elif ('spatial' in ckan_record) and ckan_record['spatial']:
			record["geospatial"] = json.loads(ckan_record["spatial"])

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
		self.logger.debug("Updating CKAN record %s from repo at %s" % (record['local_identifier'],self.url) )

		try:
			ckan_record = self.ckanrepo.action.package_show(id=record['local_identifier'])
			oai_record = self.format_ckan_to_oai(ckan_record,record['local_identifier'])
			if oai_record:
				self.db.write_record(oai_record, self.url,"replace")
			return True

		except ckanapi.errors.NotAuthorized:
			# Not authorized means that we currently do not have permission to access the data but we may in the future (embargo)
			self.db.touch_record(record)
			return True

		except ckanapi.errors.NotFound:
			# Not found means this record was deleted
			self.db.delete_record(record)
			return True

		except:
			self.logger.error("Updating item failed")
			self.error_count =  self.error_count + 1
			if self.error_count < self.abort_after_numerrors:
				return True

		return False

