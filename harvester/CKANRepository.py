from harvester.HarvestRepository import HarvestRepository
from functools import wraps
import ckanapi
import time

class CKANRepository(HarvestRepository):
	""" CKAN Repository """
	def _crawl(self):
		ckanrepo = ckanapi.RemoteCKAN(self.url)
		self.db.create_repo(self.url, self.name, "ckan", self.thumbnail)
		records = ckanrepo.action.package_list()

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

		if ('author' in ckan_record) and ckan_record['author']:
			record["creator"] = ckan_record['author']
		elif ('maintainer' in ckan_record) and ckan_record['maintainer']:
			record["creator"] = ckan_record['maintainer']
		else:
			record["creator"] = ckan_record['organization']['title']

		record["identifier"] = local_identifier
		record["title"] = ckan_record['title']
		record["description"] = ckan_record['notes']
		record["date"] = ckan_record['date_published']
		record["subject"] = ckan_record['subject']
		record["rights"] = ckan_record['attribution']
		record["dc:source"] = ckan_record['url']

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
		self.logger.debug("Updating record %s from repo at %s" % (record['local_identifier'],self.url) )

		try:
			ckanrepo = ckanapi.RemoteCKAN(self.url)
			ckan_record = ckanrepo.action.package_show(id=record['local_identifier'])
			oai_record = self.format_ckan_to_oai(ckan_record,record['local_identifier'])
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
			if self.error_count >= self.abort_after_numerrors:
				return False

		return False
