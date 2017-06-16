from harvester.HarvestRepository import HarvestRepository
from functools import wraps
import ckanapi
import time
import json
import re
import os.path


class CKANRepository(HarvestRepository):
	""" CKAN Repository """

	def setRepoParams(self, repoParams):
		self.metadataprefix = "ckan"
		super(CKANRepository, self).setRepoParams(repoParams)
		self.ckanrepo = ckanapi.RemoteCKAN(self.url)

		domain_metadata_file = "metadata/" + self.metadataprefix.lower()
		if os.path.isfile(domain_metadata_file):
			with open(domain_metadata_file) as dmf:
				# F gon' give it to ya
				self.domain_metadata = dmf.readlines()
		else:
			self.domain_metadata = []


	def _crawl(self):
		self.repository_id = self.db.update_repo(self.repository_id, self.url, self.set, self.name, "ckan", self.enabled, self.thumbnail, self.item_url_pattern,self.abort_after_numerrors,self.max_records_updated_per_run,self.update_log_after_numitems,self.record_refresh_days,self.repo_refresh_days)
		records = self.ckanrepo.action.package_list()

		item_count = 0
		for ckan_identifier in records:
			result = self.db.write_header(ckan_identifier, self.repository_id)
			item_count = item_count + 1
			if (item_count % self.update_log_after_numitems == 0):
				tdelta = time.time() - self.tstart + 0.1
				self.logger.info("Done %s item headers after %s (%.1f items/sec)" % (item_count, self.formatter.humanize(tdelta), item_count/tdelta) )

		self.logger.info("Found %s items in feed" % (item_count) )

	def format_ckan_to_oai(self, ckan_record, local_identifier):
		record = {}

		if not 'date_published' in ckan_record and not 'dates' in ckan_record:
			return None

		if ('contacts' in ckan_record) and ckan_record['contacts']:
			record["creator"] = [person.get('name',"") for person in ckan_record['contacts']]
		elif ('author' in ckan_record) and ckan_record['author']:
			record["creator"] = ckan_record['author']
		elif ('maintainer' in ckan_record) and ckan_record['maintainer']:
			record["creator"] = ckan_record['maintainer']
		else:
			record["creator"] = ckan_record['organization'].get('title',"")

		record["identifier"] = local_identifier

		if isinstance(ckan_record.get("title_translated", ""), dict):
			record["title"] = ckan_record["title_translated"].get("en","")
			record["title_fr"] = ckan_record["title_translated"].get("fr","")
		else:
			record["title"] = ckan_record.get("title", "")

		if isinstance(ckan_record.get("notes_translated", ""), dict):
			record["description"] = ckan_record["notes_translated"].get("en","")
			record["description_fr"] = ckan_record["notes_translated"].get("fr","")
		else:
			record["description"] = ckan_record.get("notes", "")
			record["description_fr"] = ckan_record.get("notes_fra", "")

		record["subject"] = ckan_record.get('subject',"")

		# Open Data Canada API now returns a mangled unicode-escaped-keyed-dict-as-string; regex is the only solution
		try:
			if ('resources' in ckan_record):
				record["dc:source"] = ckan_record['resources'][0].get('url',"")
			else:
				record["dc:source"] = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?", ckan_record["url"]).group(0)
				# Prefer English URL if regex finds French URL first
				# This feels bad but the way they expose this data is worse
				record["dc:source"] = re.sub("/fr/", "/en/", record["dc:source"])
		except:
			return None

		record["rights"] = [ckan_record['license_title']]
		record["rights"].append(ckan_record.get("license_url", ""))
		record["rights"].append(ckan_record.get("attribution", ""))
		record["rights"] = " - ".join(record["rights"])

		# Some CKAN records have a trailing null timestamp after date
		if ('dates' in ckan_record):
			record["pub_date"] = ckan_record['dates'][0].get('date',"")
		else:
			record["pub_date"] = re.sub(" 00:00:00", "", ckan_record['date_published'])

		if ('contacts' in ckan_record) and ckan_record['contacts']:
			record["contact"] = ckan_record["contacts"][0].get('email', "")
		else:
			record["contact"] = ckan_record.get("author_email", ckan_record.get("maintainer_email", ""))

		try: 
			record["series"] = ckan_record["data_series_name"]["en"]
		except:
			record["series"] = ckan_record.get("data_series_name", "")

		record["tags"] = []
		record["tags_fr"] = []
		if isinstance(ckan_record.get("keywords", ""), dict):
			for tag in ckan_record["keywords"]["en"]:
					record["tags"].append(tag)
			for tag in ckan_record["keywords"]["fr"]:
					record["tags_fr"].append(tag)
		else:
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
		#self.logger.debug("Updating CKAN record %s" % (record['local_identifier']) )

		try:
			ckan_record = self.ckanrepo.action.package_show(id=record['local_identifier'])
			oai_record = self.format_ckan_to_oai(ckan_record,record['local_identifier'])
			if oai_record:
				self.db.write_record(oai_record, self.repository_id, self.metadataprefix.lower(), self.domain_metadata)
			return True

		except ckanapi.errors.NotAuthorized:
			# Not authorized means that we currently do not have permission to access the data but we may in the future (embargo)
			self.db.touch_record(record)
			return True

		except ckanapi.errors.NotFound:
			# Not found means this record was deleted
			self.db.delete_record(record)
			return True

		except Exception as e:
			self.logger.error("Updating item failed: %s" % (str(e)) )
			# Touch the record so we do not keep requesting it on every run
			self.db.touch_record(record)
			self.error_count =  self.error_count + 1
			if self.error_count < self.abort_after_numerrors:
				return True

		return False

