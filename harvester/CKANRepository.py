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
		self.domain_metadata = []


	def _crawl(self):
		kwargs = {
			"repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name, "repo_type": "ckan", 
			"enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
			"abort_after_numerrors": self.abort_after_numerrors, "max_records_updated_per_run": self.max_records_updated_per_run,
			"update_log_after_numitems": self.update_log_after_numitems, "record_refresh_days": self.record_refresh_days,
			"repo_refresh_days": self.repo_refresh_days
		}
		self.repository_id = self.db.update_repo(**kwargs)
		records = self.ckanrepo.action.package_list()

		item_count = 0
		for ckan_identifier in records:
			result = self.db.write_header(ckan_identifier, self.repository_id)
			item_count = item_count + 1
			if (item_count % self.update_log_after_numitems == 0):
				tdelta = time.time() - self.tstart + 0.1
				self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count, self.formatter.humanize(tdelta), item_count/tdelta) )

		self.logger.info("Found {} items in feed".format(item_count) )

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

		if ('sector' in ckan_record):
			record["subject"] = ckan_record.get('sector', "")
		else:
			record["subject"] = ckan_record.get('subject',"")

		record["rights"] = [ckan_record['license_title']]
		record["rights"].append(ckan_record.get("license_url", ""))
		record["rights"].append(ckan_record.get("attribution", ""))
		record["rights"] = " - ".join(record["rights"])

		# Look for publication date in a few places
		# All of these assume the date will start with year first
		record["pub_date"] = ""
		if ('record_publish_date' in ckan_record):
			# Prefer an explicit publish date if it exists
			record["pub_date"] = ckan_record["record_publish_date"]
		elif ('date_published' in ckan_record):
			# Another possible field name for publication date
			record["pub_date"] = ckan_record["date_published"]
		elif ('dates' in ckan_record and isinstance(ckan_record["dates"], list) ):
			# A list of date objects, look for the one marked as Created
			for date_object in ckan_record['dates']:
				if date_object.type == "Created":
					record["pub_date"] = date_object.date

		# Some date formats have a trailing timestamp after date (ie: "2014-12-10T15:05:03.074998Z")
		record["pub_date"] = re.sub("[T ][0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.?[0-9]*[Z]?$", "", record["pub_date"])
		# Ensure date separator is a dash
		record["pub_date"] = record["pub_date"].replace("/","-")

		# Look at the year and make sure it is feasible, otherwise blank out the date
		# Limiting records to being published from 1300-2399
		publication_year = int(record["pub_date"][:2])
		if (publication_year < 13 or publication_year > 23):
			record["pub_date"] = ""

		if ('contacts' in ckan_record) and ckan_record['contacts']:
			record["contact"] = ckan_record["contacts"][0].get('email', "")
		else:
			record["contact"] = ckan_record.get("author_email", ckan_record.get("maintainer_email", ""))

		try: 
			record["series"] = ckan_record["data_series_name"]["en"]
		except:
			record["series"] = ckan_record.get("data_series_name", "")

		if isinstance(record["series"], dict):
			if len(record["series"]) > 0:
				record["series"] = ",".join(str(v) for v in list(record["series"].values()))
			else:
				record["series"] = ""

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

		# Access Constraints, if available
		if ('private' in ckan_record) and ckan_record['private'] == True:
			record["access"] = "Limited"
		elif ('private' in ckan_record) and ckan_record['private'] == False:
			record["access"] = "Public"
		else:
			record["access"] = ckan_record.get("download_audience")
			record["access"] = ckan_record.get("view_audience")

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
		#self.logger.debug("Updating CKAN record {}".format(record['local_identifier']) )

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
			self.logger.error("Updating record {} failed: {}".format(record['local_identifier'],e) )
			# Touch the record so we do not keep requesting it on every run
			self.db.touch_record(record)
			self.error_count =  self.error_count + 1
			if self.error_count < self.abort_after_numerrors:
				return True

		return False

