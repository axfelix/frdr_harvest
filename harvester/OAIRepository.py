from harvester.HarvestRepository import HarvestRepository
from functools import wraps
from sickle import Sickle
from sickle.oaiexceptions import BadArgument, CannotDisseminateFormat, IdDoesNotExist, NoSetHierarchy, \
	BadResumptionToken, NoRecordsMatch, OAIError
import re
import os.path
import time


class OAIRepository(HarvestRepository):
	""" OAI Repository """

	def setRepoParams(self, repoParams):
		self.metadataprefix = "oai_dc"
		super(OAIRepository, self).setRepoParams(repoParams)
		self.sickle = Sickle(self.url)

		domain_metadata_file = "metadata/" + self.metadataprefix.lower()
		if os.path.isfile(domain_metadata_file):
			with open(domain_metadata_file) as dmf:
				# F gon' give it to ya
				self.domain_metadata = dmf.readlines()
		else:
			self.domain_metadata = []


	def _crawl(self):
		records = []

		try:
			if self.set is None or self.set == "":
				records = self.sickle.ListRecords(metadataPrefix=self.metadataprefix, ignore_deleted=True)
			else:
				records = self.sickle.ListRecords(metadataPrefix=self.metadataprefix, ignore_deleted=True, set=self.set)
		except:
			self.logger.info("No items were found")

		self.repository_id = self.db.update_repo(self.repository_id, self.url, self.set, self.name, "oai",  self.enabled, self.thumbnail, self.item_url_pattern,self.abort_after_numerrors,self.max_records_updated_per_run,self.update_log_after_numitems,self.record_refresh_days,self.repo_refresh_days)
		item_count = 0

		while records:
			try:
				record = records.next()
				metadata = record.metadata

				# Search for a hyperlink in the list of identifiers
				if 'identifier' in metadata.keys():
					if not isinstance(metadata['identifier'], list):
						metadata['identifier'] = [metadata['identifier']]
					for idt in metadata['identifier']:
						if idt.lower().startswith("http"):
							metadata['dc:source'] = idt
						if idt.lower().startswith("doi:"):
							metadata['dc:source'] = "https://dx.doi.org/" + idt[4:]
						if idt.lower().startswith("hdl:"):
							metadata['dc:source'] = "https://hdl.handle.net/" + idt[4:]

				# EPrints workaround for using header datestamp in lieu of date
				if 'date' not in metadata.keys() and record.header.datestamp:
					metadata["date"] = record.header.datestamp

				# Use the header id for the database key (needed later for OAI GetRecord calls)
				metadata['identifier'] = record.header.identifier
				oai_record = self.unpack_oai_metadata(metadata)
				self.db.write_record(oai_record, self.repository_id, self.metadataprefix.lower(), self.domain_metadata)
				item_count = item_count + 1
				if (item_count % self.update_log_after_numitems == 0):
					tdelta = time.time() - self.tstart + 0.1
					self.logger.info("Done %s items after %s (%.1f items/sec)" % (
					item_count, self.formatter.humanize(tdelta), (item_count / tdelta)))

			except AttributeError:
				# probably not a valid OAI record
				# Islandora throws this for non-object directories
				self.logger.debug("AttributeError while working on item %i" % (item_count))
				pass

			except StopIteration:
				break

		self.logger.info("Processed %s items in feed" % (item_count))

	def unpack_oai_metadata(self, record):
		record["pub_date"] = record.get("date")
		
		if self.metadataprefix.lower() == "ddi":
			# TODO: better DDI implementation that doesn't simply flatten everything, see: https://sickle.readthedocs.io/en/latest/customizing.html
			# Mapping as per http://www.ddialliance.org/resources/ddi-profiles/dc
			record["title"] = record.get("titl")
			record["creator"] = record.get("AuthEnty")
			record["subject"] = record.get("keyword", [])
			if "topcClas" in record.keys() and len(record["topcClas"]) > 0:
					record['subject'].extend(filter(None,record["topcClas"]))
			record["description"] = record.get("abstract")
			record["publisher"] = record.get("producer")
			record["contributor"] = record.get("othId")
			record["pub_date"] = record.get("prodDate")
			record["type"] = record.get("dataKind")
			record["identifier"] = record.get("IDNo")
			record["rights"] = record.get("copyright")

			if "northBL" in record.keys():
				# This record has geoSpatial bounding lines
				# Convert into an array of closed bounding box points (clockwise polygon)
				record["geospatial"] = {"type": "Polygon", "coordinates": [[[record["northBL"][0], record["westBL"][0]], [record["northBL"][0], record["eastBL"][0]], [record["southBL"][0], record["westBL"][0]], [record["southBL"][0], record["eastBL"][0]]]]}


		if self.metadataprefix.lower() == "fgdc":
			#record["title"] = record.get("title")
			record["creator"] = record.get("origin")
			record["subject"] = record.get("themekey")
			record["description"] = record.get("abstract")
			record["publisher"] = record.get("cntorg")
			record["pub_date"] = [record.get("begdate"), record.get("enddate")]
			record["type"] = record.get("geoform")
			record["identifier"] = record.get("onlink")
			record["rights"] = record.get("distliab")

			if "bounding" in record.keys():
				# Sometimes point data is hacked in as a bounding box
				if record["westbc"] == record["eastbc"] and record["northbc"] == record["southbc"]:
					record["geospatial"] = {"type": "Point", "coordinates": [[[record["northbc"][0], record["westbc"][0]]]]}
				else:
					record["geospatial"] = {"type": "Polygon", "coordinates": [[[record["northbc"][0], record["westbc"][0]], [record["northbc"][0], record["eastbc"][0]], [record["southbc"][0], record["westbc"][0]], [record["southbc"][0], record["eastbc"][0]]]]}


		if self.metadataprefix.lower() == "frdr":
			record["coverage"] = record.get("geolocationPlace")

			if "geolocationPoint" in record.keys():
				record["geospatial"] = {"type": "Point", "coordinates": [record["geolocationPoint"][0].split()]}
			
			if "geolocationBox" in record.keys():
				boxcoordinates = record["geolocationBox"][0].split()
				record["geospatial"] = {"type": "Polygon", "coordinates": [[boxcoordinates[x:x+2] for x in range (0, len(boxcoordinates),2)]]}


		if 'identifier' not in record.keys():
			return None

		if record["pub_date"] is None:
			return None

		# If there are multiple identifiers, and one of them contains a link, then prefer it
		# Otherwise just take the first one
		if isinstance(record["identifier"], list):
			valid_id = record["identifier"][0]
			for idstring in record["identifier"]:
				if "http" in idstring.lower():
					valid_id = idstring
			record["identifier"] = valid_id

		if 'creator' not in record.keys() and 'contributor' not in record.keys() and 'publisher' not in record.keys():
			self.logger.debug("Item %s is missing creator - will not be added" % (record["identifier"]))
			return None
		elif 'creator' not in record.keys() and 'contributor' in record.keys():
			record["creator"] = record["contributor"]
		elif 'creator' not in record.keys() and 'publisher' in record.keys():
			record["creator"] = record["publisher"]

		# If date is undefined add an empty key
		if 'pub_date' not in record.keys():
			record["pub_date"] = ""

		# If there are multiple dates choose the longest one (likely the most specific)
		if isinstance(record["pub_date"], list):
			valid_date = record["pub_date"][0]
			for datestring in record["pub_date"]:
				if len(datestring) > len(valid_date):
					valid_date = datestring
			record["pub_date"] = valid_date

		# If date is still a one-value list, make it a string
		if isinstance(record["pub_date"], list):
			record["pub_date"] = record["pub_date"][0]

		# Convert long dates into YYYY-MM-DD
		datestring = re.search("(\d{4}[-/]?\d{2}[-/]?\d{2})", record["pub_date"])
		if datestring:
			record["pub_date"] = datestring.group(0).replace("/", "-")

		# If dates are entirely numeric, add separators
		if not re.search("\D", record["pub_date"]):
			if (len(record["pub_date"]) == 6):
				record["pub_date"] = record["pub_date"][0] + record["pub_date"][1] + record["pub_date"][2] + record["pub_date"][3] + "-" + record["pub_date"][4] + record["pub_date"][5]
			if (len(record["pub_date"]) == 8):
				record["pub_date"] = record["pub_date"][0] + record["pub_date"][1] + record["pub_date"][2] + record["pub_date"][3] + "-" + record["pub_date"][4] + record["pub_date"][5] + "-" + record["pub_date"][6] + record["pub_date"][7]

		if isinstance(record["title"], list):
			record["title"] = record["title"][0]

		if "contact" not in record.keys():
			record["contact"] = ""
		if isinstance(record["contact"], list):
			record["contact"] = record["contact"][0]

		if "series" not in record.keys():
			record["series"] = ""

		# EPrints workaround for liberal use of dc:identifier
		# Rather not hardcode a single source URL for this
		if self.url == "http://spectrum.library.concordia.ca/cgi/oai2":
			for relation in record["relation"]:
				if "http://spectrum.library.concordia.ca" in relation:
					record["dc:source"] = relation

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
	def _update_record(self, record):
		self.logger.debug("Updating OAI record %s" % (record['local_identifier']) )

		try:
			single_record = self.sickle.GetRecord(identifier=record["local_identifier"], metadataPrefix=self.metadataprefix)

			metadata = single_record.metadata
			if 'identifier' in metadata.keys() and isinstance(metadata['identifier'], list):
				if "http" in metadata['identifier'][0].lower():
					metadata['dc:source'] = metadata['identifier'][0]

			# EPrints workaround for using header datestamp in lieu of date
			if 'date' not in metadata.keys() and record.header.datestamp:
				metadata["date"] = record.header.datestamp

			metadata['identifier'] = single_record.header.identifier
			oai_record = self.unpack_oai_metadata(metadata)
			self.db.write_record(oai_record, self.repository_id, self.metadataprefix.lower(), self.domain_metadata)
			return True

		except IdDoesNotExist:
			# Item no longer in this repo
			self.db.delete_record(record)

		except Exception as e:
			self.logger.error("Updating item failed: %s" % (str(e)) )
			# Touch the record so we do not keep requesting it on every run
			self.db.touch_record(record)
			self.error_count = self.error_count + 1
			if self.error_count < self.abort_after_numerrors:
				return True

		return False
