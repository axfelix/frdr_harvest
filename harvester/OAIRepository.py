from harvester.HarvestRepository import HarvestRepository
from functools import wraps
from sickle import Sickle
from sickle.oaiexceptions import BadArgument, CannotDisseminateFormat, IdDoesNotExist, NoSetHierarchy, \
	BadResumptionToken, NoRecordsMatch, OAIError
import re
import time


class OAIRepository(HarvestRepository):
	""" OAI Repository """

	def __init__(self, params):
		super(OAIRepository, self).__init__(params)
		self.sickle = Sickle(self.url)
		# TODO: better DDI implementation that doesn't simply flatten everything, see: https://sickle.readthedocs.io/en/latest/customizing.html
		if 'metadataprefix' in params:
			self.metadataPrefix = params['metadataprefix']
		else:
			self.metadataPrefix = "oai_dc"

	def _crawl(self):
		records = []

		try:
			if not self.set:
				records = self.sickle.ListRecords(metadataPrefix=self.metadataPrefix, ignore_deleted=True)
			else:
				records = self.sickle.ListRecords(metadataPrefix=self.metadataPrefix, ignore_deleted=True, set=self.set)
		except:
			self.logger.info("No items were found")

		self.db.create_repo(self.url, self.name, "oai", self.thumbnail, self.item_url_pattern)
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
						if "http" in idt.lower():
							metadata['dc:source'] = idt

				# Use the header id for the database key (needed later for OAI GetRecord calls)
				metadata['identifier'] = record.header.identifier
				oai_record = self.unpack_oai_metadata(metadata)
				self.db.write_record(oai_record, self.url)
				item_count = item_count + 1
				if (item_count % self.update_log_after_numitems == 0):
					tdelta = time.time() - self.tstart + 0.1
					self.logger.info("Done %s items after %s (%.1f items/sec)" % (
					item_count, self.formatter.humanize(tdelta), (item_count / tdelta)))

			except AttributeError:
				# probably not a valid OAI record
				# Islandora throws this for non-object directories
				pass

			except StopIteration:
				break

		self.logger.info("Processed %s items in feed" % (item_count))

	def unpack_oai_metadata(self, record):
		if self.metadataPrefix.lower() == "ddi":
			# Mapping as per http://www.ddialliance.org/resources/ddi-profiles/dc
			record["title"] = record.get("titl")
			record["creator"] = record.get("AuthEnty")
			record["subject"] = record.get("keyword", [])
			if "topcClas" in record.keys():
					record['subject'].extend(record["topcClas"])
			record["description"] = record.get("abstract")
			record["publisher"] = record.get("producer")
			record["contributor"] = record.get("othId")
			record["date"] = record.get("prodDate")
			record["type"] = record.get("dataKind")
			record["identifier"] = record.get("IDNo")
			record["rights"] = record.get("copyright")
			
# TODO: make this geospatial match up with DBInterface implementation
#
#			if "northBL" in record.keys():
#				# This record has geoSpatial bounding lines
#				record['geospatial'] = []
#				record['geospatial'].append({})
#				record['geospatial'][0]['type'] = "polygon"
#				record['geospatial'][0]['coordinates'] = []
#				# Convert into an array of closed bounding box points (clockwise polygon)
#				record['geospatial'][0]['coordinates'].append([record.get('southBL', 0), record.get('westBL',0)])
#				record['geospatial'][0]['coordinates'].append([record.get('northBL', 0), record.get('westBL',0)])
#				record['geospatial'][0]['coordinates'].append([record.get('northBL', 0), record.get('eastBL',0)])
#				record['geospatial'][0]['coordinates'].append([record.get('southBL', 0), record.get('eastBL',0)])
#				record['geospatial'][0]['coordinates'].append([record.get('southBL', 0), record.get('westBL',0)])
#

		if 'identifier' not in record.keys():
			return None

		# If there are multiple identifiers, and one of them contains a link, then prefer it
		# Otherwise just take the first one
		if isinstance(record["identifier"], list):
			valid_id = record["identifier"][0]
			for idstring in record["identifier"]:
				if "http" in idstring.lower():
					valid_id = idstring
			record["identifier"] = valid_id

		if 'creator' not in record.keys():
			self.logger.debug("Item %s is missing creator - will not be added" % (record["identifier"]))
			return None

		# If date is undefined add an empty key
		if 'date' not in record.keys():
			record["date"] = ""

		# If there are multiple dates choose the longest one (likely the most specific)
		if isinstance(record["date"], list):
			valid_date = record["date"][0]
			for datestring in record["date"]:
				if len(datestring) > len(valid_date):
					valid_date = datestring
			record["date"] = valid_date

		# Convert long dates into YYYY-MM-DD
		datestring = re.search("(\d{4}[-/]\d{2}[-/]\d{2})", record["date"])
		if datestring:
			record["date"] = datestring.group(0).replace("/", "-")

		if isinstance(record["title"], list):
			record["title"] = record["title"][0]

		if "contact" not in record.keys():
			record["contact"] = ""
		if isinstance(record["contact"], list):
			record["contact"] = record["contact"][0]

		if "series" not in record.keys():
			record["series"] = ""

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
		self.logger.debug(
			"Updating OAI record %s from repo at %s" % (record['local_identifier'], record['repository_url']))

		try:
			single_record = self.sickle.GetRecord(identifier=record["local_identifier"], metadataPrefix="oai_dc")

			metadata = single_record.metadata
			if 'identifier' in metadata.keys() and isinstance(metadata['identifier'], list):
				if "http" in metadata['identifier'][0].lower():
					metadata['dc:source'] = metadata['identifier'][0]
			metadata['identifier'] = single_record.header.identifier
			oai_record = self.unpack_oai_metadata(metadata)
			self.db.write_record(oai_record, self.url, "replace")
			return True

		except IdDoesNotExist:
			# Item no longer in this repo
			self.db.delete_record(record)

		except:
			self.logger.error("Updating item failed")
			self.error_count = self.error_count + 1
			if self.error_count < self.abort_after_numerrors:
				return True

		return False
