import time
import re
from string import Template
import json
import os
import sys
import html

class Exporter(object):
	""" Read records from the database and export to given formats """

	__records_per_loop = 500

	def __init__(self, db, log, params):
		self.db = db
		self.logger = log
		self.dbtype = params.get('type', None)

	def _construct_local_url(self, record):
		# Check if the local_identifier has already been turned into a url
		if "http" in record["local_identifier"].lower():
			return record["local_identifier"]

		# Check for OAI format of identifier (oai:domain:id)
		oai_id = None
		oai_search = re.search("oai:(.+):(.+)", record["local_identifier"])
		if oai_search:
			oai_id = oai_search.group(2)
			# TODO: determine if this is needed for all repos, or just SFU?
			oai_id = oai_id.replace("_", ":")

		# If given a pattern then substitue in the item ID and return it
		if "item_url_pattern" in record and record["item_url_pattern"]:
			if oai_id:
				local_url = re.sub("(\%id\%)", oai_id, record["item_url_pattern"])
			else:
				local_url = re.sub("(\%id\%)", record["local_identifier"], record["item_url_pattern"])
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
		local_url = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?",
							  record["local_identifier"])
		if local_url:
			return local_url.group(0)

		local_url = None
		return local_url


	def _generate_gmeta(self, batch_size, export_filepath, temp_filepath, only_new_records):
		self.logger.info("Exporter: generate_gmeta called")
		gmeta = []

		try:
			with open("data/last_run_timestamp", "r") as lastrun:
				lastrun_timestamp = lastrun.read()
		except:
			lastrun_timestamp = 0

		records_con = self.db.getConnection()
		with records_con:
			records_cursor = records_con.cursor()

		records_cursor.execute(self.db._prep("""SELECT recs.record_id, recs.title, recs.pub_date, recs.contact, recs.series, recs.source_url, recs.deleted, recs.local_identifier, recs.modified_timestamp,
			repos.repository_url, repos.repository_name, repos.repository_thumbnail, repos.item_url_pattern, repos.last_crawl_timestamp 
			FROM records recs, repositories repos WHERE recs.repository_id = repos.repository_id """))

		records_assembled = 0
		gmeta_batches = 0
		for row in records_cursor:
			if records_assembled % batch_size == 0 and records_assembled!=0:
				gmeta_batches += 1
				self.logger.debug("Writing batch %s to output file" % (gmeta_batches))
				gingest_block = {"@datatype": "GIngest", "@version": "2016-11-09", "source_id": "ComputeCanada", "ingest_type": "GMetaList", "ingest_data": {"@datatype": "GMetaList", "@version": "2016-11-09", "gmeta":gmeta}}
				self._write_to_file(json.dumps(gingest_block), export_filepath, temp_filepath, "gmeta", gmeta_batches)
				del gmeta[:batch_size]

			record = (dict(zip(['record_id','title', 'pub_date', 'contact', 'series', 'source_url', 'deleted', 'local_identifier', 'modified_timestamp',
				'repository_url', 'repository_name', 'repository_thumbnail', 'item_url_pattern',  'last_crawl_timestamp'], row)))
			record["deleted"] = int(record["deleted"])

			if only_new_records == True and float(lastrun_timestamp) > record["last_crawl_timestamp"]:
				continue

			if (len(record['title']) == 0):
				continue

			record["dc:source"] = self._construct_local_url(record)
			if record["dc:source"] is None:
				continue

			if record["deleted"] == 1:
				gmeta_data = {"@datatype": "GMetaEntry", "@version": "2016-11-09", "subject": record["dc:source"], "id": record["dc:source"], "visible_to": ["public"], "mimetype": "application/json", "content": {}}
				gmeta.append(gmeta_data)
				continue

			con = self.db.getConnection()
			with con:
				if self.dbtype == "sqlite":
					from sqlite3 import Row
					con.row_factory = Row
					litecur = con.cursor()
				elif self.dbtype == "postgres":
					litecur = con.cursor(cursor_factory=None)

				litecur.execute(self.db._prep("SELECT coordinate_type, lat, lon FROM geospatial WHERE record_id=?"), (record["record_id"],) )
				geodata = litecur.fetchall()
				record["nrdr:geospatial"] = []
				polycoordinates = []

				try:
					for coordinate in geodata:
						if coordinate[0] == "Polygon":
							polycoordinates.append([float(coordinate[1]), float(coordinate[2])])
						else:
							record["nrdr:geospatial"].append({"type":"Feature", "geometry":{"type":coordinate[0], "coordinates": [float(coordinate[1]), float(coordinate[2])]}})
				except:
					pass

				if polycoordinates:
					record["nrdr:geospatial"].append({"type":"Feature", "geometry":{"type":"Polygon", "coordinates": polycoordinates}})

			with con:
				if self.dbtype == "sqlite":
					con.row_factory = lambda cursor, row: row[0]
					litecur = con.cursor()
				elif self.dbtype == "postgres":
					from psycopg2.extras import DictCursor as DictCursor
					litecur = con.cursor(cursor_factory=DictCursor)


				# attach the other values to the dict
				# TODO: investigate doing this purely in SQL
				litecur.execute(self.db._prep("SELECT creator FROM creators WHERE record_id=? AND is_contributor=0"), (record["record_id"],) )
				record["dc:contributor.author"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT creator FROM creators WHERE record_id=? AND is_contributor=1"), (record["record_id"],) )
				record["dc:contributor"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT subject FROM subjects WHERE record_id=?"), (record["record_id"],) )
				record["dc:subject"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT rights FROM rights WHERE record_id=?"), (record["record_id"],) )
				record["dc:rights"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='en' "), (record["record_id"],) )
				record["dc:description"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='fr' "), (record["record_id"],) )
				record["nrdr:description_fr"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT tag FROM tags WHERE record_id=? AND language='en'"), (record["record_id"],) )
				record["nrdr:tags"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT tag FROM tags WHERE record_id=? AND language='fr'"), (record["record_id"],) )
				record["nrdr:tags_fr"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT field_name, field_value FROM domain_metadata WHERE record_id=?"), (record["record_id"],) )
				domain_metadata = litecur.fetchall()
				for row in domain_metadata:
					record[row[0]] = row[1]

			# Convert friendly column names into dc element names
			record["dc:title"]         = record["title"]
			record["dc:date"]          = record["pub_date"]
			record["nrdr:contact"]     = record["contact"]
			record["nrdr:series"]      = record["series"]
			record["nrdr:origin.id"]   = record["repository_name"]
			record["nrdr:origin.icon"] = record["repository_thumbnail"]

			# remove unneeded columns from output
			record.pop("contact", None)
			record.pop("deleted", None)
			record.pop("item_url_pattern", None)
			record.pop("last_crawl_timestamp", None)
			record.pop("local_identifier", None)
			record.pop("modified_timestamp", None)
			record.pop("pub_date", None)
			record.pop("record_id", None)
			record.pop("repository_name", None)
			record.pop("repository_thumbnail", None)
			record.pop("repository_url", None)
			record.pop("series", None)
			record.pop("source_url", None)
			record.pop("title", None)

			record["@context"] = {"dc": "http://dublincore.org/documents/dcmi-terms", "frdr": "https://frdr.ca/schema/1.0/", "datacite": "https://schema.labs.datacite.org/meta/kernel-4.0/metadata.xsd"}
			record["datacite:resourceTypeGeneral"] = "dataset"
			gmeta_data = {"@datatype": "GMetaEntry", "@version": "2016-11-09", "subject": record["dc:source"], "id": record["dc:source"], "visible_to": ["public"], "mimetype": "application/json", "content": record}
			gmeta.append(gmeta_data)

			records_assembled += 1

		self.logger.info("gmeta size: %s items in %s files" % (records_assembled, gmeta_batches + 1))
		return gmeta

	def _validate_rifcs(self, rifcs):
		self.logger.info("Exporter: _validate_rifs called")
		with open('schema/registryObjects.xsd', 'r') as f:
			schema_root = etree.XML(f.read())
		schema = etree.XMLSchema(schema_root)
		# Generate parser and fix any XML bugs from the templating.
		xmlparser = etree.XMLParser(schema=schema, recover = True)
		try:
			etree.fromstring(rifcs, xmlparser)
			self.logger.info("Valid RIFCS Generated")
		except etree.XMLSchemaError:
			self.logger.error("Invalid RIFCS Generated")
			raise SystemExit


	def _write_to_file(self, output, export_filepath, temp_filepath, export_format, gmeta_batches=False):
		try:
			os.mkdir(temp_filepath)
		except:
			pass

		if gmeta_batches:
			export_basename = "gmeta_" + str(gmeta_batches) + ".json"
		elif export_format == "gmeta":
			export_basename = "gmeta.json"
		elif export_format == "rifcs":
			export_basename = "rifcs.xml"

		temp_filename = os.path.join(temp_filepath, export_basename)

		try:
			with open(temp_filename, "w") as tempfile:
				tempfile.write(output)
		except:
			self.logger.error("Unable to write output data to temporary file: %s" % (temp_filename))

		try:
			os.remove(export_filepath)
		except:
			pass

		try:
			os.rename(temp_filename, os.path.join(export_filepath, export_basename))
		except:
			self.logger.error("Unable to move temp file %s into output file location %s" % (temp_filename, export_filepath))

	def _cleanup_previous_exports(self, dirname, export_format):
		pattern = export_format + '_?[\d]*\.[a-z]*$'
		try:
			for f in os.listdir(dirname):
				if re.search(pattern, f):
					os.remove(os.path.join(dirname, f))
		except:
			pass

	def export_to_file(self, export_format, export_filepath, export_batch_size, only_new_records, temp_filepath="temp"):
		output = None
		self._cleanup_previous_exports(export_filepath, export_format)
		self._cleanup_previous_exports(temp_filepath, export_format)

		if export_format == "gmeta":
			gmeta_block = self._generate_gmeta(export_batch_size, export_filepath, temp_filepath, only_new_records)
			output = json.dumps({"@datatype": "GIngest", "@version": "2016-11-09", "source_id": "ComputeCanada", "ingest_type": "GMetaList", "ingest_data": {"@datatype": "GMetaList", "@version": "2016-11-09", "gmeta":gmeta_block}})
		elif export_format == "rifcs":
			from lxml import etree
			output = self._generate_rifcs()
		else:
			self.logger.error("Unknown export format: %s" % (export_format))

		if output:
			self._write_to_file(output, export_filepath, temp_filepath, export_format)
