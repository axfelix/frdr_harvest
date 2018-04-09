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

	def __init__(self, db, log, finalconfig):
		self.db = db
		self.logger = log
		self.export_limit = finalconfig.get('export_file_limit_mb', 10)

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


	def _generate_gmeta(self, export_filepath, temp_filepath, only_new_records):
		self.logger.info("Exporter: generate_gmeta called")
		gmeta = []
		deleted = []

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

		buffer_limit = int(self.export_limit) * 1024 * 1024
		self.logger.info("Exporter: output file size limited to {} MB each".format(int(self.export_limit)))

		records_assembled = 0
		gmeta_batches = 0
		buffer_size = 0
		for row in records_cursor:
			if buffer_size > buffer_limit:
				gmeta_batches += 1
				self.logger.debug("Writing batch {} to output file".format(gmeta_batches))
				gingest_block = {"@datatype": "GIngest", "@version": "2016-11-09", "source_id": "ComputeCanada", "ingest_type": "GMetaList", "ingest_data": {"@datatype": "GMetaList", "@version": "2016-11-09", "gmeta":gmeta}}
				self._write_to_file(json.dumps(gingest_block), export_filepath, temp_filepath, "gmeta", gmeta_batches)
				gmeta = []
				buffer_size = 0

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
				deleted.append(record["dc:source"])
				continue

			con = self.db.getConnection()
			with con:
				if self.db.getType() == "sqlite":
					from sqlite3 import Row
					con.row_factory = Row
					litecur = con.cursor()
				elif self.db.getType() == "postgres":
					litecur = con.cursor(cursor_factory=None)

				litecur.execute(self.db._prep("SELECT coordinate_type, lat, lon FROM geospatial WHERE record_id=?"), (record["record_id"],) )
				geodata = litecur.fetchall()
				record["frdr:geospatial"] = []
				polycoordinates = []

				try:
					for coordinate in geodata:
						if coordinate[0] == "Polygon":
							polycoordinates.append([float(coordinate[1]), float(coordinate[2])])
						else:
							record["frdr:geospatial"].append({"frdr:geospatial_type":"Feature", "frdr:geospatial_geometry":{"frdr:geometry_type":coordinate[0], "frdr:geometry_coordinates": [float(coordinate[1]), float(coordinate[2])]}})
				except:
					pass

				if polycoordinates:
					record["frdr:geospatial"].append({"frdr:geospatial_type":"Feature", "frdr:geospatial_geometry":{"frdr:geometry_type":"Polygon", "frdr:geometry_coordinates": polycoordinates}})

			with con:
				if self.db.getType() == "sqlite":
					con.row_factory = lambda cursor, row: row[0]
					litecur = con.cursor()
				elif self.db.getType() == "postgres":
					from psycopg2.extras import DictCursor as DictCursor
					litecur = con.cursor(cursor_factory=DictCursor)

				# attach the other values to the dict
				litecur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id 
					WHERE records_x_creators.record_id=? AND records_x_creators.is_contributor=0"""), (record["record_id"],) )
				record["dc:contributor.author"] = litecur.fetchall()

				litecur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id 
					WHERE records_x_creators.record_id=? AND records_x_creators.is_contributor=1"""), (record["record_id"],) )
				record["dc:contributor"] = litecur.fetchall()

				litecur.execute(self.db._prep("""SELECT subjects.subject FROM subjects JOIN records_x_subjects on records_x_subjects.subject_id = subjects.subject_id 
					WHERE records_x_subjects.record_id=?"""), (record["record_id"],) )
				record["dc:subject"] = litecur.fetchall()

				litecur.execute(self.db._prep("""SELECT publishers.publisher FROM publishers JOIN records_x_publishers on records_x_publishers.publisher_id = publishers.publisher_id 
					WHERE records_x_publishers.record_id=?"""), (record["record_id"],) )
				record["dc:publisher"] = litecur.fetchall()

				litecur.execute(self.db._prep("""SELECT rights.rights FROM rights JOIN records_x_rights on records_x_rights.rights_id = rights.rights_id 
					WHERE records_x_rights.record_id=?"""), (record["record_id"],) )
				record["dc:rights"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='en' "), (record["record_id"],) )
				record["dc:description"] = litecur.fetchall()

				litecur.execute(self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='fr' "), (record["record_id"],) )
				record["frdr:description_fr"] = litecur.fetchall()

				litecur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id 
					WHERE records_x_tags.record_id=? and tags.language = 'en' """), (record["record_id"],) )
				record["frdr:tags"] = litecur.fetchall()

				litecur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id 
					WHERE records_x_tags.record_id=? and tags.language = 'fr' """), (record["record_id"],) )
				record["frdr:tags_fr"] = litecur.fetchall()

				litecur.execute(self.db._prep("""SELECT access.access FROM access JOIN records_x_access on records_x_access.access_id = access.access_id 
					WHERE records_x_access.record_id=?"""), (record["record_id"],) )
				record["frdr:access"] = litecur.fetchall()

			domain_schemas = {}
			with con:
				if self.db.getType() == "sqlite":
					from sqlite3 import Row
					con.row_factory = Row
					litecur = con.cursor()
				elif self.db.getType() == "postgres":
					litecur = con.cursor(cursor_factory=None)

				litecur.execute(self.db._prep("SELECT ds.namespace, dm.field_name, dm.field_value FROM domain_metadata dm, domain_schemas ds WHERE dm.schema_id=ds.schema_id and dm.record_id=?"), (record["record_id"],) )
				for row in litecur:
					domain_namespace = str(row[0])
					if domain_namespace not in domain_schemas.keys():
						current_count = len(domain_schemas)
						domain_schemas[domain_namespace] = "frdrcust" + str(current_count+1)
					custom_label = domain_schemas[domain_namespace] + ":" + str(row[1])
					record[custom_label] = str(row[2])

			# Convert friendly column names into dc element names
			record["dc:title"]         = record["title"]
			record["dc:date"]          = record["pub_date"]
			record["frdr:contact"]     = record["contact"]
			record["frdr:series"]      = record["series"]
			record["frdr:origin.id"]   = record["repository_name"]
			record["frdr:origin.icon"] = record["repository_thumbnail"]

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

			record["@context"] = {
				"dc": "http://dublincore.org/documents/dcmi-terms", 
				"frdr": "https://frdr.ca/schema/1.0", 
				"datacite": "https://schema.labs.datacite.org/meta/kernel-4.0/metadata.xsd"
			}
			for custom_schema in domain_schemas:
				short_label = domain_schemas[custom_schema]
				record["@context"].update({short_label: custom_schema})
			record["datacite:resourceTypeGeneral"] = "dataset"
			gmeta_data = {"@datatype": "GMetaEntry", "@version": "2016-11-09", "subject": record["dc:source"], "id": record["dc:source"], "visible_to": ["public"], "mimetype": "application/json", "content": record}
			gmeta.append(gmeta_data)

			buffer_size = buffer_size + len(json.dumps(gmeta_data))
			records_assembled += 1
			if (records_assembled % 1000 == 0):
				self.logger.info("Done processing {} records for export".format(records_assembled))

		self.logger.info("gmeta size: {} items in {} files".format(records_assembled, gmeta_batches + 1))
		return gmeta, deleted

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


	def _write_to_file(self, output, export_filepath, temp_filepath, export_format, batch_number=False):
		try:
			os.mkdir(temp_filepath)
		except:
			pass

		if batch_number:
			export_basename = "gmeta_" + str(batch_number) + ".json"
		elif export_format == "gmeta":
			export_basename = "gmeta.json"
		elif export_format == "rifcs":
			export_basename = "rifcs.xml"
		elif export_format == "delete":
			export_basename = "delete.txt"

		temp_filename = os.path.join(temp_filepath, export_basename)

		try:
			with open(temp_filename, "w") as tempfile:
				tempfile.write(output)
		except:
			self.logger.error("Unable to write output data to temporary file: {}".format(temp_filename))

		try:
			os.remove(export_filepath)
		except:
			pass

		try:
			os.rename(temp_filename, os.path.join(export_filepath, export_basename))
		except:
			self.logger.error("Unable to move temp file {} into output file location {}".format(temp_filename, export_filepath))

	def _cleanup_previous_exports(self, dirname, export_format):
		pattern = export_format + '_?[\d]*\.[a-z]*$'
		try:
			for f in os.listdir(dirname):
				if re.search(pattern, f):
					os.remove(os.path.join(dirname, f))
		except:
			pass

	def export_to_file(self, **kwargs):
		for key, value in kwargs.items():
			setattr(self, key, value)
		output = None
		self._cleanup_previous_exports(self.export_filepath, self.export_format)
		self._cleanup_previous_exports(self.export_filepath, "delete")
		self._cleanup_previous_exports(self.temp_filepath, self.export_format)

		if self.export_format == "gmeta":
			gmeta_block, delete_list = self._generate_gmeta(self.export_filepath, self.temp_filepath, self.only_new_records)
			output = json.dumps({"@datatype": "GIngest", "@version": "2016-11-09", "source_id": "ComputeCanada", "ingest_type": "GMetaList", "ingest_data": {"@datatype": "GMetaList", "@version": "2016-11-09", "gmeta":gmeta_block}})
		elif self.export_format == "rifcs":
			from lxml import etree
			output = self._generate_rifcs()
		else:
			self.logger.error("Unknown export format: {}".format(self.export_format))

		if output:
			self._write_to_file(output, self.export_filepath, self.temp_filepath, self.export_format)
		if len(delete_list):
			output = "\n".join(delete_list)
			self._write_to_file(output, self.export_filepath, self.temp_filepath, "delete")
