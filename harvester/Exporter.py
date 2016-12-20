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
		doi = re.search("(doi|DOI):\s%s\S+", record["local_identifier"])
		if doi:
			doi = doi.group(0).rstrip('\.')
			local_url = re.sub("(doi|DOI):\s%s", "http://dx.doi.org/", doi)
			return local_url

		# If the item has a source URL, use it
		if ('source_url' in record) and record['source_url']:
			return record['source_url']

		# URL is in the identifier
		local_url = re.search("(http|ftp|https)://([\w_-]+(%s:(%s:\.[\w_-]+)+))([\w.,@%s^=%&:/~+#-]*[\w@%s^=%&/~+#-])%s",
							  record["local_identifier"])
		if local_url:
			return local_url.group(0)

		local_url = None
		return local_url


	def _generate_gmeta(self, batch_size, export_filepath, temp_filepath):
		self.logger.info("Exporter: generate_gmeta called")
		con = self.db.getConnection()
		gmeta = []

		# Only select records that have complete data
		if self.dbtype == "sqlite":
			records = con.execute("""SELECT recs.title, recs.date, recs.contact, recs.series, recs.source_url, recs.deleted, recs.local_identifier, recs.repository_url, repos.repository_name as "nrdr:origin.id", repos.repository_thumbnail as "nrdr:origin.icon", repos.item_url_pattern
					FROM records recs, repositories repos
					WHERE recs.title != '' and recs.repository_url = repos.repository_url """)

		elif self.dbtype == "postgres":
			with con:
				cur = con.cursor()
				cur.execute("""SELECT recs.title, recs.date, recs.contact, recs.series, recs.source_url, recs.deleted, recs.local_identifier, recs.repository_url, repos.repository_name as "nrdr:origin.id", repos.repository_thumbnail as "nrdr:origin.icon", repos.item_url_pattern
						FROM records recs, repositories repos
						WHERE recs.title != '' and recs.repository_url = repos.repository_url """)
				records = cur.fetchall()

		records_assembled = 0
		gmeta_batches = 0
		for record in records:
			if records_assembled % batch_size == 0 and records_assembled!=0:
				gmeta_batches += 1
				gingest_block = {"@datatype": "GIngest", "@version": "2016-11-09", "source_id": "ComputeCanada", "ingest_type": "GMetaList", "ingest_data": {"@datatype": "GMetaList", "@version": "2016-11-09", "gmeta":gmeta}}
				self._write_to_file(json.dumps(gingest_block), export_filepath, temp_filepath, "gmeta", gmeta_batches)
				del gmeta[:batch_size]


			if self.dbtype == "sqlite":
				record = dict(zip([tuple[0] for tuple in records.description], record))
			elif self.dbtype == "postgres":
				record = (dict(zip(['title', 'date', 'contact', 'series', 'source_url', 'deleted', 'local_identifier', 'repository_url', 'nrdr:origin.id', 'nrdr:origin.icon', 'item_url_pattern'], record)))
				record["deleted"] = int(record["deleted"])


			record["dc:source"] = self._construct_local_url(record)
			if record["dc:source"] is None:
				continue

			if record["deleted"] == 1:
				gmeta_data = {record["dc:source"]: {"mimetype": "application/json", "content": None}}
				gmeta.append(gmeta_data)
				continue

			with con:
				if self.dbtype == "sqlite":
					from sqlite3 import Row
					con.row_factory = Row
					litecur = con.cursor()
				elif self.dbtype == "postgres":
					litecur = con.cursor(cursor_factory=None)

				litecur.execute(
					"SELECT coordinate_type, lat, lon FROM geospatial WHERE local_identifier=%s AND repository_url=%s",
					(record["local_identifier"], record["repository_url"]))
				geodata = litecur.fetchall()
				record["nrdr:geospatial"] = []
				polycoordinates = []

				for coordinate in geodata:
					if coordinate[0] == "Polygon":
						polycoordinates.append([float(coordinate[1]), float(coordinate[2])])
					else:
						record["nrdr:geospatial"].append({"type":"Feature", "geometry":{"type":coordinate[0], "coordinates": [float(coordinate[1]), float(coordinate[2])]}})

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
				litecur.execute(
					"SELECT creator FROM creators WHERE local_identifier=%s AND repository_url=%s AND is_contributor=0",
					(record["local_identifier"], record["repository_url"]))
				record["dc:contributor.author"] = litecur.fetchall()

				litecur.execute(
					"SELECT creator FROM creators WHERE local_identifier=%s AND repository_url=%s AND is_contributor=1",
					(record["local_identifier"], record["repository_url"]))
				record["dc:contributor"] = litecur.fetchall()

				litecur.execute("SELECT subject FROM subjects WHERE local_identifier=%s AND repository_url=%s",
								(record["local_identifier"], record["repository_url"]))
				record["dc:subject"] = litecur.fetchall()

				litecur.execute("SELECT rights FROM rights WHERE local_identifier=%s AND repository_url=%s",
								(record["local_identifier"], record["repository_url"]))
				record["dc:rights"] = litecur.fetchall()

				litecur.execute("SELECT description FROM descriptions WHERE local_identifier=%s AND repository_url=%s",
								(record["local_identifier"], record["repository_url"]))
				record["dc:description"] = litecur.fetchall()

				litecur.execute(
					"SELECT description FROM fra_descriptions WHERE local_identifier=%s AND repository_url=%s",
					(record["local_identifier"], record["repository_url"]))
				record["nrdr:description_fr"] = litecur.fetchall()

				litecur.execute("SELECT tag FROM tags WHERE local_identifier=%s AND repository_url=%s AND language='en'",
								(record["local_identifier"], record["repository_url"]))
				record["nrdr:tags"] = litecur.fetchall()

				litecur.execute("SELECT tag FROM tags WHERE local_identifier=%s AND repository_url=%s AND language='fr'",
								(record["local_identifier"], record["repository_url"]))
				record["nrdr:tags_fr"] = litecur.fetchall()

				record.pop("repository_url", None)
				record.pop("local_identifier", None)

			record["dc:title"] = record["title"]
			record.pop("title", None)
			record["dc:date"] = record["date"]
			record.pop("date", None)
			record["nrdr:contact"] = record["contact"]
			record.pop("contact", None)
			record["nrdr:series"] = record["series"]
			record.pop("series", None)
			record.pop("source_url", None)
			record.pop("deleted", None)

			record["@context"] = {"dc": "http://dublincore.org/documents/dcmi-terms", "nrdr": "http://nrdr-ednr.ca/schema/1.0/", "datacite": "https://schema.labs.datacite.org/meta/kernel-4.0/metadata.xsd"}
			record["datacite:resourceTypeGeneral"] = "dataset"
			gmeta_data = {"@datatype": "GMetaEntry", "@version": "2016-11-09", "subject": record["dc:source"], "id": record["dc:source"], "visible_to": ["public"], "mimetype": "application/json", "content": record}
			gmeta.append(gmeta_data)

			records_assembled += 1

		self.logger.info("gmeta size: %s items" % (len(gmeta)))
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

	def _generate_rifcs(self):
		self.logger.info("Exporter: generate_rifcs called")
		rifcs_header_xml = open("templates/rifcs_header.xml").read()
		rifcs_footer_xml = open("templates/rifcs_footer.xml").read()
		rifcs_object_xml = open("templates/rifcs_object.xml").read()
		rifcs_object_template = Template(rifcs_object_xml)
		con = self.db.getConnection()
		rifcs = ""
		rec_start = 0
		rec_limit = self.__records_per_loop
		found_records = True

		while found_records:
			found_records = False

			# Select a window of records at a time
			records = con.execute("""SELECT recs.title, recs.date, recs.contact, recs.series, recs.source_url, recs.deleted, recs.local_identifier, recs.repository_url,
					repos.repository_name as "nrdr_origin_id", repos.repository_thumbnail as "nrdr_origin_icon", repos.item_url_pattern
					FROM records recs, repositories repos
					WHERE recs.title != '' and recs.repository_url = repos.repository_url
					LIMIT %s OFFSET %s""", (rec_limit, rec_start))

			num_records = 0
			for record in records:
				found_records = True
				num_records += 1
				record = dict(zip([tuple[0] for tuple in records.description], record))
				record["dc_source"] = self._construct_local_url(record)
				if record["dc_source"] is None:
					continue

				if record["deleted"] == 1:
					# TODO: find out how RIF-CS represents deleted records
					rifcs += ""
					continue

				with con:
					if self.dbtype == "sqlite":
						con.row_factory = lambda cursor, row: row[0]
						litecur = con.cursor()
					elif self.dbtype == "postgres":
						from psycopg2.extras import DictCursor as DictCursor
						litecur = con.cursor(cursor_factory=DictCursor)

					litecur.execute(
						"SELECT creator FROM creators WHERE local_identifier=%s AND repository_url=%s AND is_contributor=0",
						(record["local_identifier"], record["repository_url"]))
					record["dc_contributor_author"] = litecur.fetchall()

					litecur.execute(
						"SELECT creator FROM creators WHERE local_identifier=%s AND repository_url=%s AND is_contributor=1",
						(record["local_identifier"], record["repository_url"]))
					record["dc_contributor"] = litecur.fetchall()

					litecur.execute("SELECT subject FROM subjects WHERE local_identifier=%s AND repository_url=%s",
									(record["local_identifier"], record["repository_url"]))
					record["dc_subject"] = litecur.fetchall()

					litecur.execute("SELECT rights FROM rights WHERE local_identifier=%s AND repository_url=%s",
									(record["local_identifier"], record["repository_url"]))
					record["dc_rights"] = litecur.fetchall()

					litecur.execute(
						"SELECT description FROM descriptions WHERE local_identifier=%s AND repository_url=%s",
						(record["local_identifier"], record["repository_url"]))
					record["dc_description"] = litecur.fetchall()

					litecur.execute(
						"SELECT description FROM fra_descriptions WHERE local_identifier=%s AND repository_url=%s",
						(record["local_identifier"], record["repository_url"]))
					record["nrdr_fra_description"] = litecur.fetchall()

					litecur.execute("SELECT tag FROM tags WHERE local_identifier=%s AND repository_url=%s AND language='en'",
									(record["local_identifier"], record["repository_url"]))
					record["nrdr:tags"] = litecur.fetchall()

					litecur.execute("SELECT tag FROM tags WHERE local_identifier=%s AND repository_url=%s AND language='fr'",
									(record["local_identifier"], record["repository_url"]))
					record["nrdr:tags_fr"] = litecur.fetchall()

					litecur.execute(
						"SELECT coordinate_type, lat, lon FROM geospatial WHERE local_identifier=%s AND repository_url=%s",
						(record["local_identifier"], record["repository_url"]))
					record["nrdr_geospatial"] = litecur.fetchall()

				record["dc_title"] = record["title"]
				record["dc_date"] = record["date"]
				record["nrdr_contact"] = record["contact"]
				record["nrdr_series"] = record["series"]

				# TODO: break this apart so that where multi-valued elements can exist in the XML, then multiple XML blocks are output
				# Right now these are just being (wrongly) joined into a single string for testing purposes
				for key, value in record.items():
					if isinstance(value, list):
						record[key] = html.escape(', '.join(fld or "" for fld in value))
					if isinstance(value, str):
						record[key] = html.escape(value)
				# TODO: determine how some records end up with blank nrdr_origin_id
				if 'nrdr_origin_id' in record.keys():
					rifcs += rifcs_object_template.substitute(record)

			if found_records:
				self.logger.info("Done exporting records %s to %s" % (rec_start, (rec_start + num_records)))
			rec_start += rec_limit

		# TODO: flush buffer to file after every block of records is done, so it doesn't get so large in memory
		rifcs = rifcs_header_xml + rifcs + rifcs_footer_xml
		self._validate_rifcs(rifcs)
		self.logger.info("rifcs size: %s bytes" % (len(rifcs)))
		return rifcs


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
				self.logger.info("Writing output file")
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
			self.logger.error(
				"Unable to move temp file %s into output file location %s" % (temp_filename, export_filepath))


	def export_to_file(self, export_format, export_filepath, export_batch_size, temp_filepath="temp"):
		output = None

		if export_format == "gmeta":
			gmeta_block = self._generate_gmeta(export_batch_size, export_filepath, temp_filepath)
			output = json.dumps({"@datatype": "GIngest", "@version": "2016-11-09", "source_id": "ComputeCanada", "ingest_type": "GMetaList", "ingest_data": {"@datatype": "GMetaList", "@version": "2016-11-09", "gmeta":gmeta_block}})
		elif export_format == "rifcs":
			from lxml import etree
			output = self._generate_rifcs()
		else:
			self.logger.error("Unknown export format: %s" % (export_format))

		if output:
			self._write_to_file(output, export_filepath, temp_filepath, export_format)
