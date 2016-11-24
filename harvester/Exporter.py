import time
import re
from string import Template
import json
import os
import sys
from lxml import etree
import html
import sqlite3

class Exporter(object):
	""" Read records from the database and export to given formats """

	__records_per_loop = 500

	def __init__(self, db, log):
		self.db = db
		self.logger = log

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


	def _generate_gmeta(self, batch_size, export_filepath, temp_filepath):
		self.logger.info("Exporter: generate_gmeta called")
		con = self.db.getConnection()
		gmeta = []

		# Only select records that have complete data
		records = con.execute("""SELECT recs.title, recs.date, recs.contact, recs.series, recs.source_url, recs.deleted, recs.local_identifier, recs.repository_url, repos.repository_name as "nrdr:origin.id", repos.repository_thumbnail as "nrdr:origin.icon", repos.item_url_pattern
				FROM records recs, repositories repos
				WHERE recs.title != '' and recs.repository_url = repos.repository_url """)

		records_assembled = 0
		gmeta_batches = 0
		for record in records:
			if records_assembled % batch_size == 0 and records_assembled!=0:
				gmeta_batches += 1
				self._write_to_file(json.dumps({"_gmeta":gmeta}), export_filepath, temp_filepath, "gmeta", gmeta_batches)
				del gmeta[:batch_size]

			record = dict(zip([tuple[0] for tuple in records.description], record))
			record["dc:source"] = self._construct_local_url(record)
			if record["dc:source"] is None:
				continue

			if record["deleted"] == 1:
				gmeta_data = {record["dc:source"]: {"mimetype": "application/json", "content": None}}
				gmeta.append(gmeta_data)
				continue

			with con:
				con.row_factory = sqlite3.Row
				litecur = con.cursor()
				litecur.execute(
					"SELECT coordinate_type, lat, lon FROM geospatial WHERE local_identifier=? AND repository_url=?",
					(record["local_identifier"], record["repository_url"]))
				geodata = litecur.fetchall()
				record["nrdr:geospatial"] = []
				polycoordinates = []

				for coordinate in geodata:
					if coordinate[0] == "Polygon":
						polycoordinates.append([coordinate[1], coordinate[2]])
					else:
						record["nrdr:geospatial"].append({"type":"Feature", "geometry":{"type":coordinate[0], "coordinates": [coordinate[1], coordinate[2]]}})

				if polycoordinates:
					record["nrdr:geospatial"].append({"type":"Feature", "geometry":{"type":"Polygon", "coordinates": polycoordinates}})

			with con:
				con.row_factory = lambda cursor, row: row[0]
				litecur = con.cursor()

				# attach the other values to the dict
				# TODO: investigate doing this purely in SQL

				litecur.execute(
					"SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=0",
					(record["local_identifier"], record["repository_url"]))
				record["dc:contributor.author"] = litecur.fetchall()

				litecur.execute(
					"SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=1",
					(record["local_identifier"], record["repository_url"]))
				record["dc:contributor"] = litecur.fetchall()

				litecur.execute("SELECT subject FROM subjects WHERE local_identifier=? AND repository_url=?",
								(record["local_identifier"], record["repository_url"]))
				record["dc:subject"] = litecur.fetchall()

				litecur.execute("SELECT rights FROM rights WHERE local_identifier=? AND repository_url=?",
								(record["local_identifier"], record["repository_url"]))
				record["dc:rights"] = litecur.fetchall()

				litecur.execute("SELECT description FROM descriptions WHERE local_identifier=? AND repository_url=?",
								(record["local_identifier"], record["repository_url"]))
				record["dc:description"] = litecur.fetchall()

				litecur.execute(
					"SELECT description FROM fra_descriptions WHERE local_identifier=? AND repository_url=?",
					(record["local_identifier"], record["repository_url"]))
				record["nrdr:description_fr"] = litecur.fetchall()

				litecur.execute("SELECT tag FROM tags WHERE local_identifier=? AND repository_url=? AND language='en'",
								(record["local_identifier"], record["repository_url"]))
				record["nrdr:tags"] = litecur.fetchall()

				litecur.execute("SELECT tag FROM tags WHERE local_identifier=? AND repository_url=? AND language='fr'",
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
			gmeta_data = {record["dc:source"]: {"mimetype": "application/json", "content": record}}
			gmeta.append(gmeta_data)

			records_assembled += 1

		self.logger.info("gmeta size: %s items" % (len(gmeta)))
		return gmeta

	def _validate_rifcs(self, rifcs):
		self.logger.info("Exporter: _validate_rifs called")
		with open('schema/registryObjects.xsd', 'r') as f:
			schema_root = etree.XML(f.read())
		schema = etree.XMLSchema(schema_root)
		# Generate parwser and fix any XML bugs from the templating.
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
					LIMIT ? OFFSET ?""", (rec_limit, rec_start))

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
					con.row_factory = lambda cursor, row: row[0]
					litecur = con.cursor()

					litecur.execute(
						"SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=0",
						(record["local_identifier"], record["repository_url"]))
					record["dc_contributor_author"] = litecur.fetchall()

					litecur.execute(
						"SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=1",
						(record["local_identifier"], record["repository_url"]))
					record["dc_contributor"] = litecur.fetchall()

					litecur.execute("SELECT subject FROM subjects WHERE local_identifier=? AND repository_url=?",
									(record["local_identifier"], record["repository_url"]))
					record["dc_subject"] = litecur.fetchall()

					litecur.execute("SELECT rights FROM rights WHERE local_identifier=? AND repository_url=?",
									(record["local_identifier"], record["repository_url"]))
					record["dc_rights"] = litecur.fetchall()

					litecur.execute(
						"SELECT description FROM descriptions WHERE local_identifier=? AND repository_url=?",
						(record["local_identifier"], record["repository_url"]))
					record["dc_description"] = litecur.fetchall()

					litecur.execute(
						"SELECT description FROM fra_descriptions WHERE local_identifier=? AND repository_url=?",
						(record["local_identifier"], record["repository_url"]))
					record["nrdr_fra_description"] = litecur.fetchall()

					litecur.execute("SELECT tag FROM tags WHERE local_identifier=? AND repository_url=? AND language='en'",
									(record["local_identifier"], record["repository_url"]))
					record["nrdr:tags"] = litecur.fetchall()

					litecur.execute("SELECT tag FROM tags WHERE local_identifier=? AND repository_url=? AND language='fr'",
									(record["local_identifier"], record["repository_url"]))
					record["nrdr:tags_fr"] = litecur.fetchall()

					litecur.execute(
						"SELECT coordinate_type, lat, lon FROM geospatial WHERE local_identifier=? AND repository_url=?",
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
			output = json.dumps({"_gmeta": self._generate_gmeta(export_batch_size, export_filepath, temp_filepath)})
		elif export_format == "rifcs":
			output = self._generate_rifcs()
		else:
			self.logger.error("Unknown export format: %s" % (export_format))

		if output:
			self._write_to_file(output, export_filepath, temp_filepath, export_format)