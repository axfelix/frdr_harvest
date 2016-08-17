import time
import re

class Exporter(object):
	""" Read records from the database and export to given formats """

	def __init__(self, db, log):
		self.db = db
		self.logger = log


	def construct_local_url(self, record):
		# Check if the local_identifier has already been turned into a url
		if "http" in record["local_identifier"].lower():
			return record["local_identifier"]

		# Check for OAI format of identifier (oai:domain:id)
		oai_id = None
		oai_search = re.search("oai:(.+):(.+)", record["local_identifier"])
		if oai_search:
			oai_id = oai_search.group(2)
			# TODO: determine if this is needed for all repos, or just SFU?
			oai_id = oai_id.replace("_",":")

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
		local_url = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?", record["local_identifier"])
		if local_url: 
			return local_url.group(0)

		local_url = None
		return local_url


	def generate_gmeta(self):
		self.logger.info("Exporter: generate_gmeta called")
		con = self.db.getConnection()
		gmeta = []
		
		# Only select records that have complete data
		records = con.execute("""SELECT recs.title, recs.date, recs.contact, recs.series, recs.source_url, recs.deleted, recs.local_identifier, recs.repository_url, repos.repository_name as "nrdr:origin.id", repos.repository_thumbnail as "nrdr:origin.icon", repos.item_url_pattern
				FROM records recs, repositories repos 
				WHERE recs.title != '' and recs.repository_url = repos.repository_url """)

		for record in records:
			record = dict(zip([tuple[0] for tuple in records.description], record))
			record["dc:source"] = self.construct_local_url(record)
			if record["dc:source"] is None:
				continue

			if record["deleted"] == 1:
				gmeta_data = {record["dc:source"] : {"mimetype": "application/json", "content": None}}
				gmeta.append(gmeta_data)
				continue

			with con:
				con.row_factory = lambda cursor, row: row[0]
				litecur = con.cursor()

				# attach the other values to the dict
				# TODO: investigate doing this purely in SQL

				litecur.execute("SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=0", (record["local_identifier"], record["repository_url"]))
				record["dc:contributor.author"] = litecur.fetchall()

				litecur.execute("SELECT creator FROM creators WHERE local_identifier=? AND repository_url=? AND is_contributor=1", (record["local_identifier"], record["repository_url"]))
				record["dc:contributor"] = litecur.fetchall()

				litecur.execute("SELECT subject FROM subjects WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["dc:subject"] = litecur.fetchall()

				litecur.execute("SELECT rights FROM rights WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["dc:rights"] = litecur.fetchall()

				litecur.execute("SELECT description FROM descriptions WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["dc:description"] = litecur.fetchall()

				litecur.execute("SELECT description FROM fra_descriptions WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["nrdr:fra_description"] = litecur.fetchall()

				litecur.execute("SELECT tag FROM tags WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["nrdr:tags"] = litecur.fetchall()

				litecur.execute("SELECT coordinate_type, lat, lon FROM geospatial WHERE local_identifier=? AND repository_url=?", (record["local_identifier"], record["repository_url"]))
				record["nrdr:geospatial"] = litecur.fetchall()

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

			record["@context"] = {"dc" : "http://dublincore.org/documents/dcmi-terms", "nrdr" : "http://nrdr-ednr.ca/schema/1.0/"}
			gmeta_data = {record["dc:source"] : {"mimetype": "application/json", "content": record}}
			gmeta.append(gmeta_data)
			
		self.logger.info("gmeta size: %s items" % (len(gmeta)) )
		return gmeta


