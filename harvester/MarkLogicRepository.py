from harvester.HarvestRepository import HarvestRepository
from functools import wraps
import requests
import time
import json
import re
import os.path


class MarkLogicRepository(HarvestRepository):
	""" MarkLogic Repository """

	def setRepoParams(self, repoParams):
		self.metadataprefix = "marklogic"
		super(MarkLogicRepository, self).setRepoParams(repoParams)
		self.domain_metadata = []


	def _crawl(self):
		self.repository_id = self.db.update_repo(self.repository_id, self.url, self.set, self.name, "marklogic", self.enabled, self.thumbnail, self.item_url_pattern,self.abort_after_numerrors,self.max_records_updated_per_run,self.update_log_after_numitems,self.record_refresh_days,self.repo_refresh_days)

		try:
			offset = 0
			while True:
				offset_url = re.sub("%26start%3D\d+", ("%26start%3D" + str(offset)), self.url)
				records = requests.get(offset_url, verify=False).json()
				if not records["results"]:
					break
				for record in records["results"]:
					oai_record = self.format_marklogic_to_oai(record)
					if oai_record:
						self.db.write_record(oai_record, self.repository_id, self.metadataprefix.lower(), self.domain_metadata)
				offset += 10
			return True

		except Exception as e:
			self.logger.error("Updating MarkLogic Repository failed: %s" % (str(e)) )
			self.error_count =  self.error_count + 1
			if self.error_count < self.abort_after_numerrors:
				return True

		return False


	def format_marklogic_to_oai(self, marklogic_record):
		record = {}

		for entry in marklogic_record["metadata"]:
			record["creators"] = []
			if "AuthEnty" in entry:
				record["creators"].append(entry["AuthEnty"].strip())
			if "abstract" in entry:
				record["description"] = entry["abstract"].strip()
			if "TI-facet" in entry:
				record["title"] = entry["TI-facet"].strip()
			if "date" in entry:
				record["pub_date"] = str(entry["date"]).strip()
		record["identifier"] = marklogic_record["uri"].rsplit('/', 1)[1]
		record["contact"] = self.contact
		record["series"] = ""

		return record


	def _update_record(self,record):
		if not self.updated_this_run:
			self._crawl()
			self.updated_this_run = True