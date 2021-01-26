from harvester.HarvestRepository import HarvestRepository
import requests
import time
import json
import re
import os.path
from dateutil import parser


class OpenDataSoftRepository(HarvestRepository):
    """ OpenDataSoft Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "opendatasoft"
        super(OpenDataSoftRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.records_per_request = 50
        self.params = {
            "start": 0,
            "pageLength": self.records_per_request
        }
        self.query = "((*))"
        if "collection" in repoParams:
            coll = re.sub("[^a-zA-Z0-9_-]+", "", repoParams["collection"])  # Remove potentially bad chars
            self.query += "%2520AND%2520(coll:" + coll + ")"
        if "options" in repoParams:
            options = re.sub("[^a-zA-Z0-9_-]+", "", repoParams["options"])  # Remove potentially bad chars
            self.params["options"] = options

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "opendatasoft",
            "enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors,
            "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems,
            "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url,
            "repo_oai_name": self.repo_oai_name
        }
        self.repository_id = self.db.update_repo(**kwargs)

        try:
            offset = 0
            item_count = 0
            while True:
                self.params["start"] = offset
                payload = {"rows": self.records_per_request, "start": self.params["start"]}
                response = requests.get(self.url, params=payload,
                                        verify=False)  # Needs to be string not dict to force specific urlencoding
                records = response.json()
                if not records["datasets"]:
                    break
                for record in records["datasets"]:
                    item_identifier = record["datasetid"]
                    result = self.db.write_header(item_identifier, self.repository_id)
                    item_count = item_count + 1
                    if (item_count % self.update_log_after_numitems == 0):
                        tdelta = time.time() - self.tstart + 0.1
                        self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,
                                                                            item_count / tdelta))
                offset += self.records_per_request
            self.logger.info("Found {} items in feed".format(item_count))

            return True

        except Exception as e:
            self.logger.error("Updating OpenDataSoft Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def format_opendatasoft_to_oai(self, opendatasoft_record):
        record = {}
        record["identifier"] = opendatasoft_record["datasetid"]
        record["pub_date"] = parser.parse(opendatasoft_record["metas"]["modified"]).strftime('%Y-%m-%d')
        record["title"] = opendatasoft_record["metas"]["title"]
        record["description"] = opendatasoft_record["metas"].get("description", "")
        record["publisher"] = opendatasoft_record["metas"].get("publisher", "")

        if "data-owner" in opendatasoft_record["metas"] and opendatasoft_record["metas"]["data-owner"]:
            record["creator"] = opendatasoft_record["metas"]["data-owner"]
        else:
            record["creator"] = self.name

        record["tags"] = []
        if "keyword" in opendatasoft_record["metas"] and opendatasoft_record["metas"]["keyword"]:
            record["tags"].extend(opendatasoft_record["metas"]["keyword"])
        if "search-term" in opendatasoft_record["metas"] and opendatasoft_record["metas"]["search-term"]:
            for tag in opendatasoft_record["metas"]["search-term"].split(","):
                if tag not in record["tags"] and tag != "<div></div>":
                    record["tags"].append(tag.strip())

        record["subject"] = opendatasoft_record["metas"].get("theme", "")

        record["rights"] = [opendatasoft_record["metas"].get("license", "")]
        record["rights"].append(opendatasoft_record["metas"].get("license_url", ""))
        record["rights"] = "\n".join(record["rights"])
        record["rights"] = record["rights"].strip()

        if "data-team" in opendatasoft_record["metas"] and opendatasoft_record["metas"]["data-team"]:
            record["affiliation"] = opendatasoft_record["metas"]["data-team"]

        record["series"] = ""
        record["title_fr"] = ""

        if "territory" in opendatasoft_record["metas"]:
            record["geoplaces"] = []
            if isinstance(opendatasoft_record["metas"]["territory"], list):
                for territory in opendatasoft_record["metas"]["territory"]:
                    record["geoplaces"].append({"place_name": territory})

        record["geofiles"] = [{"uri": self.url.replace("datasets/1.0/search", "records/1.0/download?dataset=") + record["identifier"] + "&format=geojson",
                               "filename": ""}]

        return record

    def _update_record(self, record):
        try:
            record_url = self.url.replace("search", "") + record['local_identifier']
            try:
                item_response = requests.get(record_url)
                opendatasoft_record = json.loads(item_response.text)
            except:
                # Exception means this URL was not found
                self.db.delete_record(record)
                return True
            oai_record = self.format_opendatasoft_to_oai(opendatasoft_record)
            if oai_record:
                self.db.write_record(oai_record, self)
            return True
        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record['local_identifier'], e))
            if self.dump_on_failure == True:
                try:
                    print(opendatasoft_record)
                except:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

