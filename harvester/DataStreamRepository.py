from harvester.HarvestRepository import HarvestRepository
import requests
import time
import json
import re
import os.path
import xml.etree.ElementTree as ET


class DataStreamRepository(HarvestRepository):
    """ DataStream Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "marklogic"
        super(DataStreamRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []

        # TODO: I think we can remove this
        self.records_per_request = 50
        self.params = {
            "format": "json",
            "start": 0,
            "pageLength": self.records_per_request
        }

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "datastream",
            "enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors,
            "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems,
            "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url
        }
        self.repository_id = self.db.update_repo(**kwargs)

        try:
            # TODO: Update with requests.get('https://datastream.org/dataset/sitemap.xml'). This gives me a 404, but curl works.
            response = 'sitemap.xml'
            response_xml = ET.parse(response)
            root = response_xml.getroot()
            results = []

            for child in root:
                item_url = child[0].text
                item_dcat_json = item_url + ".dcat.json"
                record = item_dcat_json
                #record = requests.get(item_dcat_json)
                results.append(record)
                # TODO: For each item, get the schema.org (in <script type=application/ld+json>). Still having 404 issues.

            for record in results:
                oai_record = self.format_datastream_to_oai((record))
                if oai_record:
                    self.db.write_record(oai_record, self)

            return True

        except Exception as e:
            self.logger.error("Updating DataStream Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def format_datastream_to_oai(self, datastream_record):

        # TODO: Cut this; used for example only while requests isn't working
        with open("0b928801-48be-4451-8399-62538adb1515.dcat.json") as datastream_file:
            datastream_record = json.load(datastream_file)

            record = {}

            record["contact"] = self.contact
            record["series"] = ""

            if ("name" in datastream_record) and datastream_record["name"]:
                record["title"] = datastream_record["name"]

            if ("description" in datastream_record) and datastream_record["description"]:
                record["description"] = datastream_record["description"]

            if ("author" in datastream_record) and datastream_record["author"]:
                if ("name" in datastream_record["author"]) and datastream_record["author"]["name"]:
                        record["creator"] = datastream_record["author"]["name"]

            if ("keywords" in datastream_record) and datastream_record["keywords"]:
                if isinstance(datastream_record["keywords"], str):
                    record["tags"] = datastream_record["keywords"].split(",")

            if ("publisher" in datastream_record) and datastream_record["publisher"]:
                if ("name" in datastream_record["publisher"]) and datastream_record["publisher"]["name"]:
                    record["publisher"] = datastream_record["publisher"]["name"]

            if ("datePublished" in datastream_record) and datastream_record["datePublished"]:
                record["pub_date"] = datastream_record["datePublished"][:10] # TODO: Better date parsing

            if ("identifier" in datastream_record) and datastream_record["identifier"]:
                if ("url" in datastream_record["identifier"]) and datastream_record["identifier"]["url"]:
                    record["item_url"] = datastream_record['identifier']["url"]

            if "isAccessibleForFree" in datastream_record:
                if datastream_record["isAccessibleForFree"]:
                    record["access"] = "Public"
                else:
                    record["access"] = "Limited"

            if ("@id" in datastream_record) and datastream_record["@id"]:
                record["identifier"] = datastream_record["@id"]

            # TODO: Update later when updating for Geodisy
            #record["geospatial"] = datastream_record["spatialCoverage"]["geo"]["box"]

            return record

    def _update_record(self, record):
        # There is no update for individual records, they are updated on full crawl
        return True
