from harvester.HarvestRepository import HarvestRepository
from harvester.rate_limited import rate_limited
import urllib
from dateutil import parser
import time
import json
import xml.etree.ElementTree as ET
import requests


class DataStreamRepository(HarvestRepository):
    """ DataStream Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "datastream"
        super(DataStreamRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.headers = {'accept': 'application/vnd.api+json'}

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "datastream",
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
            page_number = 1
            page_size = 1000
            totalPages = 1
            item_count = 0
            while page_number <= totalPages:
                querystring = {"client-id": self.set, "page[number]": str(page_number), "page[size]": str(page_size)}
                response = requests.request("GET", self.url, headers=self.headers, params=querystring)
                response = response.json()
                totalPages = response['meta']['totalPages']
                page_number += 1
                for record in response["data"]:
                    item_identifier = record["attributes"]["url"]
                    result = self.db.write_header(item_identifier, self.repository_id)
                    item_count = item_count + 1
                    if (item_count % self.update_log_after_numitems == 0):
                        tdelta = time.time() - self.tstart + 0.1
                        self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,
                                                                                                   self.formatter.humanize(
                                                                                                       tdelta),
                                                                                                   item_count / tdelta))
            self.logger.info("Found {} items in feed".format(item_count))

            return True

        except Exception as e:
            self.logger.error("Updating DataStream Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def format_datastream_to_oai(self, datastream_dcat_json):

        datastream_record = datastream_dcat_json

        record = {}

        record["series"] = ""
        record["title_fr"] = ""

        if ("name" in datastream_record) and datastream_record["name"]:
            record["title"] = datastream_record["name"]

        if ("description" in datastream_record) and datastream_record["description"]:
            record["description"] = datastream_record["description"]

        if ("creator" in datastream_record) and datastream_record["creator"]:
            if ("name" in datastream_record["creator"]) and datastream_record["creator"]["name"]:
                    record["creator"] = datastream_record["creator"]["name"]

        if ("keywords" in datastream_record) and datastream_record["keywords"]:
            if isinstance(datastream_record["keywords"], str):
                record["tags"] = datastream_record["keywords"].split(",")

        if ("publisher" in datastream_record) and datastream_record["publisher"]:
            if ("name" in datastream_record["publisher"]) and datastream_record["publisher"]["name"]:
                record["publisher"] = datastream_record["publisher"]["name"]

        if ("datePublished" in datastream_record) and datastream_record["datePublished"]:
            record["pub_date"] = parser.parse(datastream_record["datePublished"]).strftime('%Y-%m-%d')

        if ("identifier" in datastream_record) and datastream_record["identifier"]:
            if ("url" in datastream_record["identifier"]) and datastream_record["identifier"]["url"]:
                record["item_url"] = datastream_record['identifier']["url"]

        if "isAccessibleForFree" in datastream_record:
            if datastream_record["isAccessibleForFree"]:
                record["access"] = "Public"
            else:
                record["access"] = "Limited"

        if ("license" in datastream_record) and datastream_record["license"]:
            record["rights"] = datastream_record["license"]

        if ("url" in datastream_record) and datastream_record["url"]:
            record["identifier"] = datastream_record["url"]

        if ("spatialCoverage" in datastream_record) and datastream_record["spatialCoverage"]:
            try:
                boxcoordinates = datastream_record["spatialCoverage"]["geo"]["box"].split()
                if len(boxcoordinates) == 4:
                    record["geobboxes"] = [{"westLon": boxcoordinates[0], "southLat": boxcoordinates[1],
                                            "eastLon": boxcoordinates[2], "northLat": boxcoordinates[3]}]
            except:
                pass

        return record

    @rate_limited(5)
    def _update_record(self, record):
        try:
            identifier = record['local_identifier']
            item_dcat_json_url = identifier + ".dcat.json"
            try:
                item_response = urllib.request.urlopen(item_dcat_json_url)
            except Exception as e:
                # Exception means this URL was not found
                self.db.delete_record(record)
                return True
            item_response_content = item_response.read().decode('utf-8')
            item_json = json.loads(item_response_content)

            oai_record = self.format_datastream_to_oai(item_json)
            if oai_record:
                self.db.write_record(oai_record, self)
            return True

        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record['local_identifier'], e))
            if self.dump_on_failure == True:
                try:
                    print(item_response_content)
                except:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False



