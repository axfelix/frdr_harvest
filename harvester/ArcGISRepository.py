from harvester.HarvestRepository import HarvestRepository
import requests
import time
import json
import re
import os.path
from dateutil import parser


class ArcGISRepository(HarvestRepository):
    """ ArcGISRepository Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "arcgis"
        super(ArcGISRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.records_per_request = 1000
        self.params = {
            "start": 1,
            "pageLength": self.records_per_request
        }

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "arcgis",
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
            page = self.params["start"]
            while True:
                query_url = self.url + "?page[number]=" + str(page) + "&page[size]=100&filter[source]=" + self.set
                response = requests.get(query_url)
                records = response.json()
                if not records["data"]:
                    break
                for record in records["data"]:
                    oai_record = self.format_arcgis_to_oai(record)
                    if oai_record:
                        self.db.write_record(oai_record, self)
                page = page + 1

            return True

        except Exception as e:
            self.logger.error("Updating ArcGIS Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def format_arcgis_to_oai(self, arcgis_record):
        record = {}

        # Exclude non-dataset records
        if arcgis_record["attributes"]["dataType"] == "Document":
            return False

        record["rights"] = arcgis_record["attributes"]["licenseInfo"]

        # Use licenseInfo URL as proxy for language of dataset for Ottawa
        # TODO: Merge English and French records for Ottawa
        if "https://ottawa.ca/fr/" in record["rights"]:
            record["title"]  = ""
            record["title_fr"] = record["rights"]
            record["tags_fr"] = arcgis_record["attributes"]["tags"]
            record["description_fr"] = arcgis_record["attributes"]["description"]
        else:
            # For all other repositories, default to English
            record["title"] = arcgis_record["attributes"]["name"]
            record["title_fr"] = ""
            record["tags"] = arcgis_record["attributes"]["tags"]
            record["description"] = arcgis_record["attributes"]["description"]

        record["identifier"] = arcgis_record["id"]
        record["title"] = arcgis_record["attributes"]["name"]
        record["creator"] = arcgis_record["attributes"]["source"]
        record["pub_date"] = parser.parse(arcgis_record["attributes"]["createdAt"]).strftime('%Y-%m-%d')

        record["local_identifier"] = arcgis_record["attributes"]["slug"]
        # Use URL constructed from slug, not arcgis_record["attributes"]["landingPage"]
        # Records without a URL pointing to the local repository are excluded
        try:
            slug = record["local_identifier"].split("::")[1].replace("’", "").replace("!", "").lower()
            local_item_url = self.homepage_url + "datasets/" + slug
            response = requests.request("GET", local_item_url)
            if response.status_code == 200 and response.url != self.homepage_url + "404":
                record["item_url"] = local_item_url
            else:
                return False
        except:
            pass

        # record["geospatial"] = arcgis_record["extent"] # TODO: Update for new schema

        record["series"] = ""
        record["access"] = "Public"

        return record

    def _update_record(self, record):
        # There is no update for individual records, they are updated on full crawl
        return True
