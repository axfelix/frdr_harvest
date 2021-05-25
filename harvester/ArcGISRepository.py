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
        self.default_language = "en"
        super(ArcGISRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []

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
            query_url = self.url
            response = requests.get(query_url)
            records = response.json()
            self.logger.info("Found {} items in feed".format(len(records["dataset"])))
            item_count = 0
            for record in records["dataset"]:
                oai_record = self.format_arcgis_to_oai(record)
                if oai_record:
                    item_count = item_count + 1
                    self.db.write_record(oai_record, self)
            self.logger.info("Wrote {} items from feed".format(item_count))


        except Exception as e:
            self.logger.error("Updating ArcGIS Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def format_arcgis_to_oai(self, arcgis_record):
        record = {}

        # Exclude non-dataset records
        if arcgis_record["@type"] != 'dcat:Dataset':
            # note: this is always coming back as dcat:Dataset so far, even for "Documents"
            return False

        if "distribution" in arcgis_record:
            is_dataset = False
            for distribution in arcgis_record["distribution"]:
                if distribution['title'] not in ['ArcGIS Hub Dataset', 'Esri Rest API']:
                    # if there is at least one additional distribution type, count as a dataset
                    is_dataset = True
                    break
            if not is_dataset:
                return False

        if self.default_language == "fr":
            record["title_fr"] = arcgis_record["title"]
            record["title"] = ""
            record["description_fr"] = arcgis_record["description"]
            record["tags_fr"] = arcgis_record["keyword"]
        else:
            record["title"] = arcgis_record["title"]
            record["description"] = arcgis_record["description"]
            record["tags"] = arcgis_record["keyword"]
            record["title_fr"] = ""

        record["identifier"] = arcgis_record["identifier"]
        record["item_url"] = arcgis_record["landingPage"]
        record["creator"] = arcgis_record["publisher"]["name"] # note: no field for individual authors
        record["series"] = ""

        if "accessLevel" in arcgis_record and arcgis_record["accessLevel"].lower() == "public":
            record["access"] = "Public"

        if "spatial" in arcgis_record and arcgis_record["spatial"]:
            if len(arcgis_record["spatial"].split(",")) == 4:
                coordinates = arcgis_record["spatial"].split(",")
                record["geobboxes"] = [{"westLon": coordinates[0], "southLat": coordinates[1], "eastLon": coordinates[2], "northLat": coordinates[3]}]

        if "issued" in arcgis_record and arcgis_record["issued"]:
            record["pub_date"] = parser.parse(arcgis_record["issued"]).strftime('%Y-%m-%d')

        # The information in the "license" field is a link to a license that returns a 404
        # if "license" in arcgis_record and arcgis_record["license"]:
        #     record["rights"] = arcgis_record["license"]

        # Look for Shapefiles or GeoJSON files
        arcgis_shapefile = False
        arcgis_geojson = False
        for distribution in arcgis_record["distribution"]:
            if distribution["title"] == "GeoJSON":
                arcgis_geojson = distribution
            elif distribution["title"] == "Shapefile":
                arcgis_shapefile = distribution

        # Prefer Shapefile over GeoJSON
        arcgis_geofile = ""
        if arcgis_shapefile:
            arcgis_geofile = arcgis_shapefile
        elif arcgis_geojson:
            arcgis_geofile = arcgis_geojson

        if arcgis_geofile:
            url = arcgis_geofile["accessURL"].split("?")[0]  # remove any query parameters
            filename = url.split("/")[len(url.split("/")) - 1]  # get the last part after the slash
            record["geofiles"] = [{"uri": url, "filename": filename}]

        return record

    def _update_record(self, record):
        # There is no update for individual records, they are updated on full crawl
        return True
