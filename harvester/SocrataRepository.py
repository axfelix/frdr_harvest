from harvester.HarvestRepository import HarvestRepository
from harvester.rate_limited import rate_limited
from sodapy import Socrata
from datetime import datetime
import time
import json
import re
import os.path


class SocrataRepository(HarvestRepository):
    """ Socrata Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "socrata"
        super(SocrataRepository, self).setRepoParams(repoParams)
        # sodapy doesn't like http/https preceding URLs
        self.socratarepo = Socrata(self.url, self.socrata_app_token)
        self.domain_metadata = []


    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name, "repo_type": "socrata", 
            "enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors, "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems, "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url,
            "repo_oai_name": self.repo_oai_name
        }
        self.repository_id = self.db.update_repo(**kwargs)
        records = self.socratarepo.datasets()

        item_count = 0
        for rec in records:
            result = self.db.write_header(rec["resource"]["id"], self.repository_id)
            item_count = item_count + 1
            if (item_count % self.update_log_after_numitems == 0):
                tdelta = time.time() - self.tstart + 0.1
                self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count, self.formatter.humanize(tdelta), item_count/tdelta) )

        self.logger.info("Found {} items in feed".format(item_count) )

    def format_socrata_to_oai(self, socrata_record, local_identifier):
        record = {}

        record["title"] = socrata_record.get("name","").strip()
        record["description"] = socrata_record.get("description", "")
        record["tags"] = socrata_record.get("tags", "")
        record["identifier"] = local_identifier
        record["creator"] = socrata_record.get("attribution", self.name)
        record["pub_date"] = datetime.fromtimestamp(socrata_record["publicationDate"]).strftime("%Y-%m-%d")
        record["subject"] = socrata_record.get("category", "")
        record["title_fr"] = ""
        record["series"] = ""
        record["rights"] = []

        if ("license" in socrata_record) and socrata_record["license"]:
            # Winnipeg, Nova Scotia, PEI
            record["rights"].append(socrata_record["license"].get("name", ""))
            record["rights"].append(socrata_record["license"].get("termsLink", ""))
            record["rights"] = "\n".join(record["rights"])
            record["rights"] = record["rights"].strip()

        if record["rights"] == "See Terms of Use":
            # Calgary, Edmonton
            record["rights"] = []

        if ("metadata" in socrata_record) and socrata_record["metadata"]:
            if ("custom_fields" in socrata_record["metadata"]) and socrata_record["metadata"]["custom_fields"]:
                # Rights metadata
                if ("License/Attribution" in socrata_record["metadata"]["custom_fields"]) and socrata_record["metadata"]["custom_fields"]["License/Attribution"]:
                    if ("License URL" in socrata_record["metadata"]["custom_fields"]["License/Attribution"] and socrata_record["metadata"]["custom_fields"]["License/Attribution"]["License URL"]):
                        if record["rights"] == "" or record["rights"] == []:
                            # Calgary
                            record["rights"] = socrata_record["metadata"]["custom_fields"]["License/Attribution"]["License URL"]
                    if ("License-URL" in socrata_record["metadata"]["custom_fields"]["License/Attribution"] and socrata_record["metadata"]["custom_fields"]["License/Attribution"]["License-URL"]):
                        if record["rights"] == "" or record["rights"] == []:
                            # Calgary
                            record["rights"] = socrata_record["metadata"]["custom_fields"]["License/Attribution"]["License-URL"]
                elif ("Licence" in socrata_record["metadata"]["custom_fields"]) and socrata_record["metadata"]["custom_fields"]["Licence"]:
                    if ("Licence" in socrata_record["metadata"]["custom_fields"]["Licence"]) and socrata_record["metadata"]["custom_fields"]["Licence"]["Licence"]:
                        if record["rights"] == "" or record["rights"] == []:
                            # Winnipeg
                            record["rights"] = socrata_record["metadata"]["custom_fields"]["Licence"]["Licence"]
                elif ("Attributes" in socrata_record["metadata"]["custom_fields"]) and socrata_record["metadata"]["custom_fields"]["Attributes"]:
                    if ("Licence" in socrata_record["metadata"]["custom_fields"]["Attributes"]) and socrata_record["metadata"]["custom_fields"]["Attributes"]["Licence"]:
                        if record["rights"] == "" or record["rights"] == []:
                            # Strathcona
                            record["rights"] = socrata_record["metadata"]["custom_fields"]["Attributes"]["Licence"]

            if ("geo" in socrata_record["metadata"]) and socrata_record["metadata"]["geo"]:
                if ("bbox" in socrata_record["metadata"]["geo"]) and socrata_record["metadata"]["geo"]["bbox"]:
                    bbox_array = socrata_record["metadata"]["geo"]["bbox"].split(",")
                    record["geobboxes"] = [{"westLon": bbox_array[0], "eastLon": bbox_array[2], "southLat": bbox_array[1], "northLat": bbox_array[3]}]


        if record["rights"] == "" or record["rights"] == []:
            record.pop("rights")

        #record["geofiles"] = [{"uri": "https://" + self.url + "/api/geospatial/" + record["identifier"] + "?method=export&format=Shapefile", "filename": ""}] # this doesn't work for all datasets

        record["geofiles"] = [{"uri": "https://" + self.url + "/resource/" + record["identifier"] + ".csv", "filename": record["identifier"] + ".csv"}]

        # Continue to default to English for our current Socrata repositories.
        # For Nova Scoatia, "fra" language refers to the dataset, not the metadata.
        
        # language = self.default_language
        # if "metadata" in socrata_record:
        #     if "custom_fields" in socrata_record["metadata"]:
        #         if "Detailed Metadata" in socrata_record["metadata"]["custom_fields"]:
        #             if "Language" in socrata_record["metadata"]["custom_fields"]["Detailed Metadata"]:
        #                 # Nova Scotia
        #                 language = socrata_record["metadata"]["custom_fields"]["Detailed Metadata"]["Language"]
        #         elif "Dataset Information" in socrata_record["metadata"]["custom_fields"]:
        #             if "Language" in socrata_record["metadata"]["custom_fields"]["Dataset Information"]:
        #                 # Prince Edward Island
        #                 language = socrata_record["metadata"]["custom_fields"]["Dataset Information"]["Language"]
        # language = language.lower()
        #
        # if language in ["fr", "fre", "fra", "french"]:
        #     language = "fr"
        # else:
        #     language = "en"

        return record


    @rate_limited(5)
    def _update_record(self,record):

        try:            
            socrata_record = self.socratarepo.get_metadata(record["local_identifier"])
            oai_record = self.format_socrata_to_oai(socrata_record,record["local_identifier"])
            if oai_record:
                self.db.write_record(oai_record, self)
            return True

        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record["local_identifier"], e))
            if self.dump_on_failure == True:
                try:
                    print(socrata_record)
                except:
                    pass
            if "404" in str(e):
                # The record was deleted from source - "404 Client Error: Not Found"
                self.db.delete_record(record)
            else:
                # Some other possibly transient error
                # Touch the record so we do not keep requesting it on every run
                self.db.touch_record(record)
                self.error_count = self.error_count + 1
                if self.error_count < self.abort_after_numerrors:
                    return True

        return False

