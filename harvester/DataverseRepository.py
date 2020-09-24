from harvester.HarvestRepository import HarvestRepository
import requests
import time
import json
import re
import os.path
from dateutil import parser


class DataverseRepository(HarvestRepository):
    """ DataverseRepository Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "dataverse"
        super(DataverseRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.params = {
        }

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "dataverse",
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
            if self.set == "":
                # If set is not specified, get the entire dataverse (:root)
                dataverse_id = ":root"
            else:
                # Otherwise, use the specified set as the dataverse_id
                dataverse_id = self.set
            dataverse_hierarchy = self.name
            item_count = self.get_datasets_from_dataverse_id(dataverse_id, dataverse_hierarchy, 0)
            self.logger.info("Found {} items in feed".format(item_count))
            return True
        except Exception as e:
            self.logger.error("Updating Dataverse Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def get_datasets_from_dataverse_id(self, dataverse_id, dataverse_hierarchy, item_count):
        response = requests.get(self.url.replace("%id%", str(dataverse_id)), verify=False)
        records = response.json()
        parent_dataverse_hierarchy = dataverse_hierarchy
        for record in records["data"]:
            if record["type"] == "dataset":
                item_identifier = record["id"]
                combined_identifier = str(item_identifier)
                dataverse_hierarchy_split = [x.strip() for x in dataverse_hierarchy.split(" // ")]
                if len(dataverse_hierarchy_split) > 1:
                    # Write dataverse_hierarchy - minus the repository name - plus identifier as local_identifier
                    dataverse_hierarchy_string = " // ".join(dataverse_hierarchy_split[1:])
                    combined_identifier = combined_identifier + " // " + dataverse_hierarchy_string
                result = self.db.write_header(combined_identifier, self.repository_id)
                item_count = item_count + 1
                if (item_count % self.update_log_after_numitems == 0):
                    tdelta = time.time() - self.tstart + 0.1
                    self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,self.formatter.humanize(
                                                                                               tdelta),item_count / tdelta))
            elif record["type"] == "dataverse":
                # Append the dataverse name to the overall dataverse_hierarchy
                dataverse_hierarchy = parent_dataverse_hierarchy + " // " + record["title"]
                # Recursive call to get children of this dataverse
                item_count = self.get_datasets_from_dataverse_id(record["id"], dataverse_hierarchy, item_count)
        return item_count

    def format_dataverse_to_oai(self, dataverse_record):
        record = {}
        record["identifier"] = dataverse_record["combined_identifier"]
        record["pub_date"] = dataverse_record["publicationDate"]
        record["item_url"] = dataverse_record["persistentUrl"]

        identifier_split = [x.strip() for x in dataverse_record["combined_identifier"].split(" // ")]

        if len(identifier_split) == 1:
            # dataset is direct child of repository
            record["series"] = "" # no sub-dataverses
        else:
            # dataset is direct child of sub-dataverse(s)
            record["series"] = " // ".join(identifier_split[1:]) # sub-dataverses

        if "latestVersion" not in dataverse_record:
            # Dataset is deaccessioned
            record["deleted"] = 1
            record["title"], record["title_fr"] = "", ""
            return record

        if dataverse_record["latestVersion"]["license"] != "NONE":
            # Add license information to rights, if available
            record["rights"] = dataverse_record["latestVersion"]["license"]

        if dataverse_record["latestVersion"]["fileAccessRequest"]:
            # fileAccessRequest = True indicates at least one file is restricted
            record["access"] = "Restricted"
        else:
            record["access"] = "Public"

        # Citation block
        record["description"] = []
        record["tags"] = []
        default_language = "English"
        for citation_field in dataverse_record["latestVersion"]["metadataBlocks"]["citation"]["fields"]:
            if citation_field["typeName"] == "title":
                record["title"] = citation_field["value"]
            elif citation_field["typeName"] == "author":
                record["creator"] = []
                for creator in citation_field["value"]:
                    record["creator"].append(creator["authorName"]["value"])
            elif citation_field["typeName"] == "dsDescription":
                for description in citation_field["value"]:
                    record["description"].append(description["dsDescriptionValue"]["value"])
            elif citation_field["typeName"] == "subject":
                record["subject"] = citation_field["value"]
            elif citation_field["typeName"] == "keyword":
                for keyword in citation_field["value"]:
                    if "keywordValue" in keyword:
                        record["tags"].append(keyword["keywordValue"]["value"])
                    elif "keywordVocabulary" in keyword:
                        record["tags"].append(keyword["keywordVocabulary"]["value"])
            elif citation_field["typeName"] == "topicClassification":
                for keyword in citation_field["value"]:
                    record["tags"].append(keyword["topicClassValue"]["value"])
            elif citation_field["typeName"] == "series":
                if "seriesName" in citation_field["value"]:
                    if record["series"] and len(record["series"]) > 0:
                        # If there is already a series from a sub-dataverse, append the seriesName separated by space-colon-space
                        record["series"] = record["series"] + " : " + citation_field["value"]["seriesName"]["value"]
                    else:
                        # Otherwise, set series to the seriesName
                        record["series"] = citation_field["value"]["seriesName"]["value"]
            elif citation_field["typeName"] == "language":
                for language in citation_field["value"]:
                    # If any language is "French", infer that the default_language for the metadata is French
                    if language == "French":
                        default_language = "French"
            elif citation_field["typeName"] == "notesText":
                record["description"].append(citation_field["value"])
            elif citation_field["typeName"] == "contributor":
                record["contributor"] = []
                for contributor in citation_field["value"]:
                    if "contributorName" in contributor:
                        record["contributor"].append(contributor["contributorName"]["value"])
            elif citation_field["typeName"] == "productionDate":
                # If the record has a productionDate, prefer this over the publicationDate
                record["pub_date"] = citation_field["value"]

        if default_language == "French":
            # Swap title, description, tags
            record["title_fr"] = record["title"]
            record["title"] = ""
            record["description_fr"] = record["description"]
            record["description"] = ""
            record["tags_fr"] = record["tags"]
            record["tags"] = ""
        else:
            record["title_fr"] = ""

        # Geospatial block
        if "geospatial" in dataverse_record["latestVersion"]["metadataBlocks"]:
            if "fields" in dataverse_record["latestVersion"]["metadataBlocks"]["geospatial"]:
                for geospatial_field in dataverse_record["latestVersion"]["metadataBlocks"]["geospatial"]["fields"]:
                    if geospatial_field["typeName"] == "geographicCoverage":
                        # Get country, state, city, and otherGeographicCoverage
                        geolocationPlaces = []
                        for geographicCoverage in geospatial_field["value"]:
                            geolocationPlace = {}
                            if "country" in geographicCoverage:
                                geolocationPlace["country"] = geographicCoverage["country"]["value"]
                            if "state" in geographicCoverage:
                                geolocationPlace["state"] = geographicCoverage["state"]["value"]
                            if "city" in geographicCoverage:
                                geolocationPlace["city"] = geographicCoverage["city"]["value"]
                            if "otherGeographicCoverage" in geographicCoverage:
                                geolocationPlace["other"] = geographicCoverage["otherGeographicCoverage"]["value"]
                            geolocationPlaces.append(geolocationPlace)
                    elif geospatial_field["typeName"] == "geographicBoundingBox":
                        # Get bounding box values
                        geolocationBoxes = []
                        for geographicBoundingBox in geospatial_field["value"]:
                            geolocationBox = {}
                            if "westLongitude" in geographicBoundingBox:
                                geolocationBox["westLongitude"] = geographicBoundingBox["westLongitude"]["value"]
                            if "eastLongitude" in geographicBoundingBox:
                                geolocationBox["eastLongitude"] = geographicBoundingBox["eastLongitude"]["value"]
                            if "northLongitude" in geographicBoundingBox:
                                geolocationBox["northLatitude"] = geographicBoundingBox["northLongitude"]["value"]
                            if "southLongitude" in geographicBoundingBox:
                                geolocationBox["southLatitude"] = geographicBoundingBox["southLongitude"]["value"]
                            geolocationBoxes.append(geolocationBox)

        return record

    def _update_record(self, record):
        try:
            identifier_split = record['local_identifier'].split(" // ")
            item_identifier = identifier_split[0]
            record_url = self.url.replace("dataverses/%id%/contents", "datasets/") + item_identifier
            try:
                item_response = requests.get(record_url)
                dataverse_record = item_response.json()["data"]
                dataverse_record["combined_identifier"] = record['local_identifier']
            except:
                # Exception means this URL was not found
                self.db.delete_record(record)
                return True
            oai_record = self.format_dataverse_to_oai(dataverse_record)
            if oai_record:
                self.db.write_record(oai_record, self)
                if "deleted" in oai_record:
                    # This record has been deaccessioned, remove it from the results
                    self.db.delete_record(record)
            else:
                    # Some other problem, this record will be updated by a future crawl
                    self.db.touch_record(record)
            return True
        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record['local_identifier'], e))
            if self.dump_on_failure == True:
                try:
                    print(dataverse_record)
                except:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

