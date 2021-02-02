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
        self.geofile_extensions = [".tif", ".tiff",".xyz", ".png", ".aux.xml",".tab",".twf",".tifw", ".tiffw",".wld",
                                  ".tif.prj",".tfw", ".geojson",".shp",".gpkg", ".shx", ".dbf", ".sbn",".prj", ".csv", ".txt", ".zip"]

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
            "repo_oai_name": self.repo_oai_name,
            "dataverses_list": self.dataverses_list # only retrieve these sub-dataverses; defaults to None
        }
        self.repository_id = self.db.update_repo(**kwargs)

        try:
            dataverse_id = ":root" # If set is not specified, get the entire dataverse (:root)
            if self.set != "":
                # If a single set is specified, use the specified set as the dataverse_id
                dataverse_id = self.set
            item_count = self.get_datasets_from_dataverse_id(dataverse_id, str(dataverse_id), 0, self.dataverses_list)
            self.logger.info("Found {} items in feed".format(item_count))
            return True
        except Exception as e:
            self.logger.error("Updating Dataverse Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def get_datasets_from_dataverse_id(self, dataverse_id, dataverse_hierarchy, item_count, dataverses_list=None):
        response = requests.get(self.url.replace("%id%", str(dataverse_id)), verify=False)
        records = response.json()
        for record in records["data"]:
            if record["type"] == "dataset":
                item_identifier = record["id"]
                combined_identifier = str(item_identifier)
                dataverse_hierarchy_split = [x.strip() for x in dataverse_hierarchy.split("_")]
                if len(dataverse_hierarchy_split) > 1:
                    # Write dataverse_hierarchy - minus the repository id - plus identifier as local_identifier
                    dataverse_hierarchy_string = "_".join(dataverse_hierarchy_split[1:])
                    combined_identifier = combined_identifier + "_" + dataverse_hierarchy_string
                result = self.db.write_header(combined_identifier, self.repository_id)
                item_count = item_count + 1
                if (item_count % self.update_log_after_numitems == 0):
                    tdelta = time.time() - self.tstart + 0.1
                    self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,self.formatter.humanize(
                                                                                               tdelta),item_count / tdelta))
            elif record["type"] == "dataverse":
                if dataverses_list and record["id"] not in dataverses_list:
                    # If a dataverses_list is specified, ignore any dataverses not in it
                    pass
                else:
                    # Recursive call to get children of this dataverse
                    # Append the dataverse id to the overall dataverse_hierarchy
                    item_count = self.get_datasets_from_dataverse_id(record["id"], dataverse_hierarchy + "_" + str(record["id"]), item_count)
        return item_count

    def get_dataverse_name_from_dataverse_id(self, dataverse_id):
        try:
            response = requests.get(self.url.replace("%id%/contents", str(dataverse_id)), verify=False)
            record = response.json()
            return record["data"]["name"]
        except Exception as e:
            print(e)


    def format_dataverse_to_oai(self, dataverse_record):
        record = {}
        record["identifier"] = dataverse_record["combined_identifier"]
        record["pub_date"] = dataverse_record["publicationDate"]
        record["item_url"] = dataverse_record["persistentUrl"]

        identifier_split = [x.strip() for x in dataverse_record["combined_identifier"].split("_")]

        if len(identifier_split) == 1:
            # dataset is direct child of repository
            record["series"] = "" # no sub-dataverses
        else:
            # dataset is direct child of sub-dataverse(s)
            record["series"] = []
            for dataverse_id in identifier_split[1:]:
                dataverse_name = self.get_dataverse_name_from_dataverse_id(int(dataverse_id))
                if dataverse_name:
                    record["series"].append(dataverse_name)
            record["series"] = " // ".join(record["series"]) # list of sub-dataverse names

        if "latestVersion" not in dataverse_record:
            # Dataset is deaccessioned
            record["deleted"] = 1
            record["title"], record["title_fr"] = "", ""
            return record

        if dataverse_record["latestVersion"]["license"] != "NONE":
            # Add license information to rights, if available
            record["rights"] = dataverse_record["latestVersion"]["license"]

        record["access"] = "Public" # Default to public access
        if dataverse_record["latestVersion"]["fileAccessRequest"]:
            # fileAccessRequest = True usually indicates at least one file is restricted
            # However, sometimes it is used in error--check to see if any files are actually restricted
            for file in dataverse_record["latestVersion"]["files"]:
                if 'restricted' in file and file['restricted']:
                    record["access"] = "Restricted"
                    break

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
                                geolocationPlace["province_state"] = geographicCoverage["state"]["value"]
                            if "city" in geographicCoverage:
                                geolocationPlace["city"] = geographicCoverage["city"]["value"]
                            if "otherGeographicCoverage" in geographicCoverage:
                                geolocationPlace["other"] = geographicCoverage["otherGeographicCoverage"]["value"]
                            geolocationPlaces.append(geolocationPlace)
                        record["geoplaces"] = geolocationPlaces
                    elif geospatial_field["typeName"] == "geographicBoundingBox":
                        # Get bounding box values
                        geolocationBoxes = []
                        for geographicBoundingBox in geospatial_field["value"]:
                            geolocationBox = {}
                            if "westLongitude" in geographicBoundingBox:
                                geolocationBox["westLon"] = \
                                    self.check_for_dms(geographicBoundingBox["westLongitude"]["value"])
                            if "eastLongitude" in geographicBoundingBox:
                                geolocationBox["eastLon"] = \
                                    self.check_for_dms(geographicBoundingBox["eastLongitude"]["value"])
                            if "northLongitude" in geographicBoundingBox:
                                geolocationBox["northLat"] = \
                                    self.check_for_dms(geographicBoundingBox["northLongitude"]["value"])
                            if "southLongitude" in geographicBoundingBox:
                                geolocationBox["southLat"] = \
                                    self.check_for_dms(geographicBoundingBox["southLongitude"]["value"])
                            geolocationBoxes.append(geolocationBox)
                        record["geobboxes"] = geolocationBoxes

        if "files" in dataverse_record["latestVersion"]:
            record["geofiles"] = []
            for dataverse_file in dataverse_record["latestVersion"]["files"]:
                if "dataFile" in dataverse_file:
                    if not dataverse_file["restricted"]:
                        try:
                            extension = "." + dataverse_file["dataFile"]["filename"].split(".")[1]
                            if extension.lower() in self.geofile_extensions:
                                geofile = {}
                                geofile["filename"] = dataverse_file["dataFile"]["filename"]
                                geofile["uri"] = self.url.replace("dataverses/%id%/contents", "access/datafile/") + str(
                                    dataverse_file["dataFile"]["id"])
                                record["geofiles"].append(geofile)
                        except IndexError: # no extension
                            pass

            if len(record["geofiles"]) == 0:
                record.pop("geofiles")

        return record

    def _update_record(self, record):
        try:
            identifier_split = record['local_identifier'].split("_")
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



