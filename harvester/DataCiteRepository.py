from harvester.HarvestRepository import HarvestRepository
import requests
import time
import json
import re
import os.path
from dateutil import parser


class DataCiteRepository(HarvestRepository):
    """ DataCite Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "datacite"
        self.default_language = "en"
        super(DataCiteRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.headers = {'accept': 'application/vnd.api+json'}

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "datacite",
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
                if (totalPages * page_size) < 10000: # use page numbers for less than 10K DOIs
                    page_number += 1
                    for record in response["data"]:
                        item_identifier = record["id"]
                        result = self.db.write_header(item_identifier, self.repository_id)
                        item_count = item_count + 1
                        if (item_count % self.update_log_after_numitems == 0):
                            tdelta = time.time() - self.tstart + 0.1
                            self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,self.formatter.humanize(tdelta),item_count / tdelta))
                else: # use cursor
                    querystring = {"client-id": self.set, "page[cursor]": str(page_number), "page[size]": str(page_size)}
                    response = requests.request("GET", self.url, headers=self.headers, params=querystring)
                    response = response.json()
                    next = response["links"]["next"]
                    while next:
                        page_number = page_number + 1
                        print("Cursor #" + str(page_number) + ": " + next) # FIXME remove this
                        for record in response["data"]:
                            item_identifier = record["id"]
                            result = self.db.write_header(item_identifier, self.repository_id)
                            item_count = item_count + 1
                            if (item_count % self.update_log_after_numitems == 0):
                                tdelta = time.time() - self.tstart + 0.1
                                self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count, self.formatter.humanize(tdelta), item_count / tdelta))
                        response = requests.request("GET", next, headers=self.headers)
                        response = response.json()
                        try:
                            next = response["links"]["next"]
                        except:
                            next = None
                    break # exit while page_number < totalPages loop

            self.logger.info("Found {} items in feed".format(item_count))
            return True

        except Exception as e:
            self.logger.error("Updating DataCite Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True
        return False

    def format_datacite_to_oai(self, datacite_record):
        record = {}
        record["identifier"] = datacite_record["id"]
        if "resourceTypeGeneral" in datacite_record["attributes"]["types"]:
            if datacite_record["attributes"]["types"]["resourceTypeGeneral"] not in ["Dataset"]:
                # TODO set additional types to include per repository (e.g. Collection for Canadensys)
                #print(datacite_record["attributes"]["types"]["resourceTypeGeneral"])
                return False

        record["creator"] = []
        record["affiliation"] = []
        for creator in datacite_record["attributes"]["creators"]:
            if "name" in creator and creator["name"]:
                record["creator"].append(creator["name"])
            if len(creator["affiliation"]) > 0:
                for affiliation in creator["affiliation"]:
                    record["affiliation"].append(affiliation)
        if len(record["affiliation"]) == 0:
            record.pop("affiliation")

        record["title"] = ""
        record["title_fr"] = ""
        for title in datacite_record["attributes"]["titles"]:
            # If there is a lang attribute, use that to determine title / title_fr
            if "lang" in title and title["lang"]:
                if "fr" in title["lang"] and record["title_fr"] == "":
                    record["title_fr"] = title["title"]
                elif "en" in title["lang"] and record["title"] == "":
                    record["title"] = title["title"]
        # If no titles had a lang attribute, use the first title and set based on default lang
        if record["title"] == "" and record["title_fr"] == "":
            if self.default_language == "fr":
                record["title_fr"] = datacite_record["attributes"]["titles"][0]["title"]
            else:
                record["title"] = datacite_record["attributes"]["titles"][0]["title"]

        record["publisher"] = datacite_record["attributes"]["publisher"]
        record["series"] = ""

        record["pub_date"] = datacite_record["attributes"]["publicationYear"]
        dateType_found = False
        # Prefer date issued, then available, then created; otherwise, keep publicationYear
        for date in datacite_record["attributes"]["dates"]:
            if date["dateType"] == "Issued" and len(date["date"]) > 4:
                record["pub_date"] = date["date"]
                dateType_found = True
                break
        if dateType_found == False:
            for date in datacite_record["attributes"]["dates"]:
                if date["dateType"] == "Available" and len(date["date"]) > 4:
                    record["pub_date"] = date["date"]
                    dateType_found = True
                    break
        if dateType_found == False:
            for date in datacite_record["attributes"]["dates"]:
                if date["dateType"] == "Created" and len(date["date"]) > 4:
                    record["pub_date"] = date["date"]
                    dateType_found = True
                    break

        if len(datacite_record["attributes"]["subjects"]) > 0:
            record["tags"] = []
            record["tags_fr"] = []
            for tag in datacite_record["attributes"]["subjects"]:
                if "lang" in tag and tag["lang"]:
                    if "fr" in tag["lang"]:
                        record["tags_fr"].append(tag["subject"])
                    else:
                        record["tags"].append(tag["subject"])
                elif self.default_language == "fr":
                    record["tags_fr"].append(tag["subject"])
                else:
                    record["tags"].append(tag["subject"])
            if len(record["tags"]) == 0:
                record.pop("tags")
            if len(record["tags_fr"]) == 0:
                record.pop("tags_fr")

        if len(datacite_record["attributes"]["contributors"]) > 0:
            record["contributor"] = []
            for contributor in datacite_record["attributes"]["contributors"]:
                if contributor["name"] not in record["creator"]: # don't add duplicates from creators
                    record["contributor"].append(contributor["name"])
            if len(record["contributor"]) == 0:
                record.pop("contributor")

        if len(datacite_record["attributes"]["rightsList"]) > 0:
            record["rights"] = []
            for rights in datacite_record["attributes"]["rightsList"]:
                rightsEntry = ""
                if "rights" in rights:
                    rightsEntry = rights["rights"]
                if "rightsUri" in rights:
                    rightsEntry = rightsEntry + "\n" + rights["rightsUri"]
                if rightsEntry != "":
                    record["rights"].append(rightsEntry)

        if len(datacite_record["attributes"]["descriptions"]) > 0:
            record["description"] = []
            record["description_fr"] = []
            for description in datacite_record["attributes"]["descriptions"]:
                if "description" in description and description["description"]:
                    if "lang" in description and description["lang"]:
                        if "fr" in description["lang"]:
                            record["description_fr"].append(description["description"])
                        else:
                            record["description"].append(description["description"])
                    elif self.default_language == "fr":
                        record["description_fr"].append(description["description"])
                    else:
                        record["description"].append(description["description"])
            if len(record["description"]) == 0:
                record.pop("description")
            if len(record["description_fr"]) == 0:
                record.pop("description_fr")

        if len(datacite_record["attributes"]["geoLocations"]) > 0:
            record["geoplaces"] = []
            record["geopoints"] = []
            record["geobboxes"] = []
            for geolocation in datacite_record["attributes"]["geoLocations"]:
                if "geoLocationPlace" in geolocation:
                    record["geoplaces"].append({"place_name": geolocation["geoLocationPlace"]})
                if "geoLocationPoint" in geolocation:
                    record["geopoints"].append({"lat": geolocation["geoLocationPoint"]["pointLatitude"],
                                                "lon": geolocation["geoLocationPoint"]["pointLongitude"]})
                if "geoLocationBox" in geolocation:
                    record["geobboxes"].append({"westLon": geolocation["geoLocationBox"]["westBoundLongitude"],
                                                "eastLon": geolocation["geoLocationBox"]["eastBoundLongitude"],
                                                "northLat": geolocation["geoLocationBox"]["northBoundLatitude"],
                                                "southLat": geolocation["geoLocationBox"]["southBoundLatitude"]})
            if len(record["geoplaces"]) == 0:
                record.pop("geoplaces")
            if len(record["geopoints"]) == 0:
                record.pop("geopoints")
            if len(record["geobboxes"]) == 0:
                record.pop("geobboxes")

        return record

    def _update_record(self, record):
        try:
            record_url = self.url + "/" + record["local_identifier"]
            try:
                item_response = requests.get(record_url)
                datacite_record = json.loads(item_response.text)["data"]
            except Exception as e:
                # Exception means this URL was not found
                self.logger.error("Fetching record {} failed: {}".format(record_url, e))
                return True
            oai_record = self.format_datacite_to_oai(datacite_record)
            if oai_record:
                self.db.write_record(oai_record, self)
            else:
                if oai_record is False:
                    # This record is not a dataset, remove it from the results
                    self.db.delete_record(record)
                else:
                    # Some other problem, this record will be updated by a future crawl
                    self.db.touch_record(record)
            return True
        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record['local_identifier'], e))
            if self.dump_on_failure == True:
                try:
                    print(datacite_record)
                except:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

