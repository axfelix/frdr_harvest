from harvester.HarvestRepository import HarvestRepository
from harvester.rate_limited import rate_limited
import ckanapi
import time
import json
import re
import os.path
import ftfy
import requests

class CKANRepository(HarvestRepository):
    """ CKAN Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "ckan"
        self.default_language = "en"
        super(CKANRepository, self).setRepoParams(repoParams)
        self.ckanrepo = ckanapi.RemoteCKAN(self.url)
        self.domain_metadata = []
        if "ckan_api_endpoint" not in repoParams:
            self.ckan_api_endpoint = ""
        if "ckan_ignore_private" not in repoParams:
            self.ckan_ignore_private = False
        if "ckan_include_identifier_pattern" not in repoParams:
            self.ckan_include_identifier_pattern = ""
        if "ckan_strip_from_identifier" not in repoParams:
            self.ckan_strip_from_identifier = ""
        if "ckan_access_field" not in repoParams:
            self.ckan_access_field = ""
        if "ckan_ignore_date" not in repoParams:
            self.ckan_ignore_date = False

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "ckan",
            "enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors,
            "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems,
            "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url,
            "repo_oai_name": self.repo_oai_name
        }
        self.repository_id = self.db.update_repo(**kwargs)

        if self.ckan_api_endpoint: # Yukon
            r = requests.get(self.url + self.ckan_api_endpoint + "/package_list")
            records = json.loads(r.text)["result"]
        else:
            records = self.ckanrepo.call_action('package_list', requests_kwargs={'verify': False})

        # If response is limited to 1000, get all records with pagination
        if len(records) == 1000:
            offset = 0
            records = self.ckanrepo.call_action('package_list?limit=1000&offset=' + str(offset), requests_kwargs={'verify': False})

            # Iterate through sets of 1000 records until no records returned
            while len(records) % 1000 == 0:
                offset +=1000
                response = self.ckanrepo.call_action('package_list?limit=1000&offset=' + str(offset), requests_kwargs={'verify': False})
                if len(response) == 0:
                    break
                records = records + response

        item_count = 0
        for ckan_identifier in records:
            if not self.ckan_include_identifier_pattern or self.ckan_include_identifier_pattern in ckan_identifier: # Yukon
                if self.ckan_strip_from_identifier:
                    ckan_identifier = ckan_identifier.replace(self.ckan_strip_from_identifier,"")
                result = self.db.write_header(ckan_identifier, self.repository_id)
                item_count = item_count + 1
                if (item_count % self.update_log_after_numitems == 0):
                    tdelta = time.time() - self.tstart + 0.1
                    self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,
                            self.formatter.humanize(tdelta), item_count / tdelta))

        self.logger.info("Found {} items in feed".format(item_count))

    def format_ckan_to_oai(self, ckan_record, local_identifier):
        record = {}

        if ('type' in ckan_record) and ckan_record['type']:
            # Exclude showcases and other non-dataset records (publications from Alberta, info from Open Data Canada)
            if ckan_record['type'] in ['showcase', 'publications', 'info', 'harvest']:
                return False

        if not 'date_published' in ckan_record and not 'dates' in ckan_record and not 'record_publish_date' in ckan_record and not 'metadata_created' in ckan_record and not 'date_issued' in ckan_record:
            return None

        if ('contacts' in ckan_record) and ckan_record['contacts']:
            record["creator"] = [person.get('name', "") for person in ckan_record['contacts']]
        elif ('author' in ckan_record) and ckan_record['author']:
            try:
                authors = json.loads(ckan_record["author"])
                record["creator"] = []
                for author in authors:
                    record["creator"].append(author["author_name"])
            except:
                record["creator"] = ckan_record['author']
        elif ('maintainer' in ckan_record) and ckan_record['maintainer']:
            record["creator"] = ckan_record['maintainer']
        elif ('creator' in ckan_record) and ckan_record['creator']:
            record["creator"] = ckan_record["creator"]
        elif ('organization' in ckan_record) and ckan_record['organization'] and ckan_record['organization'].get('title', "") != "":
            record["creator"] = ckan_record['organization'].get('title', "")
        elif self.name == 'Canadian Integrated Ocean Observing System (CIOOS)':
            record["creator"] = []
            if ('cited-responsible-party' in ckan_record) and ckan_record['cited-responsible-party']:
                for creator in json.loads(ckan_record['cited-responsible-party']):
                    if ("organisation-name" in creator) and creator["organisation-name"]:
                        if creator["organisation-name"] not in record["creator"]:
                            record["creator"].append(creator["organisation-name"])
                    if ("individual-name" in creator) and creator["individual-name"]:
                        if creator["individual-name"] not in record["creator"]:
                            record["creator"].append(creator["individual-name"])
        else:
            record["creator"] = self.name

        if not isinstance(record["creator"], list):
            record["creator"] = [record["creator"]]

        record["creator"] = [ftfy.fixes.decode_escapes(x).strip() for x in record["creator"] if x != '']

        if ('owner_division' in ckan_record) and ckan_record['owner_division']:
            # Toronto
            record["publisher"] = ckan_record['owner_division']

        # CIOOS only
        if ('metadata-point-of-contact' in ckan_record) and ckan_record['metadata-point-of-contact']:
            record["contributor"] = []
            point_of_contact = json.loads(ckan_record['metadata-point-of-contact'])
            if ("organisation-name" in point_of_contact) and point_of_contact["organisation-name"]:
                if point_of_contact["organisation-name"] not in record["creator"]:
                    record["contributor"].append(point_of_contact["organisation-name"])
            if ("individual-name" in point_of_contact) and point_of_contact["individual-name"]:
                if point_of_contact["individual-name"] not in record["creator"]:
                    record["contributor"].append(point_of_contact["individual-name"])
            if isinstance(record["contributor"], list):
                record["contributor"] = [x.strip() for x in record["contributor"] if x != '']

        # CIOOS only
        if ('organization' in ckan_record) and ckan_record['organization']:
            if ('title_translated' in ckan_record['organization']) and ckan_record['organization']['title_translated']:
                record["publisher"] = ckan_record['organization']['title_translated'].get("en", "") \
                                      + " / " + ckan_record['organization']['title_translated'].get("fr", "")
                if len(record["publisher"]) == 3:
                    record["publisher"] = ""
                elif record["publisher"][:3] == (" / "):
                    record["publisher"] = record["publisher"][3:]
                elif record["publisher"][-3:] == (" / "):
                    record["publisher"] = record["publisher"][:-3]

        record["identifier"] = local_identifier

        if self.item_url_pattern:
            record["item_url"] = self.item_url_pattern.replace("%id%", ckan_record["id"])
        else:
            record["item_url"] = ckan_record["url"]

        if isinstance(ckan_record.get("title_translated", ""), dict):
            record["title"] = ckan_record["title_translated"].get("en", "")
            record["title_fr"] = ckan_record["title_translated"].get("fr", "")
            if "fr-t-en" in ckan_record["title_translated"]:
                record["title_fr"] = ckan_record["title_translated"].get("fr-t-en", "")
            if "en-t-fr" in ckan_record["title_translated"]:
                record["title"] = ckan_record["title_translated"].get("en-t-fr", "")
        elif isinstance(ckan_record.get("title", ""), dict):
            record["title"] = ckan_record["title"].get("en", "")
            record["title_fr"] = ckan_record["title"].get("fr", "")
            if "fr-t-en" in ckan_record["title"]:
                record["title_fr"] = ckan_record["title"].get("fr-t-en", "")
            if "en-t-fr" in ckan_record["title_translated"]:
                record["title"] = ckan_record["title_translated"].get("en-t-fr", "")
        else:
            if self.default_language == "en":
                record["title"] = ckan_record.get("title", "")
            if self.default_language == "fr":
                record["title_fr"] = ckan_record.get("title", "")

        # Create empty title and title_fr if not present
        if "title" not in record:
            record["title"] = ""
        if "title_fr" not in record:
            record["title_fr"] = ""

        record["title"] = record["title"].strip()
        record["title_fr"] = record["title_fr"].strip()

        # Do not include records without at least one title
        if record["title"] == "" and record["title_fr"] == "":
            return None

        if isinstance(ckan_record.get("notes_translated", ""), dict):
            record["description"] = ckan_record["notes_translated"].get("en", "")
            record["description_fr"] = ckan_record["notes_translated"].get("fr", "")
            if "fr-t-en" in ckan_record["notes_translated"]:
                record["description_fr"] = ckan_record["notes_translated"].get("fr-t-en", "")
            if "en-t-fr" in ckan_record["notes_translated"]:
                record["description"] = ckan_record["notes_translated"].get("en-t-fr", "")
        elif isinstance(ckan_record.get("description", ""), dict):
            record["description"] = ckan_record["description"].get("en", "")
            record["description_fr"] = ckan_record["description"].get("fr", "")
            if "fr-t-en" in ckan_record["description"]:
                record["description_fr"] = ckan_record["description"].get("fr-t-en", "")
            if "en-t-fr" in ckan_record["notes_translated"]:
                record["description"] = ckan_record["notes_translated"].get("en-t-fr", "")
        else:
            record["description"] = ckan_record.get("notes", "")
            record["description_fr"] = ckan_record.get("notes_fra", "")
            if self.default_language == "fr":
                record["description_fr"] = ckan_record.get("notes", "")
                record["description"] = ""

        record["description"] = ftfy.fixes.decode_escapes(record["description"]).strip()
        record["description_fr"] = ftfy.fixes.decode_escapes(record["description_fr"]).strip()

        if ('sector' in ckan_record):
            # BC Data Catalogue
            if self.default_language == "en":
                record["subject"] = ckan_record.get('sector', "")
            elif self.default_language == "fr":
                record["subject_fr"] = ckan_record.get('sector', "")
        elif ('subject' in ckan_record):
            # Open Data Canada
            if self.default_language == "en":
                record["subject"] = ckan_record.get('subject', "")
            elif self.default_language == "fr":
                record["subject_fr"] = ckan_record.get('subject', "")
        elif "groups" in ckan_record and ckan_record["groups"]:
            # Surrey, CanWin Data Hub, Quebec, Montreal, Yukon (plus Regina, Guelph, Niagara)
            record["subject"] = []
            record["subject_fr"] = []
            for group in ckan_record["groups"]:
                if self.default_language == "en":
                    if "display_name" in group:
                        record["subject"].append(group.get("display_name", ""))
                    elif "title" in group: # Yukon
                        record["subject"].append(group.get("title", ""))
                elif self.default_language == "fr":
                    if "display_name" in group:
                        record["subject_fr"].append(group.get("display_name", ""))
                    elif "title" in group:
                        record["subject_fr"].append(group.get("title", ""))
        elif "topic" in ckan_record and ckan_record["topic"]:
            # Alberta
            if self.default_language == "en":
                record["subject"] = ckan_record["topic"]
            elif self.default_language == "fr":
                record["subject_fr"] = ckan_record["topic"]
        elif "topics" in ckan_record and ckan_record["topics"]:
            # Toronto
            record["subject"] = ckan_record["topics"].split(",")
            if "civic_issues" in ckan_record and ckan_record["civic_issues"]:
                record["subject"].extend(ckan_record["civic_issues"].split(","))
        elif "civic_issues" in ckan_record and ckan_record["civic_issues"]:
            # Toronto, records without "topics"
            record["subject"] = ckan_record["civic_issues"].split(",")


        if ('license_title' in ckan_record) and ckan_record['license_title']:
            record["rights"] = [ckan_record['license_title']]
            record["rights"].append(ckan_record.get("license_url", ""))
            record["rights"].append(ckan_record.get("attribution", ""))
            record["rights"] = "\n".join(record["rights"])
            record["rights"] = record["rights"].strip()

        if ("rights" in record) and record["rights"] == "/open-government-licence-yukon":
            record["rights"] = "Open Government Licence - Yukon\nhttps://open.yukon.ca/open-government-licence-yukon"

        # Look for publication date in a few places
        # All of these assume the date will start with year first
        record["pub_date"] = ""
        if ('record_publish_date' in ckan_record):
            # Prefer an explicit publish date if it exists
            record["pub_date"] = ckan_record["record_publish_date"]
        elif ('date_published' in ckan_record and ckan_record["date_published"]):
            # Another possible field name for publication date
            record["pub_date"] = ckan_record["date_published"]
        elif ('dates' in ckan_record and isinstance(ckan_record["dates"], list)):
            # A list of date objects, look for the one marked as Created
            for date_object in ckan_record['dates']:
                if date_object.type == "Created":
                    record["pub_date"] = date_object.date
        elif ('date_issued' in ckan_record):
            record["pub_date"] = ckan_record["date_issued"]
        elif ('metadata_created' in ckan_record):
            record["pub_date"] = ckan_record["metadata_created"]
            if self.ckan_ignore_date and self.ckan_ignore_date in record["pub_date"]: # Yukon
                if ('metadata_modified' in ckan_record):
                    record["pub_date"] = ckan_record["metadata_modified"]
                if self.ckan_ignore_date in record["pub_date"]:
                    record["pub_date"] = ckan_record["revision_timestamp"]
            try:
                month_day_year = record["pub_date"].split(", ")[1].split(" - ")[0].split("/")
                record["pub_date"] = month_day_year[2] + "-" + month_day_year[0] + "-" + month_day_year[1]
            except:
                pass

        # Some date formats have a trailing timestamp after date (ie: "2014-12-10T15:05:03.074998Z")
        record["pub_date"] = re.sub("[T ][0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.?[0-9]*[Z]?$", "", record["pub_date"])
        # Ensure date separator is a dash
        record["pub_date"] = record["pub_date"].replace("/", "-")

        # Look at the year and make sure it is feasible, otherwise blank out the date
        # Limiting records to being published from 1300-2399
        publication_year = int(record["pub_date"][:2])
        if (publication_year < 13 or publication_year > 23):
            record["pub_date"] = ""

        try:
            record["series"] = ckan_record["data_series_name"]["en"]
        except:
            record["series"] = ckan_record.get("data_series_name", "")

        if isinstance(record["series"], dict):
            if len(record["series"]) > 0:
                record["series"] = ",".join(str(v) for v in list(record["series"].values()))
            else:
                record["series"] = ""

        record["tags"] = []
        record["tags_fr"] = []
        if isinstance(ckan_record.get("keywords", ""), dict):
            if "en" in ckan_record["keywords"]:
                for tag in ckan_record["keywords"]["en"]:
                    record["tags"].append(tag)
            if "fr" in ckan_record["keywords"]:
                for tag in ckan_record["keywords"]["fr"]:
                    record["tags_fr"].append(tag)
            if "fr-t-en" in ckan_record["keywords"]:
                for tag in ckan_record["keywords"]["fr-t-en"]:
                    record["tags_fr"].append(tag)
            if "en-t-fr" in ckan_record["keywords"]:
                for tag in ckan_record["keywords"]["en-t-fr"]:
                    record["tags"].append(tag)
        elif isinstance(ckan_record.get("tags_translated", ""), dict):
            if "en" in ckan_record["tags_translated"]:
                for tag in ckan_record["tags_translated"]["en"]:
                    record["tags"].append(tag)
            if "fr" in ckan_record["tags_translated"]:
                for tag in ckan_record["tags_translated"]["fr"]:
                    record["tags_fr"].append(tag)
            if "fr-t-en" in ckan_record["tags_translated"]:
                for tag in ckan_record["tags_translated"]["fr-t-en"]:
                    record["tags_fr"].append(tag)
            if "en-t-fr" in ckan_record["keywords"]:
                for tag in ckan_record["keywords"]["en-t-fr"]:
                    record["tags"].append(tag)
        else:
            if ("tags" in ckan_record) and ckan_record["tags"]:
                for tag in ckan_record["tags"]:
                    if self.default_language == "fr":
                        if "display_name" in tag:
                            record["tags_fr"].append(tag["display_name"])
                        elif "name" in tag:
                            record["tags_fr"].append(tag["name"])
                    else:
                        if "display_name" in tag:
                            record["tags"].append(tag["display_name"])
                        elif "name" in tag: # Yukon
                            record["tags"].append(tag["name"])

        if ("west_bound_longitude" in ckan_record) and ("east_bound_longitude" in ckan_record) and \
                ('north_bound_latitude' in ckan_record) and ('south_bound_latitude' in ckan_record):
            # BC Data Catalogue
            record["geobboxes"] = [{"southLat": ckan_record['south_bound_latitude'], "westLon": ckan_record['west_bound_longitude'],
                                    "northLat": ckan_record['north_bound_latitude'], "eastLon": ckan_record['east_bound_longitude']}]
        elif ("bbox-west-long" in ckan_record) and ("bbox-east-long" in ckan_record) and \
                ('bbox-north-lat' in ckan_record) and ('bbox-south-lat' in ckan_record):
            # CIOOS
            record["geobboxes"] = [{"southLat": ckan_record['bbox-south-lat'], "westLon": ckan_record['bbox-west-long'],
                                    "northLat": ckan_record['bbox-north-lat'], "eastLon": ckan_record['bbox-east-long']}]
        elif ("spatialcoverage1" in ckan_record) and ckan_record["spatialcoverage1"]:
            # Alberta
            spatialcoverage1 = ckan_record["spatialcoverage1"].split(",")
            if len(spatialcoverage1) == 4:
                # Check to make sure we have the right number of pieces because sometimes spatialcoverage1 is a place name
                # Coordinates are in W N E S order
                record["geobboxes"] = [
                    {"southLat": spatialcoverage1[3], "westLon": spatialcoverage1[0],
                     "northLat": spatialcoverage1[1], "eastLon": spatialcoverage1[2]}]
            else:
                # If it isn't split into 4 pieces, use as a place name
                record["geoplaces"] = [{"place_name": ckan_record['spatialcoverage1']}]
        elif ("spatial" in ckan_record) and ckan_record['spatial']:
            # CanWin Data Hub, Open Data Canada, plus a few from BC Data Catalogue
            spatial = ckan_record["spatial"]
            if isinstance(ckan_record["spatial"], str):
                # Open Data Canada
                spatial = json.loads(ckan_record["spatial"])
            xValues = []
            yValues = []
            if spatial["type"] == "Polygon":
                record["geobboxes"] = []
                # Calculate the bounding box based on the coordinates
                for coordinates in spatial['coordinates']:
                    for coordinate_pair in coordinates:
                        if coordinate_pair[0] not in xValues:
                            xValues.append(coordinate_pair[0])
                        if coordinate_pair[1] not in yValues:
                            yValues.append(coordinate_pair[1])
                    if len(xValues) > 1 and len(yValues) > 1:
                        # In most cases there are 2 xValue and 2 yValues: this is a regular bbox
                        # If there are 3 or more xValues or yValues, this is the largest bounding box encompassing the polygon
                        record["geobboxes"].append({"southLat": min(yValues), "westLon": min(xValues),
                             "northLat": max(yValues), "eastLon": max(xValues)})
                if len(record["geobboxes"]) == 0:
                    record.pop("geobboxes")
        if ("ext_spatial" in ckan_record) and ckan_record['ext_spatial']:
            # Quebec, Montreal
            record["geoplaces"] = [{"place_name": ckan_record['ext_spatial']}]

        # Access Constraints, if available
        if ("private" in ckan_record):
            if ckan_record["private"] and not self.ckan_ignore_private:
                record["access"] = "Limited"
            else:
                record["access"] = "Public"
                if self.ckan_access_field:
                    record["access"] = ckan_record.get(self.ckan_access_field,"")
                if record["access"].lower() in ["open", "unrestricted", "public", ""]:
                    record["access"] = "Public"

        # Files
        if "resources" in ckan_record and isinstance(ckan_record["resources"], list):
            record["geofiles"] = []
            for ckan_file in ckan_record["resources"]:
                geofile = {}
                try:
                    url = ckan_file["url"].split("?")[0] # remove any query parameters
                    filename = url.split("/")[len(url.split("/"))-1] # get the last part after the slasy
                    extension = "." + filename.split(".")[len(filename.split("."))-1]
                    if extension.lower() in self.geofile_extensions:
                        geofile["uri"] = url
                        geofile["filename"] = filename
                        record["geofiles"].append(geofile)
                except IndexError:
                    pass
            if len(record["geofiles"]) == 0:
                record.pop("geofiles")

        return record

    @rate_limited(5)
    def _update_record(self, record):
        # self.logger.debug("Updating CKAN record {}".format(record['local_identifier']) )

        try:
            if self.ckan_api_endpoint: # Yukon
                r = requests.get(self.url + self.ckan_api_endpoint + "/package_show?id=" + record['local_identifier'])
                try:
                    ckan_record = json.loads(r.text)["result"][0]
                except IndexError:
                    raise ckanapi.errors.NotFound
            else:
                ckan_record = self.ckanrepo.call_action('package_show', {'id':record['local_identifier']}, requests_kwargs={'verify': False})
            oai_record = self.format_ckan_to_oai(ckan_record, record['local_identifier'])
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

        except ckanapi.errors.NotAuthorized:
            # Not authorized may mean the record is embargoed, but ODC also uses this to indicate the record was deleted
            self.db.delete_record(record)
            return True

        except ckanapi.errors.NotFound:
            # Not found means this record was deleted
            self.db.delete_record(record)
            return True

        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record['local_identifier'], e))
            if self.dump_on_failure == True:
                try:
                    print(ckan_record)
                except:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False
