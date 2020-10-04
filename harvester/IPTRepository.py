from harvester.HarvestRepository import HarvestRepository
from harvester.rate_limited import rate_limited
import requests
from dateutil import parser
import time
import json
import xml.etree.ElementTree as ET


class IPTRepository(HarvestRepository):
    """ IPTRepository Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "ipt"
        super(IPTRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "ipt",
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
            response = requests.get(self.url)
            records = response.json()

            item_count = 0
            for record in records:
                item_identifier = record["uid"]
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
            self.logger.error("Updating IPT Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def format_ipt_to_oai(self, item_eml_text, identifier):

        def getIndividualName(element):
            try:
                individualName = element.find("individualName")
                if individualName is not None:
                    givenName = ""
                    surName = ""
                    if individualName.find("givenName") is not None:
                        givenName = individualName.find("givenName").text.strip()
                    if individualName.find("surName") is not None:
                        surName = individualName.find("surName").text.strip()

                    combinedName = surName + ", " + givenName
                    if combinedName[-2:] == ", ":
                        return combinedName[:-2]
                    elif combinedName[:2] == ", ":
                        return combinedName[2:]
                    else:
                        return combinedName
                else:
                    return ""
            except Exception as e:
                return ""

        eml_root = ET.fromstring(item_eml_text)
        eml_dataset = eml_root.find("dataset")
        eml_additionalMetadata = eml_root.find("additionalMetadata")
        record = {}

        record["identifier"] = identifier
        record["title"] = eml_dataset.find("title").text.strip()
        record["pub_date"] = eml_dataset.find("pubDate").text.strip() # TODO check format
        record["description"] = eml_dataset.find("abstract").find("para").text.strip()
        record["series"] = ""

        record["creator"] = [getIndividualName(eml_dataset.find("creator"))]

        # Add contributors that don't duplicate the creator (or other contributors)
        record["contributor"] = []
        if eml_dataset.find("metadataProvider") is not None:
            metadataProvider = getIndividualName(eml_dataset.find("metadataProvider"))
            if metadataProvider not in record["creator"] and metadataProvider not in record["contributor"]:
                record["contributor"].append(metadataProvider)
        if eml_dataset.find("associatedParty") is not None:
            if eml_dataset.find("associatedParty").find("individualName") is not None:
                # Assign individualName to contributor
                associatedParty = getIndividualName(eml_dataset.find("associatedParty"))
                if associatedParty not in record["creator"] and associatedParty not in record["contributor"]:
                    record["contributor"].append(associatedParty)
            elif eml_dataset.find("associatedParty").find("organizationName") is not None:
                # If individualName is missing and role is publisher, use organizationName for publisher
                if eml_dataset.find("associatedParty").find("role").text == "publisher":
                    record["publisher"] = eml_dataset.find("associatedParty").find("organizationName").text
        if eml_dataset.find("contact") is not None:
            contact = getIndividualName(eml_dataset.find("contact"))
            if contact not in record["creator"] and contact not in record["contributor"]:
                record["contributor"].append(contact)
        record["contributor"] = [x for x in  record["contributor"] if x != ''] # Remove empty strings

        record["tags"] = []
        record["subject"] = []
        if len(eml_dataset.findall("keywordSet")) > 0:
            for keywordSet in eml_dataset.findall("keywordSet"):
                keywords = []
                keywordElements = keywordSet.findall("keyword")
                for keywordElement in keywordElements:
                    keywords.append(keywordElement.text)
                keywordThesaurus = keywordSet.find("keywordThesaurus")
                if keywordThesaurus.text.lower() == "n/a":
                    # Add keywords with "n/a" thesaurus to tags
                    record["tags"].extend(keywords)
                else:
                    # Add controlled keywords to subject
                    record["subject"].extend(keywords)

        record["item_url"] = ""
        for alternateIdentifier in eml_dataset.findall("alternateIdentifier"):
            if "doi:" in alternateIdentifier.text:
                record["item_url"] = "https://doi.org/" + alternateIdentifier.text.split("doi:")[1]
                break
            elif "doi.org/" in alternateIdentifier.text:
                record["item_url"] = "https://doi.org/" + alternateIdentifier.text.split("doi.org/")[1]
                break
            elif "http" in alternateIdentifier.text:
                record["item_url"] = alternateIdentifier.text
                break
        if "doi" not in record["item_url"]:
            try:
                doi = eml_additionalMetadata.find("metadata").find("gbif").find("citation").attrib["identifier"].split("doi:")[1]
                record["item_url"] = "https://doi.org/" + doi
            except:
                pass

        # TODO get rights
        # TODO get geospatial
        metadata_lang = eml_dataset.find("title").attrib['{http://www.w3.org/XML/1998/namespace}lang']
        if metadata_lang == "fre":
            record["title_fr"] = record["title"]
            record["title"] = ""
            record["tags_fr"] = record["tags"]
            record["tags"] = ""
            record["subject_fr"] = record["subject"]
            record["subject"] = ""
        else: # all have metadata_lang == "eng"
            record["title_fr"] = ""

        return record

    @rate_limited(5)
    def _update_record(self, record):
        try:
            identifier = record['local_identifier']
            item_json_url = "https://data.canadensys.net/collections/ws/dataResource/" + identifier
            try:
                # Get JSON for record
                response = requests.get(item_json_url)
                item_json = response.json()

                # Get XML EML for record
                connectionParameters_url = item_json["connectionParameters"]["url"]
                if "archive.do" in connectionParameters_url:
                    item_eml_url = connectionParameters_url.replace("archive.do", "eml.do")
                    print(item_eml_url)
                    response = requests.get(item_eml_url)
                    item_eml_text = response.text
                else:
                    raise Exception

            except Exception as e:
                # Exception means this URL was not found
                self.db.delete_record(record)
                return True

            oai_record = self.format_ipt_to_oai(item_eml_text, identifier)
            if oai_record:
                self.db.write_record(oai_record, self)
            return True

        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record['local_identifier'], e))
            if self.dump_on_failure == True:
                try:
                    print() # TODO print something useful
                except:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False



