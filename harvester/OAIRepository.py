from harvester.HarvestRepository import HarvestRepository
from functools import wraps
from sickle import Sickle
from sickle.iterator import BaseOAIIterator, OAIItemIterator, OAIResponseIterator
from sickle.models import OAIItem, Record, Header
from sickle.oaiexceptions import BadArgument, CannotDisseminateFormat, IdDoesNotExist, NoSetHierarchy, \
    BadResumptionToken, NoRecordsMatch, OAIError
from collections import defaultdict
import re
import os.path
import time
import json


# import dateparser
# from time import strftime

class FRDRRecord(OAIItem):
    """ Override Sickle OAIItem to handle stripping only known namespaces """

    def __init__(self, record_element, strip_ns=False):
        super(FRDRRecord, self).__init__(record_element, strip_ns=strip_ns)
        self.header = Header(self.xml.find('.//' + self._oai_namespace + 'header'))
        self.deleted = self.header.deleted
        if not self.deleted:
            self.metadata = self.xml_to_dict(self.xml.find('.//' + self._oai_namespace + 'metadata').getchildren()[0])

    def xml_to_dict(self, tree, paths=None):
        """ Modified from Sickle.utils to strip only some namespaces """
        namespaces_to_strip = [
            'http://purl.org/dc/elements/1.1/',
            'https://schema.datacite.org/meta/kernel-3/',
            'http://www.openarchives.org/OAI/2.0/'
        ]
        paths = paths or ['.//']
        fields = defaultdict(list)
        for path in paths:
            elements = tree.findall(path, {})
            for element in elements:
                tag_namespace = re.search('\{(.*)\}', element.tag).group(1)
                if tag_namespace in namespaces_to_strip:
                    tag = re.sub(r'\{.*\}', '', element.tag)
                else:
                    tag = tag_namespace + "#" + re.sub(r'\{.*\}', '', element.tag)
                fields[tag].append(element.text)
        return dict(fields)


class FRDRItemIterator(BaseOAIIterator):
    """ Modifed from Sickle.interator.OAIItemIterator to implement custom item mapping """

    def __init__(self, sickle, params, ignore_deleted=False):
        VERBS_ELEMENTS = {
            'GetRecord': 'record',
            'ListRecords': 'record',
            'ListIdentifiers': 'header',
            'ListSets': 'set',
            'ListMetadataFormats': 'metadataFormat',
            'Identify': 'Identify',
        }
        self.mapper = FRDRRecord
        self.element = VERBS_ELEMENTS[params.get('verb')]
        super(FRDRItemIterator, self).__init__(sickle, params, ignore_deleted)

    def _next_response(self):
        super(FRDRItemIterator, self)._next_response()
        self._items = self.oai_response.xml.iterfind('.//' + self.sickle.oai_namespace + self.element)

    def next(self):
        """Return the next record/header/set."""
        while True:
            for item in self._items:
                mapped = self.mapper(item)
                if self.ignore_deleted and mapped.deleted:
                    continue
                return mapped
            if self.resumption_token and self.resumption_token.token:
                self._next_response()
            else:
                raise StopIteration


class OAIRepository(HarvestRepository):
    """ OAI Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "oai_dc"
        super(OAIRepository, self).setRepoParams(repoParams)
        self.sickle = Sickle(self.url, iterator=FRDRItemIterator)

    def _crawl(self):
        records = []

        try:
            if self.set is None or self.set == "":
                records = self.sickle.ListRecords(metadataPrefix=self.metadataprefix, ignore_deleted=True)
            else:
                records = self.sickle.ListRecords(metadataPrefix=self.metadataprefix, ignore_deleted=True, set=self.set)
        except:
            self.logger.info("No items were found")

        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "oai",
            "enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors,
            "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems,
            "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url
        }
        self.repository_id = self.db.update_repo(**kwargs)
        item_count = 0

        while records:
            try:
                record = records.next()
                metadata = record.metadata

                # Search for a hyperlink in the list of identifiers
                if 'identifier' in metadata.keys():
                    if not isinstance(metadata['identifier'], list):
                        metadata['identifier'] = [metadata['identifier']]
                    for idt in metadata['identifier']:
                        # TODO - what about multiple identifiers? We should have some priority here, so we always pick the same one regardless of ordering
                        if idt.lower().startswith("http"):
                            metadata['dc:source'] = idt
                        if idt.lower().startswith("doi:"):
                            metadata['dc:source'] = "https://dx.doi.org/" + idt[4:]
                        if idt.lower().startswith("hdl:"):
                            metadata['dc:source'] = "https://hdl.handle.net/" + idt[4:]

                # EPrints workaround for using header datestamp in lieu of date
                if 'date' not in metadata.keys() and record.header.datestamp:
                    metadata["date"] = record.header.datestamp

                # Use the header id for the database key (needed later for OAI GetRecord calls)
                metadata['identifier'] = record.header.identifier
                oai_record = self.unpack_oai_metadata(metadata)
                domain_metadata = self.find_domain_metadata(metadata)

                self.db.write_record(oai_record, self.repository_id, self.metadataprefix.lower(), domain_metadata)
                item_count = item_count + 1
                if (item_count % self.update_log_after_numitems == 0):
                    tdelta = time.time() - self.tstart + 0.1
                    self.logger.info(
                        "Done {} items after {} ({:.1f} items/sec)".format(item_count, self.formatter.humanize(tdelta),
                                                                           (item_count / tdelta)))

            except AttributeError:
                # probably not a valid OAI record
                # Islandora throws this for non-object directories
                self.logger.debug("AttributeError while working on item {}".format(item_count))
                pass

            except StopIteration:
                break

        self.logger.info("Processed {} items in feed".format(item_count))

    def unpack_oai_metadata(self, record):
        record["pub_date"] = record.get("date")

        if self.metadataprefix.lower() == "ddi":
            # TODO: better DDI implementation that doesn't simply flatten everything, see: https://sickle.readthedocs.io/en/latest/customizing.html
            # Mapping as per http://www.ddialliance.org/resources/ddi-profiles/dc
            record["title"] = record.get("titl")
            record["creator"] = record.get("AuthEnty")
            record["subject"] = record.get("keyword", [])
            if "topcClas" in record.keys() and len(record["topcClas"]) > 0:
                record['subject'].extend(filter(None, record["topcClas"]))
            record["description"] = record.get("abstract")
            record["publisher"] = record.get("producer")
            record["contributor"] = record.get("othId")
            record["pub_date"] = record.get("prodDate")
            record["type"] = record.get("dataKind")
            record["identifier"] = record.get("IDNo")
            record["rights"] = record.get("copyright")

            if "northBL" in record.keys():
                # This record has geoSpatial bounding lines
                # Convert into an array of closed bounding box points (clockwise polygon)
                record["geospatial"] = {"type": "Polygon", "coordinates": [
                    [[record["northBL"][0], record["westBL"][0]], [record["northBL"][0], record["eastBL"][0]],
                     [record["southBL"][0], record["westBL"][0]], [record["southBL"][0], record["eastBL"][0]]]]}

        if self.metadataprefix.lower() == "fgdc" or self.metadataprefix.lower() == "fgdc-std":
            record["creator"] = record.get("origin")
            record["subject"] = record.get("themekey")
            record["description"] = record.get("abstract")
            record["publisher"] = record.get("cntorg")
            # Put these dates in preferred order
            record["pub_date"] = [record.get("pubdate"), record.get("begdate"), record.get("enddate")]
            record["type"] = record.get("geoform")
            record["dc:source"] = record.get("onlink")
            record["rights"] = record.get("distliab")
            record["access"] = record.get("accconst")

            if "bounding" in record.keys():
                # Sometimes point data is hacked in as a bounding box
                if record["westbc"] == record["eastbc"] and record["northbc"] == record["southbc"]:
                    record["geospatial"] = {"type": "Point",
                                            "coordinates": [[[record["northbc"][0], record["westbc"][0]]]]}
                else:
                    record["geospatial"] = {"type": "Polygon", "coordinates": [
                        [[record["northbc"][0], record["westbc"][0]], [record["northbc"][0], record["eastbc"][0]],
                         [record["southbc"][0], record["westbc"][0]], [record["southbc"][0], record["eastbc"][0]]]]}

        # Parse FRDR records
        if self.metadataprefix.lower() == "frdr":
            record["coverage"] = record.get("geolocationPlace")

            if "geolocationPoint" in record.keys():
                point_split = re.compile(",? ").split(record["geolocationPoint"][0])
                record["geospatial"] = {"type": "Point", "coordinates": [[point_split]]}

            if "geolocationBox" in record.keys():
                boxcoordinates = record["geolocationBox"][0].split()
                record["geospatial"] = {"type": "Polygon", "coordinates": [
                    [boxcoordinates[x:x + 2] for x in range(0, len(boxcoordinates), 2)]]}
            # Look for datacite.creatorAffiliation
            if "creatorAffiliation" in record:
                record["affiliation"] = record.get("creatorAffiliation")

        if 'identifier' not in record.keys():
            return None

        if record["pub_date"] is None:
            return None

        # If there are multiple identifiers, and one of them contains a link, then prefer it
        # Otherwise just take the first one
        if isinstance(record["identifier"], list):
            valid_id = record["identifier"][0]
            for idstring in record["identifier"]:
                if "http" in idstring.lower():
                    valid_id = idstring
            record["identifier"] = valid_id

        if 'creator' not in record.keys() and 'contributor' not in record.keys() and 'publisher' not in record.keys():
            self.logger.debug("Item {} is missing creator - will not be added".format(record["identifier"]))
            return None
        elif 'creator' not in record.keys() and 'contributor' in record.keys():
            record["creator"] = record["contributor"]
        elif 'creator' not in record.keys() and 'publisher' in record.keys():
            record["creator"] = record["publisher"]
        # Workaround for WOUDC, which doesn't attribute individual datasets
        elif self.metadataprefix.lower() == "fgdc-std":
            record["creator"] = self.name

        # If date is undefined add an empty key
        if 'pub_date' not in record.keys():
            record["pub_date"] = ""

        # If there are multiple dates choose the longest one (likely the most specific)
        # If there are a few dates with the same length the first one will be used, which assumes we grabbed them in a preferred order
        # Exception test added for some strange PDC dates of [null, null]
        if isinstance(record["pub_date"], list):
            valid_date = record["pub_date"][0] or ""
            for datestring in record["pub_date"]:
                if datestring is not None:
                    if len(datestring) > len(valid_date):
                        valid_date = datestring
            record["pub_date"] = valid_date

        # If date is still a one-value list, make it a string
        if isinstance(record["pub_date"], list):
            record["pub_date"] = record["pub_date"][0]

        # Convert long dates into YYYY-MM-DD
        datestring = re.search("(\d{4}[-/]?\d{2}[-/]?\d{2})", record["pub_date"])
        if datestring:
            record["pub_date"] = datestring.group(0).replace("/", "-")

        # If dates are entirely numeric, add separators
        if not re.search("\D", record["pub_date"]):
            if (len(record["pub_date"]) == 6):
                record["pub_date"] = record["pub_date"][0] + record["pub_date"][1] + record["pub_date"][2] + \
                                     record["pub_date"][3] + "-" + record["pub_date"][4] + record["pub_date"][5]
            if (len(record["pub_date"]) == 8):
                record["pub_date"] = record["pub_date"][0] + record["pub_date"][1] + record["pub_date"][2] + \
                                     record["pub_date"][3] + "-" + record["pub_date"][4] + record["pub_date"][5] + "-" + \
                                     record["pub_date"][6] + record["pub_date"][7]

        # If a date has question marks, chuck it
        if "?" in record["pub_date"]:
            return None

        # Make sure dates are valid
        if not re.search("^(1|2)\d{3}(-?(0[1-9]|1[0-2])(-?(0[1-9]|1[0-9]|2[0-9]|3[0-1]))?)?$", record["pub_date"]):
            self.logger.debug("Invalid date for record {}".format(record["dc:source"]))
            return None

            # record["pub_date"] = dateparser.parse(record["pub_date"]).strftime("%Y-%m-%d")

        if "title" not in record.keys():
            return None
        if isinstance(record["title"], list):
            record["title"] = record["title"][0]

        if "contact" not in record.keys():
            record["contact"] = ""
            if "publisher" in record.keys():
                if isinstance(record["publisher"], list):
                    record["publisher"] = record["publisher"][0]
                if record["publisher"] is not None:
                    contact_address = re.search(r"[\w\.-]+@([\w-]+\.)+[\w-]{2,4}", record["publisher"])
                    try:
                        record["contact"] = contact_address.group(0)
                    except:
                        pass
        if isinstance(record["contact"], list):
            record["contact"] = record["contact"][0]

        if "series" not in record.keys():
            record["series"] = ""

        # DSpace workaround to exclude theses and non-data content
        if self.prune_non_dataset_items:
            if record["type"] and "Dataset" not in record["type"]:
                return None

        # EPrints workaround to fix duplicates and Nones in Rights
        if "rights" in record.keys() and isinstance(record["rights"], list):
            record["rights"] = list(set(filter(None.__ne__, record["rights"])))

        # EPrints workaround for liberal use of dc:identifier
        # Rather not hardcode a single source URL for this
        if self.url == "http://spectrum.library.concordia.ca/cgi/oai2":
            for relation in record["relation"]:
                if "http://spectrum.library.concordia.ca" in relation:
                    record["dc:source"] = relation

        return record

    def find_domain_metadata(self, record):
        newRecord = {}
        for elementName in list(record.keys()):
            if '#' in elementName:
                newRecord[elementName] = record.pop(elementName, None)
        return newRecord

    def _rate_limited(max_per_second):
        """ Decorator that make functions not be called faster than a set rate """
        threading = __import__('threading')
        lock = threading.Lock()
        min_interval = 1.0 / float(max_per_second)

        def decorate(func):
            last_time_called = [0.0]

            @wraps(func)
            def rate_limited_function(*args, **kwargs):
                lock.acquire()
                elapsed = time.clock() - last_time_called[0]
                left_to_wait = min_interval - elapsed

                if left_to_wait > 0:
                    time.sleep(left_to_wait)

                lock.release()

                ret = func(*args, **kwargs)
                last_time_called[0] = time.clock()
                return ret

            return rate_limited_function

        return decorate

    @_rate_limited(5)
    def _update_record(self, record):
        self.logger.debug("Updating OAI record {}".format(record['local_identifier']))

        try:
            single_record = self.sickle.GetRecord(identifier=record["local_identifier"],
                                                  metadataPrefix=self.metadataprefix)

            try:
                metadata = single_record.metadata
                if 'identifier' in metadata.keys() and isinstance(metadata['identifier'], list):
                    if "http" in metadata['identifier'][0].lower():
                        metadata['dc:source'] = metadata['identifier'][0]
            except AttributeError:
                metadata = {}

            # EPrints workaround for using header datestamp in lieu of date
            if 'date' not in metadata.keys() and single_record.header.datestamp:
                metadata["date"] = single_record.header.datestamp

            metadata['identifier'] = single_record.header.identifier
            oai_record = self.unpack_oai_metadata(metadata)
            domain_metadata = self.find_domain_metadata(metadata)
            if oai_record is None:
                self.db.delete_record(record)
                return False
            self.db.write_record(oai_record, self.repository_id, self.metadataprefix.lower(), domain_metadata)
            return True

        except IdDoesNotExist:
            # Item no longer in this repo
            self.db.delete_record(record)
            return True

        except Exception as e:
            self.logger.error("Updating item failed (repo_id:{}, oai_id:{}): {}".format(self.repository_id,
                                                                                        record['local_identifier'], e))
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False
