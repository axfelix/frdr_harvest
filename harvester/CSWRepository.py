from harvester.HarvestRepository import HarvestRepository
from harvester.rate_limited import rate_limited
from owslib.csw import CatalogueServiceWeb
import time
import json
import re
import os.path
import warnings

# Disable FutureWarning from owslib - we do not use the properties listed in
# https://github.com/geopython/OWSLib/issues/38 and
# https://github.com/geopython/OWSLib/issues/301
warnings.simplefilter("ignore", category=FutureWarning)

class CSWRepository(HarvestRepository):
    """ CSW Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "csw"
        super(CSWRepository, self).setRepoParams(repoParams)
        try:
            self.cswrepo = CatalogueServiceWeb(self.url)
        except:
            self.cswrepo = None
        self.domain_metadata = []

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "csw",
            "enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors,
            "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems,
            "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url,
            "repo_oai_name": self.repo_oai_name
        }
        self.repository_id = self.db.update_repo(**kwargs)

        if self.cswrepo is None:
            self.logger.error("Could not initiate this repo to crawl it")
            return

        item_count = 0
        while True:
            try:
                self.cswrepo.getrecords2(startposition=self.cswrepo.results['nextrecord'])
            except:
                self.cswrepo.getrecords2()

            for rec in self.cswrepo.records:
                result = self.db.write_header(self.cswrepo.records[rec].identifier, self.repository_id)
                item_count = item_count + 1
                if (item_count % self.update_log_after_numitems == 0):
                    tdelta = time.time() - self.tstart + 0.1
                    self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,
                                                                                               self.formatter.humanize(
                                                                                                   tdelta),
                                                                                               item_count / tdelta))
            if item_count >= self.cswrepo.results['matches']:
                break

        self.logger.info("Found {} items in feed".format(item_count))

    def format_csw_to_oai(self, csw_record, local_identifier):
        record = {}

        if csw_record.language == "eng":
            record["title"] = csw_record.title
            record["title"] = record["title"].strip()
            record["title_fr"] = ""
            record["tags"] = csw_record.subjects
        elif csw_record.language == "fre":
            record["title_fr"] = csw_record.title
            record["title_fr"] = record["title_fr"].strip()
            record["title"] = ""
            record["tags_fr"] = csw_record.subjects

        record["description"] = csw_record.abstract
        record["identifier"] = local_identifier
        record["creator"] = self.name
        record["series"] = ""

        if csw_record.bbox:
            # Workaround to address issue in oswlib related to EPSG:4326 CRS code that flips coordinates
            if float(csw_record.bbox.minx) > float(csw_record.bbox.maxx):
                # longitude values (minx and maxx) are switched by oswlib; switch them back
                record["geobboxes"] = [{"southLat": csw_record.bbox.miny, "westLon": csw_record.bbox.maxx,
                                        "northLat": csw_record.bbox.maxy, "eastLon": csw_record.bbox.minx}]
            elif float(csw_record.bbox.miny) > float(csw_record.bbox.maxy):
                # sometimes x and y values are switched, so the lats are longs and vice versa
                # we can look for the same discrepancy that happens in the longs, except it's in the y values now
                record["geobboxes"] = [{"southLat": csw_record.bbox.minx, "westLon": csw_record.bbox.maxy,
                                       "northLat": csw_record.bbox.maxx, "eastLon": csw_record.bbox.miny}]
            else:
                # default if nothing is wrong (this code isn't executed currently)
                record["geobboxes"] = [{"southLat": csw_record.bbox.miny, "westLon": csw_record.bbox.minx,
                                        "northLat": csw_record.bbox.maxy, "eastLon": csw_record.bbox.maxx}]

        return record

    @rate_limited(5)
    def _update_record(self, record):
        if self.cswrepo is None:
            return

        try:
            self.cswrepo.getrecordbyid(id=[record['local_identifier']])
        except:
            self.logger.error("Unable to update record: {}".format(record['local_identifier']))
            self.db.delete_record(record)
            return False

        if self.cswrepo.records:
            csw_record = self.cswrepo.records[record['local_identifier']]
            oai_record = self.format_csw_to_oai(csw_record, record['local_identifier'])
            # We have to request a second schema to get valid dates, no idea if issue is Hakai-specific
            self.cswrepo.getrecordbyid(id=[record['local_identifier']], outputschema="http://www.isotc211.org/2005/gmd")
            oai_record["pub_date"] = self.cswrepo.records[record['local_identifier']].datestamp
            oai_record["pub_date"] = re.sub("[T ][0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.?[0-9]*[Z]?$", "",
                                            oai_record["pub_date"])
            if oai_record:
                try:
                    self.db.write_record(oai_record, self)
                except:
                    if self.dump_on_failure == True:
                        try:
                            print(csw_record)
                        except:
                            pass
            return True

        else:
            # This record was deleted
            self.db.delete_record(record)
            return True

        return False
