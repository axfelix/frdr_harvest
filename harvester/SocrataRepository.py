from harvester.HarvestRepository import HarvestRepository
from functools import wraps
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
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url
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

        record["title"] = socrata_record["name"]
        record["description"] = socrata_record.get("description", "")
        record["tags"] = socrata_record.get("tags", "")
        record["identifier"] = local_identifier
        record["creator"] = socrata_record.get("attribution", self.name)
        record["pub_date"] = datetime.fromtimestamp(socrata_record["publicationDate"]).strftime('%Y-%m-%d')
        record["contact"] = self.contact
        record["series"] = socrata_record.get("category", "")

        return record

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
    def _update_record(self,record):

        try:            
            socrata_record = self.socratarepo.get_metadata(record['local_identifier'])
            oai_record = self.format_socrata_to_oai(socrata_record,record['local_identifier'])
            if oai_record:
                self.db.write_record(oai_record, self.repository_id, self.metadataprefix.lower(), self.domain_metadata)
            return True

        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record['local_identifier'], e))
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

