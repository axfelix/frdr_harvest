import time
import re
from harvester.TimeFormatter import TimeFormatter

import urllib3

urllib3.disable_warnings()  # We are not loading any unsafe sites, just repos we trust


class HarvestRepository(object):
    """ Top level representation of a repository """

    def __init__(self, globalParams):
        defaultParams = {
            'url': None,
            'type': None,
            'name': None,
            'set': '',
            'thumbnail': None,
            'abort_after_numerrors': 5,
            'max_records_updated_per_run': 100,
            'update_log_after_numitems': 100,
            'record_refresh_days': 30,
            'repo_refresh_days': 7,
            'item_url_pattern': None,
            'prune_non_dataset_items': False,
            'enabled': False,
            'formatter': TimeFormatter(),
            'error_count': 0,
            'db': None,
            'logger': None,
            'dataverses_list': None
        }
        for key, value in defaultParams.items():
            setattr(self, key, value)
        # Inherit global config
        for key, value in globalParams.items():
            setattr(self, key, value)
        self.repository_id = 0

    def setRepoParams(self, repoParams):
        """ Set local repo params and let them override the global config """
        for key, value in repoParams.items():
            setattr(self, key, value)

        repo_oai_name = repoParams["homepage_url"].replace("https://", "").replace("www.", "").replace("http://", "")
        if repo_oai_name[-1] == "/":
            repo_oai_name = repo_oai_name[:-1]
        repo_oai_name = re.sub('[^0-9a-zA-Z\-\.]+', '-', repo_oai_name)
        setattr(self, "repo_oai_name", repo_oai_name)
        setattr(self, "geofile_extensions", [".tif", ".tiff",".xyz", ".png", ".aux.xml",".tab",".twf",".tifw", ".tiffw",".wld",
                                  ".tif.prj",".tfw", ".geojson",".shp",".gpkg", ".shx", ".dbf", ".sbn",".prj", ".csv", ".txt", ".zip"])

    def setLogger(self, l):
        self.logger = l

    def setDatabase(self, d):
        self.db = d

    def setFormatter(self, f):
        self.formatter = f

    def crawl(self):
        self.tstart = time.time()
        if self.repository_id == 0:
            self.repository_id = self.db.get_repo_id(self.url, self.set)
        self.last_crawl = self.db.get_repo_last_crawl(self.repository_id)

        if self.last_crawl == 0:
            self.logger.info("*** Repo: {}, type: {}, (last harvested: never)".format(self.name, self.type))
        else:
            self.logger.info("*** Repo: {}, type: {}, (last harvested: {} ago)".format(self.name, self.type,
                                                                                       self.formatter.humanize(
                                                                                           self.tstart - self.last_crawl)))

        if (self.enabled):
            if (self.last_crawl + self.repo_refresh_days * 86400) < self.tstart:
                try:
                    self._crawl()
                    self.db.update_last_crawl(self.repository_id)
                except Exception as e:
                    self.logger.error("Repository {} unable to be harvested: {}".format(self.name, e))
            else:
                self.logger.info("This repo is not yet due to be harvested")
        else:
            self.logger.info("This repo is not enabled for harvesting")

    def _update_record(self, record):
        """ This method to be overridden """
        return True

    def update_stale_records(self, dbparams):
        """ This method will be called by a child class only, so that it uses its own _update_record() method """
        if self.enabled != True:
            return True
        if self.db == None:
            self.logger.error("Database configuration is not complete")
            return False
        record_count = 0
        tstart = time.time()
        self.logger.info("Looking for stale records to update")
        stale_timestamp = int(time.time() - self.record_refresh_days * 86400)

        records = self.db.get_stale_records(stale_timestamp, self.repository_id, self.max_records_updated_per_run)
        for record in records:
            if record_count == 0:
                self.logger.info("Started processing for {} records".format(len(records)))

            status = self._update_record(record)
            if not status:
                self.logger.error(
                    "Aborting due to errors after {} items updated in {} ({:.1f} items/sec)".format(
                        record_count, 
                        self.formatter.humanize(time.time() - tstart), 
                        record_count / (time.time() - tstart + 0.1))
                    )
                break

            record_count = record_count + 1
            if (record_count % self.update_log_after_numitems == 0):
                tdelta = time.time() - tstart + 0.1
                self.logger.info(
                    "Done {} items after {} ({:.1f} items/sec)".format(record_count, self.formatter.humanize(tdelta),
                                                                       (record_count / tdelta)))

        self.logger.info("Updated {} items in {} ({:.1f} items/sec)".format(record_count, self.formatter.humanize(
            time.time() - tstart), record_count / (time.time() - tstart + 0.1)))

    def check_for_dms(self, coordinate):
        lowercase = coordinate.lower()
        if "west" in lowercase or "w" in lowercase or "south" in lowercase or ("s" in lowercase and "east" not in lowercase):
            lowercase = self.remove_direction(lowercase)
            return self.convert_dms_2_dd(lowercase, False)
        elif "east" in lowercase or ("e" in lowercase and "west" not in lowercase) or "north" in lowercase or "n" in lowercase:
            lowercase = self.remove_direction(lowercase)
            return self.convert_dms_2_dd(lowercase, True)
        else:
            return coordinate

    def remove_direction(self, coordinate):
        directions = ['n', 'north', 's', 'south', 'e', 'east', 'w', 'west']
        for d in directions:
            coordinate = coordinate.replace(d, '')
        return coordinate

    def convert_dms_2_dd(self, lowercase, positive):
        try:
            partsOrig = re.split('[°\'"]+', lowercase)
            parts = []
            for part in partsOrig:
                if not part == ' ':
                    parts.append(part)
            if len(parts) == 3:
                coord = self.dms2dd(positive, parts[0], parts[1], parts[2])
            elif len(parts) == 2:
                coord = self.dms2dd(positive, parts[0], parts[1])
            elif len(parts) == 1:
                coord = self.dms2dd(positive, parts[0])
            else:
                return ""
        except:
            return ""
        return str(coord) if coord != 3600 else ""

    def dms2dd(self, positive, degrees, minutes="0", secs="0"):
        try:
            dd = float(degrees) + float(minutes) / 60 + float(secs) / (60 * 60);
            if not positive:
                dd *= -1
            return dd
        except ValueError:
            self.logger.info("Something went wrong parsing a coordinate: with degree {}, minute {}, and second{}".format(degrees, minutes, secs))
            return 3600
