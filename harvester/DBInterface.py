import os
import time
import sys
import hashlib
import json
import re


class DBInterface:
    def __init__(self, params):
        self.dbtype = params.get('type', None)
        self.dbname = params.get('dbname', None)
        self.host = params.get('host', None)
        self.schema = params.get('schema', None)
        self.user = params.get('user', None)
        self.password = params.get('pass', None)
        self.connection = None
        self.logger = None

        if self.dbtype == "sqlite":
            self.dblayer = __import__('sqlite3')
            if os.name == "posix":
                try:
                    os.chmod(self.dbname, 0o664)
                except:
                    pass

        elif self.dbtype == "postgres":
            self.dblayer = __import__('psycopg2')

        else:
            raise ValueError('Database type must be sqlite or postgres in config file')

        con = self.getConnection()
        with con:
            cur = self.getCursor(con)

            # This table must always exist
            cur.execute(
                "create table if not exists settings (setting_id INTEGER PRIMARY KEY NOT NULL, setting_name TEXT, setting_value TEXT)")

            # Determine if the database schema needs to be updated
            dbversion = self.get_setting("dbversion")
            files = os.listdir("sql/" + str(self.dbtype) + "/")
            files.sort()
            for filename in files:
                if filename.endswith(".sql"):
                    scriptversion = int(filename.split('.')[0])
                    if scriptversion > dbversion:
                        # Run this script to update the schema, then record it as done
                        with open("sql/" + str(self.dbtype) + "/" + filename, 'r') as scriptfile:
                            scriptcontents = scriptfile.read()
                        if self.dbtype == "postgres":
                            cur.execute(scriptcontents)
                        else:
                            cur.executescript(scriptcontents)
                        self.set_setting("dbversion", scriptversion)
                        dbversion = scriptversion
                        print("Updated database to version: {:d}".format(scriptversion))  # No logger yet

        self.tabledict = {}
        with open("sql/tables.json", 'r') as jsonfile:
            self.tabledict = json.load(jsonfile)

    def setLogger(self, l):
        self.logger = l

    def getConnection(self):
        if self.connection == None:
            if self.dbtype == "sqlite":
                self.connection = self.dblayer.connect(self.dbname)
            elif self.dbtype == "postgres":
                self.connection = self.dblayer.connect("dbname='%s' user='%s' password='%s' host='%s'" % (
                    self.dbname, self.user, self.password, self.host))
                self.connection.autocommit = True

        return self.connection

    def getCursor(self, con):
        if self.dbtype == "sqlite":
            con.row_factory = self.getRow()
        cur = con.cursor()
        if self.dbtype == "postgres":
            from psycopg2.extras import RealDictCursor
            cur = con.cursor(cursor_factory=RealDictCursor)

        return cur

    def getRow(self):
        return self.dblayer.Row

    def getType(self):
        return self.dbtype

    def _prep(self, statement):
        if (self.dbtype == "postgres"):
            return statement.replace('?', '%s')
        return statement

    def get_setting(self, setting_name):
        # Get an internal setting - always returning an int value
        setting_value = 0
        con = self.getConnection()
        res = None
        with con:
            cur = self.getCursor(con)
            cur.execute(
                self._prep("select setting_value from settings where setting_name = ? order by setting_value desc"),
                (setting_name,))
            if cur is not None:
                res = cur.fetchone()
            if res is not None:
                setting_value = int(res['setting_value'])
        return setting_value

    def set_setting(self, setting_name, new_value):
        curent_value = self.get_setting(setting_name)
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            if curent_value == 0:
                cur.execute(self._prep("insert into settings(setting_value, setting_name) values (?,?)"),
                            (new_value, setting_name))
            else:
                cur.execute(self._prep("update settings set setting_value = ? where setting_name = ?"),
                            (new_value, setting_name))

    def update_repo(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            if self.repo_id > 0:
                # Existing repo
                try:
                    self.logger.debug("This repo already exists in the database; updating")
                    cur.execute(self._prep("""UPDATE repositories
                        set repository_url=?, repository_set=?, repository_name=?, repository_type=?, repository_thumbnail=?, last_crawl_timestamp=?, item_url_pattern=?,enabled=?,
                        abort_after_numerrors=?,max_records_updated_per_run=?,update_log_after_numitems=?,record_refresh_days=?,repo_refresh_days=?,homepage_url=?,repo_oai_name=?
                        WHERE repository_id=?"""), (
                        self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail, time.time(),
                        self.item_url_pattern,
                        self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                        self.update_log_after_numitems,
                        self.record_refresh_days, self.repo_refresh_days, self.homepage_url, self.repo_oai_name, self.repo_id))
                except self.dblayer.IntegrityError as e:
                    # record already present in repo
                    self.logger.error("Integrity error in update {}".format(e))
                    return self.repo_id
            else:
                # Create new repo record
                try:
                    self.logger.debug("This repo does not exist in the database; adding")
                    if self.dbtype == "postgres":
                        cur.execute(self._prep("""INSERT INTO repositories
                            (repository_url, repository_set, repository_name, repository_type, repository_thumbnail, last_crawl_timestamp, item_url_pattern, enabled,
                            abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days,homepage_url,repo_oai_name)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING repository_id"""), (
                            self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail,
                            time.time(), self.item_url_pattern,
                            self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                            self.update_log_after_numitems,
                            self.record_refresh_days, self.repo_refresh_days, self.homepage_url, self.repo_oai_name))
                        self.repo_id = int(cur.fetchone()['repository_id'])

                    if self.dbtype == "sqlite":
                        cur.execute(self._prep("""INSERT INTO repositories
                            (repository_url, repository_set, repository_name, repository_type, repository_thumbnail, last_crawl_timestamp, item_url_pattern, enabled,
                            abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days,homepage_url,repo_oai_name)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""), (
                            self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail,
                            time.time(), self.item_url_pattern,
                            self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                            self.update_log_after_numitems,
                            self.record_refresh_days, self.repo_refresh_days, self.homepage_url, self.repo_oai_name))
                        self.repo_id = int(cur.lastrowid)

                except self.dblayer.IntegrityError as e:
                    self.logger.error("Cannot add repository: {}".format(e))

        return self.repo_id

    def get_repo_id(self, repo_url, repo_set):
        returnvalue = 0
        extrawhere = ""
        if repo_set is not None:
            extrawhere = "and repository_set='{}'".format(repo_set)
        records = self.get_multiple_records("repositories", "repository_id", "repository_url", repo_url, extrawhere)
        for record in records:
            returnvalue = int(record['repository_id'])
        # If not found, look for insecure version of the url, it may have just changed to https on this pass
        if returnvalue == 0 and repo_url and repo_url.startswith('https:'):
            repo_url = repo_url.replace("https:", "http:")
            records = self.get_multiple_records("repositories", "repository_id", "repository_url", repo_url, extrawhere)
            for record in records:
                returnvalue = int(record['repository_id'])
        return returnvalue

    def get_repo_last_crawl(self, repo_id):
        returnvalue = 0
        if repo_id == 0 or repo_id is None:
            return 0
        records = self.get_multiple_records("repositories", "last_crawl_timestamp", "repository_id", repo_id)
        for record in records:
            returnvalue = int(record['last_crawl_timestamp'])
        self.logger.debug("Last crawl ts for repo_id {} is {}".format(repo_id, returnvalue))
        return returnvalue

    def get_repositories(self):
        records = self.get_multiple_records("repositories", "*", "enabled", "1", "or enabled = 'true'")
        repos = [dict(rec) for rec in records]
        for i in range(len(repos)):
            records = self.get_multiple_records("records", "count(*) as cnt", "repository_id",
                                                repos[i]["repository_id"], "and modified_timestamp!=0 and (title != '' or title_fr != '') and deleted=0")
            for rec in records:
                repos[i]["item_count"] = int(rec["cnt"])
        return repos

    def update_last_crawl(self, repo_id):
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            cur.execute(self._prep("update repositories set last_crawl_timestamp = ? where repository_id = ?"),
                        (int(time.time()), repo_id))

    def delete_record(self, record):
        con = self.getConnection()
        if record['record_id'] == 0:
            return False
        with con:
            cur = self.getCursor(con)

            try:
                cur.execute(self._prep("UPDATE records set deleted = 1, modified_timestamp = ? where record_id=?"),
                            (time.time(), record['record_id']))
            except:
                self.logger.error("Unable to mark as deleted record {}".format(record['local_identifier']))
                return False

            try:
                self.delete_all_related_records("records_x_access", record['record_id'])
                self.delete_all_related_records("records_x_creators", record['record_id'])
                self.delete_all_related_records("records_x_publishers", record['record_id'])
                self.delete_all_related_records("records_x_rights", record['record_id'])
                self.delete_all_related_records("records_x_subjects", record['record_id'])
                self.delete_all_related_records("records_x_affiliations", record['record_id'])
                self.delete_all_related_records("records_x_tags", record['record_id'])
                self.delete_all_related_records("descriptions", record['record_id'])
                self.delete_all_related_records("geospatial", record['record_id'])
                self.delete_all_related_records("domain_metadata", record['record_id'])
            except:
                self.logger.error(
                    "Unable to delete related table rows for record {}".format(record['local_identifier']))
                return False

        self.logger.debug("Marked as deleted: record {}".format(record['local_identifier']))
        return True

    def purge_deleted_records(self):
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            try:
                sqlstring = "DELETE from records where deleted=1"
                cur.execute(sqlstring)
            except:
                return False
        return True

    def delete_all_related_records(self, crosstable, record_id):
        return self.delete_row_generic(crosstable, "record_id", record_id)

    def delete_one_related_record(self, crosstable, column_value, record_id):
        columnname = self.get_table_value_column(crosstable)
        self.delete_row_generic(crosstable, columnname, column_value, "and record_id="+str(record_id) )

    def delete_row_generic(self, tablename, columnname, column_value, extrawhere=""):
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            try:
                sqlstring = "DELETE from {} where {}=? {}".format(tablename, columnname, extrawhere)
                cur.execute(self._prep(sqlstring), (column_value,))
            except:
                return False
        return True

    def get_table_id_column(self, tablename):
        if tablename in self.tabledict and "idcol" in self.tabledict[tablename]:
            return str(self.tabledict[tablename]["idcol"])
        raise ValueError("tables.json missing idcol definition for {}".format(tablename))

    def get_table_value_column(self, tablename):
        if tablename in self.tabledict and "valcol" in self.tabledict[tablename]:
            return str(self.tabledict[tablename]["valcol"])
        raise ValueError("tables.json missing valcol definition for {}".format(tablename))

    def insert_related_record(self, tablename, val, **kwargs):
        valcolumn = self.get_table_value_column(tablename)
        idcolumn = self.get_table_id_column(tablename)
        related_record_id = None
        paramlist = {valcolumn: val}
        for key, value in kwargs.items():
            paramlist[key] = value
        sqlstring = "INSERT INTO {} ({}) VALUES ({})".format(
            tablename, ",".join(str(k) for k in list(paramlist.keys())),
            ",".join(str("?") for k in list(paramlist.keys())))

        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            try:
                if self.dbtype == "postgres":
                    cur.execute(self._prep(sqlstring + " RETURNING " + idcolumn), list(paramlist.values()))
                    related_record_id = int(cur.fetchone()[idcolumn])
                if self.dbtype == "sqlite":
                    cur.execute(self._prep(sqlstring), list(paramlist.values()))
                    related_record_id = int(cur.lastrowid)
            except self.dblayer.IntegrityError as e:
                self.logger.error("Record insertion problem: {}".format(e))

        return related_record_id

    def insert_cross_record(self, crosstable, relatedtable, related_id, record_id, **kwargs):
        cross_table_id = None
        idcolumn = self.get_table_id_column(crosstable)
        relatedidcolumn = self.get_table_id_column(relatedtable)
        paramlist = {"record_id": record_id, relatedidcolumn: related_id}
        for key, value in kwargs.items():
            paramlist[key] = value
        sqlstring = "INSERT INTO {} ({}) VALUES ({})".format(
            crosstable, ",".join(str(k) for k in list(paramlist.keys())),
            ",".join(str("?") for k in list(paramlist.keys())))

        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            try:
                if self.dbtype == "postgres":
                    cur.execute(self._prep(sqlstring + " RETURNING " + idcolumn), list(paramlist.values()))
                    cross_table_id = int(cur.fetchone()[idcolumn])
                if self.dbtype == "sqlite":
                    cur.execute(self._prep(sqlstring), list(paramlist.values()))
                    cross_table_id = int(cur.lastrowid)
            except self.dblayer.IntegrityError as e:
                self.logger.error("Record insertion problem: {}".format(e))

    def get_multiple_records(self, tablename, columnlist, given_col, given_val, extrawhere=""):
        records = []
        sqlstring = "select {} from {} where {}=? {}".format(columnlist, tablename, given_col, extrawhere)
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            cur.execute(self._prep(sqlstring), (given_val,))
            if cur is not None:
                records = cur.fetchall()

        return records

    def get_single_record_id(self, tablename, val, extrawhere=""):
        returnvalue = None
        idcolumn = self.get_table_id_column(tablename)
        valcolumn = self.get_table_value_column(tablename)
        records = self.get_multiple_records(tablename, idcolumn, valcolumn, val, extrawhere)
        for record in records:
            returnvalue = int(record[idcolumn])

        return returnvalue

    def construct_local_url(self, record):
        oai_id = None
        oai_search = None
        # Check if the local_identifier has already been turned into a url
        if "local_identifier" in record:
            if record["local_identifier"] and record["local_identifier"].startswith(("http","HTTP")):
                return record["local_identifier"]
            # No link found, see if there is an OAI identifier
            oai_search = re.search("oai:(.+):(.+)", record["local_identifier"])
        else:
            oai_search = re.search("oai:(.+):(.+)", record["identifier"])

        if oai_search:
            # Check for OAI format of identifier (oai:domain:id) and extract just the ID
            oai_id = oai_search.group(2)
            # Replace underscores in IDs with colons (SFU Radar)
            oai_id = oai_id.replace("_", ":")

        # If given a pattern then substitue in the item ID and return it
        if "item_url_pattern" in record and record["item_url_pattern"]:
            if oai_id:
                local_url = re.sub("(\%id\%)", oai_id, record["item_url_pattern"])
            else:
                # No OAI ID found, but we still got passed a pattern, so use it with full identifier
                if "local_identifier" in record and record["local_identifier"]:
                    local_url = re.sub("(\%id\%)", record["local_identifier"], record["item_url_pattern"])
                else:
                    local_url = re.sub("(\%id\%)", record["identifier"], record["item_url_pattern"])
            return local_url

        # Check if the identifier is a DOI
        if "local_identifier" in record and record["local_identifier"]:
            doi = re.search("(doi|DOI):\s?\S+", record["local_identifier"])
            if doi:
                doi = doi.group(0).rstrip('\.')
                local_url = re.sub("(doi|DOI):\s?", "https://doi.org/", doi)
                return local_url

        # Check if the source is already a link
        if "source_url" in record:
            if record["source_url"] and record["source_url"].startswith(("http","HTTP")):
                return record["source_url"]
        if "dc:source" in record:
            if isinstance(record["dc:source"], list):
                if record["dc:source"][0] and record["dc:source"][0].startswith(("http","HTTP")):
                        return record["dc:source"][0]
            else:
                if record["dc:source"] and record["dc:source"].startswith(("http","HTTP")):
                    return record["dc:source"]

        # URL is in the identifier
        local_url = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?",
                              record["local_identifier"])
        if local_url:
            return local_url.group(0)

        self.logger.error("construct_local_url() failed for item: {}".format(json.dumps(record)) )
        return None

    def create_new_record(self, rec, source_url, repo_id):
        returnvalue = None
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            try:
                if self.dbtype == "postgres":
                    cur.execute(self._prep(
                        """INSERT INTO records (title, title_fr, pub_date, series, modified_timestamp, source_url, deleted, local_identifier, item_url, repository_id)
                        VALUES(?,?,?,?,?,?,?,?,?,?) RETURNING record_id"""),
                        (rec["title"], rec["title_fr"], rec["pub_date"],  rec["series"], time.time(), source_url, 0,
                         rec["identifier"], rec["item_url"], repo_id))
                    returnvalue = int(cur.fetchone()['record_id'])
                if self.dbtype == "sqlite":
                    cur.execute(self._prep(
                        """INSERT INTO records (title, title_fr, pub_date, series, modified_timestamp, source_url, deleted, local_identifier, item_url, repository_id)
                        VALUES(?,?,?,?,?,?,?,?,?,?)"""),
                        (rec["title"], rec["title_fr"], rec["pub_date"], rec["series"], time.time(), source_url, 0,
                         rec["identifier"], rec["item_url"], repo_id))
                    returnvalue = int(cur.lastrowid)
            except self.dblayer.IntegrityError as e:
                self.logger.error("Record insertion problem: {}".format(e))

        return returnvalue

    def write_record(self, record, repo):
        repo_id = repo.repository_id
        metadata_prefix = repo.metadataprefix.lower()
        domain_metadata = repo.domain_metadata
        modified_upstream = False # Track whether metadata changed since last crawl

        if record == None:
            return None
        record["record_id"] = self.get_single_record_id("records", record["identifier"],
                                                        "and repository_id=" + str(repo_id))
        record["item_url_pattern"] = repo.item_url_pattern
        if record.get("item_url", None) is None:
            record["item_url"] = self.construct_local_url(record)

        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            source_url = ""
            if 'dc:source' in record:
                if isinstance(record["dc:source"], list):
                    source_url = record["dc:source"][0]
                else:
                    source_url = record["dc:source"]

            if record["record_id"] is None:
                modified_upstream = True # New record has new metadata
                record["record_id"] = self.create_new_record(record, source_url, repo_id)
            else:
                # Compare title, title_fr, pub_date, series, source_url, item_url, local_identifier for changes
                records = self.get_multiple_records("records", "*", "record_id", record["record_id"])
                if len(records) == 1:
                    existing_record = records[0]
                    if existing_record["title"] != record["title"]:
                        modified_upstream = True
                    elif existing_record["title_fr"] != record["title_fr"]:
                        modified_upstream = True
                    elif existing_record["pub_date"] != record["pub_date"]:
                        modified_upstream = True
                    elif existing_record["series"] != record["series"]:
                        modified_upstream = True
                    elif existing_record["source_url"]!=None and  existing_record["source_url"] != source_url:
                        modified_upstream = True
                    elif existing_record["item_url"] != record["item_url"]:
                        modified_upstream = True
                    elif existing_record["local_identifier"] != record["identifier"]:
                        modified_upstream = True

                cur.execute(self._prep(
                    """UPDATE records set title=?, title_fr=?, pub_date=?, series=?, modified_timestamp=?, source_url=?, deleted=?, local_identifier=?, item_url=?
                    WHERE record_id = ?"""),
                    (record["title"], record["title_fr"], record["pub_date"], record["series"], time.time(),
                     source_url, 0, record["identifier"], record["item_url"], record["record_id"]))

            if record["record_id"] is None:
                return None

            if "creator" in record:
                if not isinstance(record["creator"], list):
                    record["creator"] = [record["creator"]]
                extrawhere = "and is_contributor=0"
                existing_creator_recs = self.get_multiple_records("records_x_creators", "creator_id", "record_id",
                                                                  record["record_id"], extrawhere)
                existing_creator_ids = [e["creator_id"] for e in existing_creator_recs]
                new_creator_ids = []
                for creator in record["creator"]:
                    creator_id = self.get_single_record_id("creators", creator)
                    if creator_id is None:
                        creator_id = self.insert_related_record("creators", creator)
                        modified_upstream = True
                    if creator_id is not None:
                        new_creator_ids.append(creator_id)
                        if creator_id not in existing_creator_ids:
                            extras = {"is_contributor": 0}
                            self.insert_cross_record("records_x_creators", "creators", creator_id, record["record_id"],
                                                     **extras)
                            modified_upstream = True
                for eid in existing_creator_ids:
                    if eid not in new_creator_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_creators", eid, record["record_id"], extrawhere)

            if "contributor" in record:
                if not isinstance(record["contributor"], list):
                    record["contributor"] = [record["contributor"]]
                extrawhere = "and is_contributor=1"
                existing_creator_recs = self.get_multiple_records("records_x_creators", "creator_id", "record_id",
                                                                  record["record_id"], extrawhere)
                existing_creator_ids = [e["creator_id"] for e in existing_creator_recs]
                new_creator_ids = []
                for creator in record["contributor"]:
                    creator_id = self.get_single_record_id("creators", creator)
                    if creator_id is None:
                        creator_id = self.insert_related_record("creators", creator)
                        modified_upstream = True
                    if creator_id is not None:
                        new_creator_ids.append(creator_id)
                        if creator_id not in existing_creator_ids:
                            extras = {"is_contributor": 1}
                            self.insert_cross_record("records_x_creators", "creators", creator_id, record["record_id"],
                                                     **extras)
                            modified_upstream = True
                for eid in existing_creator_ids:
                    if eid not in new_creator_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_creators", eid, record["record_id"], extrawhere)

            if "subject" in record:
                if not isinstance(record["subject"], list):
                    record["subject"] = [record["subject"]]
                existing_subject_recs = self.get_multiple_records("records_x_subjects", "subject_id", "record_id",
                                                                  record["record_id"])
                existing_subject_ids = [e["subject_id"] for e in existing_subject_recs]
                new_subject_ids = []
                for subject in record["subject"]:
                    subject_id = self.get_single_record_id("subjects", subject, "and language='en'")
                    if subject_id is None:
                        extras = {"language": "en"}
                        subject_id = self.insert_related_record("subjects", subject, **extras)
                        modified_upstream = True
                    if subject_id is not None:
                        new_subject_ids.append(subject_id)
                        if subject_id not in existing_subject_ids:
                            self.insert_cross_record("records_x_subjects", "subjects", subject_id, record["record_id"])
                            modified_upstream = True
                for eid in existing_subject_ids:
                    if eid not in new_subject_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_subjects", eid, record["record_id"])

            if "subject_fr" in record:
                if not isinstance(record["subject_fr"], list):
                    record["subject_fr"] = [record["subject_fr"]]
                existing_subject_recs = self.get_multiple_records("records_x_subjects", "subject_id", "record_id",
                                                                  record["record_id"])
                existing_subject_ids = [e["subject_id"] for e in existing_subject_recs]
                new_subject_ids = []
                for subject in record["subject_fr"]:
                    subject_id = self.get_single_record_id("subjects", subject, "and language='fr'")
                    if subject_id is None:
                        extras = {"language": "fr"}
                        subject_id = self.insert_related_record("subjects", subject, **extras)
                        modified_upstream = True
                    if subject_id is not None:
                        new_subject_ids.append(subject_id)
                        if subject_id not in existing_subject_ids:
                            self.insert_cross_record("records_x_subjects", "subjects", subject_id, record["record_id"])
                            modified_upstream = True
                for eid in existing_subject_ids:
                    if eid not in new_subject_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_subjects", eid, record["record_id"])

            if "publisher" in record:
                if not isinstance(record["publisher"], list):
                    record["publisher"] = [record["publisher"]]
                existing_publisher_recs = self.get_multiple_records("records_x_publishers", "publisher_id", "record_id",
                                                                    record["record_id"])
                existing_publisher_ids = [e["publisher_id"] for e in existing_publisher_recs]
                new_publisher_ids = []
                for publisher in record["publisher"]:
                    publisher_id = self.get_single_record_id("publishers", publisher)
                    if publisher_id is None:
                        publisher_id = self.insert_related_record("publishers", publisher)
                        modified_upstream = True
                    if publisher_id is not None:
                        new_publisher_ids.append(publisher_id)
                        if publisher_id not in existing_publisher_ids:
                            self.insert_cross_record("records_x_publishers", "publishers", publisher_id,
                                                     record["record_id"])
                            modified_upstream = True
                for eid in existing_publisher_ids:
                    if eid not in new_publisher_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_publishers", eid, record["record_id"])

            if "affiliation" in record:
                if not isinstance(record["affiliation"], list):
                    record["affiliation"] = [record["affiliation"]]
                existing_affiliation_recs = self.get_multiple_records("records_x_affiliations", "affiliation_id", "record_id",
                                                                    record["record_id"])
                existing_affiliation_ids = [e["affiliation_id"] for e in existing_affiliation_recs]
                new_affiliation_ids = []
                for affil in record["affiliation"]:
                    affiliation_id = self.get_single_record_id("affiliations", affil)
                    if affiliation_id is None:
                        affiliation_id = self.insert_related_record("affiliations", affil)
                        modified_upstream = True
                    if affiliation_id is not None:
                        if affiliation_id not in existing_affiliation_ids and affiliation_id not in new_affiliation_ids:
                            self.insert_cross_record("records_x_affiliations", "affiliations", affiliation_id,
                                                     record["record_id"])
                            modified_upstream = True
                        if affiliation_id not in new_affiliation_ids:
                            new_affiliation_ids.append(affiliation_id)
                for eid in existing_affiliation_ids:
                    if eid not in new_affiliation_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_affiliations", eid, record["record_id"])

            if "rights" in record:
                if not isinstance(record["rights"], list):
                    record["rights"] = [record["rights"]]
                existing_rights_recs = self.get_multiple_records("records_x_rights", "rights_id", "record_id",
                                                                 record["record_id"])
                existing_rights_ids = [e["rights_id"] for e in existing_rights_recs]
                new_rights_ids = []
                for rights in record["rights"]:
                    # Use a hash for lookups so we don't need to maintain a full text index
                    sha1 = hashlib.sha1()
                    sha1.update(rights.encode('utf-8'))
                    rights_hash = sha1.hexdigest()
                    rights_id = self.get_single_record_id("rights", rights_hash)
                    if rights_id is None:
                        self.delete_all_related_records("records_x_rights", record[
                            "record_id"])  # Needed for transition, can be removed once all rights rows have hashes
                        extras = {"rights": rights}
                        rights_id = self.insert_related_record("rights", rights_hash, **extras)
                        modified_upstream = True
                    if rights_id is not None:
                        new_rights_ids.append(rights_id)
                        if rights_id not in existing_rights_ids:
                            self.insert_cross_record("records_x_rights", "rights", rights_id, record["record_id"])
                            modified_upstream = True
                for eid in existing_rights_ids:
                    if eid not in new_rights_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_rights", eid, record["record_id"])

            if "description" in record:
                if not isinstance(record["description"], list):
                    record["description"] = [record["description"]]
                existing_description_recs = self.get_multiple_records("descriptions", "description_id", "record_id",
                                                              record["record_id"], "and language='en'")
                existing_description_ids = [e["description_id"] for e in existing_description_recs]
                new_description_ids = []
                for description in record["description"]:
                    # Use a hash for lookups so we don't need to maintain a full text index
                    if description is not None:
                        sha1 = hashlib.sha1()
                        sha1.update(description.encode('utf-8'))
                        description_hash = sha1.hexdigest()
                        description_id = self.get_single_record_id("descriptions", description_hash, "and record_id=" + str(
                            record["record_id"]) + " and language='en'")
                        if description_id is None:
                            extras = {"record_id": record["record_id"], "language": "en", "description": description}
                            description_id = self.insert_related_record("descriptions", description_hash, **extras)
                            modified_upstream = True
                        if description_id is not None:
                            new_description_ids.append(description_id)
                for eid in existing_description_ids:
                    if eid not in new_description_ids:
                        self.delete_row_generic("descriptions", "description_id", eid)
                        modified_upstream = True

            if "description_fr" in record:
                if not isinstance(record["description_fr"], list):
                    record["description_fr"] = [record["description_fr"]]
                existing_description_recs = self.get_multiple_records("descriptions", "description_id", "record_id",
                                                                      record["record_id"], "and language='fr'")
                existing_description_ids = [e["description_id"] for e in existing_description_recs]
                new_description_ids = []
                for description in record["description_fr"]:
                    # Use a hash for lookups so we don't need to maintain a full text index
                    if description is not None:
                        sha1 = hashlib.sha1()
                        sha1.update(description.encode('utf-8'))
                        description_hash = sha1.hexdigest()
                        description_id = self.get_single_record_id("descriptions", description_hash, "and record_id=" + str(
                            record["record_id"]) + " and language='fr'")
                        if description_id is None:
                            extras = {"record_id": record["record_id"], "language": "fr", "description": description}
                            description_id = self.insert_related_record("descriptions", description_hash, **extras)
                            modified_upstream = True
                        if description_id is not None:
                            new_description_ids.append(description_id)
                for eid in existing_description_ids:
                    if eid not in new_description_ids:
                        self.delete_row_generic("descriptions", "description_id", eid)
                        modified_upstream = True

            if "tags" in record:
                if not isinstance(record["tags"], list):
                    record["tags"] = [record["tags"]]
                existing_tag_recs = self.get_multiple_records("records_x_tags", "tag_id", "record_id",
                                                              record["record_id"])
                existing_tag_ids = [e["tag_id"] for e in existing_tag_recs]
                new_tag_ids = []
                for tag in record["tags"]:
                    tag_id = self.get_single_record_id("tags", tag, "and language='en'")
                    if tag_id is None:
                        extras = {"language": "en"}
                        tag_id = self.insert_related_record("tags", tag, **extras)
                        modified_upstream = True
                    if tag_id is not None:
                        new_tag_ids.append(tag_id)
                        if tag_id not in existing_tag_ids:
                            self.insert_cross_record("records_x_tags", "tags", tag_id, record["record_id"])
                            modified_upstream = True
                for eid in existing_tag_ids:
                    if eid not in new_tag_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_tags", eid, record["record_id"])

            if "tags_fr" in record:
                if not isinstance(record["tags_fr"], list):
                    record["tags_fr"] = [record["tags_fr"]]
                existing_tag_recs = self.get_multiple_records("records_x_tags", "tag_id", "record_id",
                                                              record["record_id"])
                existing_tag_ids = [e["tag_id"] for e in existing_tag_recs]
                new_tag_ids = []
                for tag in record["tags_fr"]:
                    tag_id = self.get_single_record_id("tags", tag, "and language='fr'")
                    if tag_id is None:
                        extras = {"language": "fr"}
                        tag_id = self.insert_related_record("tags", tag, **extras)
                        modified_upstream = True
                    if tag_id is not None:
                        new_tag_ids.append(tag_id)
                        if tag_id not in existing_tag_ids:
                            self.insert_cross_record("records_x_tags", "tags", tag_id, record["record_id"])
                            modified_upstream = True
                for eid in existing_tag_ids:
                    if eid not in new_tag_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_tags", eid, record["record_id"])

            if "access" in record:
                if not isinstance(record["access"], list):
                    record["access"] = [record["access"]]
                existing_access_recs = self.get_multiple_records("records_x_access", "access_id", "record_id",
                                                                 record["record_id"])
                existing_access_ids = [e["access_id"] for e in existing_access_recs]
                new_access_ids = []
                for access in record["access"]:
                    access_id = self.get_single_record_id("access", access)
                    if access_id is None:
                        access_id = self.insert_related_record("access", access)
                        modified_upstream = True
                    if access_id is not None:
                        new_access_ids.append(access_id)
                        if access_id not in existing_access_ids:
                            self.insert_cross_record("records_x_access", "access", access_id, record["record_id"])
                            modified_upstream = True
                for eid in existing_access_ids:
                    if eid not in new_access_ids:
                        modified_upstream = True
                        self.delete_one_related_record("records_x_access", eid, record["record_id"])

            if "geobboxes" in record:
                existing_geobbox_recs = self.get_multiple_records("geobbox", "*", "record_id",
                                                                    record["record_id"])
                existing_geobbox_ids = [e["geobbox_id"] for e in existing_geobbox_recs]
                new_geobbox_ids = []
                for geobbox in record["geobboxes"]:
                    # Fill in any missing values
                    if "eastLon" not in geobbox and "westLon" in geobbox:
                        geobbox["eastLon"] = geobbox["westLon"]
                    if "westLon" not in geobbox and "eastLon" in geobbox:
                        geobbox["westLon"] = geobbox["eastLon"]
                    if "northLat" not in geobbox and "southLat" in geobbox:
                        geobbox["northLat"] = geobbox["southLat"]
                    if "southLat" not in geobbox and "northLat" in geobbox:
                            geobbox["southLat"] = geobbox["northLat"]

                    if "westLon" in geobbox and "eastLon" in geobbox and "northLat" in geobbox and "southLat" in geobbox:
                        if geobbox["westLon"] != geobbox["eastLon"] or geobbox["northLat"] != geobbox["southLat"]:
                            # If west/east or north/south don't match, this is a box
                            extrawhere = "and westLon=" + str(geobbox["westLon"]) + " and eastLon=" + str(geobbox["eastLon"]) + \
                                        " and northLat=" + str(geobbox["northLat"]) + " and southLat=" + str(geobbox["southLat"])
                            extras = {"westLon": geobbox["westLon"], "eastLon": geobbox["eastLon"],
                                      "northLat": geobbox["northLat"], "southLat": geobbox["southLat"]}
                            geobbox_id = self.get_single_record_id("geobbox", record["record_id"], extrawhere)
                            if geobbox_id is None:
                                geobbox_id = self.insert_related_record("geobbox", record["record_id"], **extras)
                                modified_upstream = True
                            if geobbox_id is not None:
                                new_geobbox_ids.append(geobbox_id)
                        else:
                            # If west/east and north/south match, this is a point
                            if "geopoints" not in record:
                                record["geopoints"] = []
                            record["geopoints"].append({"lat": geobbox["northLat"], "lon": geobbox["westLon"]})
                # Remove any existing boxes that aren't also in the new boxes
                for eid in existing_geobbox_ids:
                    if eid not in new_geobbox_ids:
                        self.delete_row_generic("geobbox", "geobbox_id", eid)
                        modified_upstream = True

            if "geopoints" in record:
                existing_geopoint_recs = self.get_multiple_records("geopoint", "*", "record_id",
                                                                    record["record_id"])
                existing_geopoint_ids = [e["geopoint_id"] for e in existing_geopoint_recs]
                new_geopoint_ids = []
                for geopoint in record["geopoints"]:
                    if "lat" in geopoint and "lon" in geopoint:
                        extrawhere = "and lat=" + str(geopoint["lat"]) + " and lon=" + str(geopoint["lon"])
                        extras = {"lat": geopoint["lat"], "lon": geopoint["lon"]}
                        geopoint_id = self.get_single_record_id("geopoint", record["record_id"], extrawhere)
                        if geopoint_id is None:
                            self.insert_related_record("geopoint", record["record_id"], **extras)
                            modified_upstream = True
                        if geopoint_id is not None:
                            new_geopoint_ids.append(geopoint_id)
                # Remove any existing points that aren't also in the new points
                for eid in existing_geopoint_ids:
                    if eid not in new_geopoint_ids:
                        self.delete_row_generic("geopoint", "geopoint_id", eid)
                        modified_upstream = True

            if "geoplaces" in record:
                existing_geoplace_recs = self.get_multiple_records("records_x_geoplace", "*", "record_id",
                                                              record["record_id"])
                existing_geoplace_ids = [e["geoplace_id"] for e in existing_geoplace_recs]
                new_geoplace_ids = []
                for geoplace in record["geoplaces"]:
                    if "country" not in geoplace:
                        geoplace["country"] = ""
                    if "province_state" not in geoplace:
                        geoplace["province_state"] = ""
                    if "city" not in geoplace:
                        geoplace["city"] = ""
                    if "other" not in geoplace:
                        geoplace["other"] = ""
                    if "place_name" not in geoplace:
                        geoplace["place_name"] = ""
                    extrawhere = "and country='" + geoplace["country"] + "' and province_state='" + geoplace["province_state"] +\
                                 "' and city='" + geoplace["city"] + "' and other='" + geoplace["other"] + "'"
                    extras = {"country": geoplace["country"], "province_state": geoplace["province_state"], \
                              "city": geoplace["city"], "other": geoplace["other"]}
                    geoplace_id = self.get_single_record_id("geoplace", geoplace["place_name"], extrawhere)

                    if geoplace_id is None:
                        geoplace_id = self.insert_related_record("geoplace", geoplace["place_name"], **extras)
                    if geoplace_id is not None:
                        new_geoplace_ids.append(geoplace_id)
                        if geoplace_id not in existing_geoplace_ids:
                            self.insert_cross_record("records_x_geoplace", "geoplace", geoplace_id, record["record_id"])

                for eid in existing_geoplace_ids:
                    if eid not in new_geoplace_ids:
                        records_x_geoplace_id = self.get_multiple_records("records_x_geoplace", "records_x_geoplace_id", "record_id", record["record_id"]," and geoplace_id='" + str(eid) + "'")[0]["records_x_geoplace_id"]
                        self.delete_row_generic("records_x_geoplace", "records_x_geoplace_id", records_x_geoplace_id)
                        modified_upstream = True

            if "geofiles" in record:
                existing_geofile_recs = self.get_multiple_records("geofile", "*", "record_id",
                                                               record["record_id"])
                existing_geofile_ids = [e["geofile_id"] for e in existing_geofile_recs]
                new_geofile_ids = []
                for geofile in record["geofiles"]:
                    if "filename" in geofile and "uri" in geofile:
                        extrawhere = "and filename='" + geofile["filename"] + "' and uri='" + geofile["uri"] + "'"
                        extras = {"filename": geofile["filename"], "uri": geofile["uri"]}
                        geofile_id = self.get_single_record_id("geofile", record["record_id"], extrawhere)
                        if geofile_id is None:
                            geofile_id = self.insert_related_record("geofile", record["record_id"], **extras)
                            modified_upstream = True
                        if geofile_id is not None:
                            new_geofile_ids.append(geofile_id)
                # Remove any existing files that aren't also in the new files
                for eid in existing_geofile_ids:
                    if eid not in new_geofile_ids:
                        self.delete_row_generic("geofile", "geofile_id", eid)
                        modified_upstream = True

            if len(domain_metadata) > 0:
                existing_metadata_ids = self.get_multiple_records("domain_metadata", "metadata_id", "record_id",
                                                                  record["record_id"])
                if not existing_metadata_ids:
                    for field_uri in domain_metadata:
                        field_pieces = field_uri.split("#")
                        domain_schema = field_pieces[0]
                        field_name = field_pieces[1]
                        schema_id = self.get_single_record_id("domain_schemas", domain_schema)
                        if schema_id is None:
                            schema_id = self.insert_related_record("domain_schemas", domain_schema)
                        if not isinstance(domain_metadata[field_uri], list):
                            domain_metadata[field_uri] = [domain_metadata[field_uri]]
                        for field_value in domain_metadata[field_uri]:
                            extras = {"record_id": record["record_id"], "field_name": field_name,
                                      "field_value": field_value}
                            self.insert_related_record("domain_metadata", schema_id, **extras)

        return None

    def get_stale_records(self, stale_timestamp, repo_id, max_records_updated_per_run):
        con = self.getConnection()
        records = []
        with con:
            cur = self.getCursor(con)
            cur.execute(self._prep("""SELECT recs.record_id, recs.title, recs.pub_date, recs.series, recs.modified_timestamp,
                recs.local_identifier, recs.item_url, repos.repository_id, repos.repository_type
                FROM records recs, repositories repos
                where recs.repository_id = repos.repository_id and recs.modified_timestamp < ? and repos.repository_id = ? and recs.deleted = 0
                LIMIT ?"""), (stale_timestamp, repo_id, max_records_updated_per_run))
            if cur is not None:
                records = cur.fetchall()

        return records

    def touch_record(self, record):
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            try:
                cur.execute(self._prep("UPDATE records set modified_timestamp = ? where record_id = ?"),
                            (time.time(), record['record_id']))
            except:
                self.logger.error("Unable to update modified_timestamp for record id {}".format(record['record_id']))
                return False

        return True

    def write_header(self, local_identifier, repo_id):
        record_id = self.get_single_record_id("records", local_identifier, "and repository_id=" + str(repo_id))
        if record_id is None:
            con = self.getConnection()
            with con:
                cur = self.getCursor(con)
                try:
                    cur.execute(self._prep(
                        "INSERT INTO records (title, title_fr, pub_date, series, modified_timestamp, local_identifier, item_url, repository_id) VALUES(?,?,?,?,?,?,?,?)"),
                        ("", "", "", "", 0, local_identifier, "", repo_id))
                except self.dblayer.IntegrityError as e:
                    self.logger.error("Error creating record header: {}".format(e))

        return None
