import os
import time
import sys
import hashlib
import json


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
            dbversion = self.get_db_version()
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
                        self.set_db_version(scriptversion)
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

    def get_db_version(self):
        dbversion = 0
        con = self.getConnection()
        res = None
        with con:
            cur = self.getCursor(con)
            cur.execute(
                self._prep("select setting_value from settings where setting_name = ? order by setting_value desc"),
                ("dbversion",))
            if cur is not None:
                res = cur.fetchone()
            if res is not None:
                dbversion = int(res['setting_value'])

        return dbversion

    def set_db_version(self, v):
        curent_version = self.get_db_version()
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            if curent_version == 0:
                cur.execute(self._prep("insert into settings(setting_value, setting_name) values (?,?)"),
                            (v, "dbversion"))
            else:
                cur.execute(self._prep("update settings set setting_value = ? where setting_name = ?"),
                            (v, "dbversion"))

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
						abort_after_numerrors=?,max_records_updated_per_run=?,update_log_after_numitems=?,record_refresh_days=?,repo_refresh_days=?,homepage_url=?
						WHERE repository_id=?"""), (
                        self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail, time.time(),
                        self.item_url_pattern,
                        self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                        self.update_log_after_numitems,
                        self.record_refresh_days, self.repo_refresh_days, self.homepage_url, self.repo_id))
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
							abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days,homepage_url) 
							VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING repository_id"""), (
                            self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail,
                            time.time(), self.item_url_pattern,
                            self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                            self.update_log_after_numitems,
                            self.record_refresh_days, self.repo_refresh_days, self.homepage_url))
                        self.repo_id = int(cur.fetchone()['repository_id'])

                    if self.dbtype == "sqlite":
                        cur.execute(self._prep("""INSERT INTO repositories 
							(repository_url, repository_set, repository_name, repository_type, repository_thumbnail, last_crawl_timestamp, item_url_pattern, enabled,
							abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days,homepage_url) 
							VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""), (
                            self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail,
                            time.time(), self.item_url_pattern,
                            self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                            self.update_log_after_numitems,
                            self.record_refresh_days, self.repo_refresh_days, self.homepage_url))
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
        if returnvalue == 0 and repo_url.startswith('https:'):
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
                                                repos[i]["repository_id"], "and deleted=0")
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
                self.delete_related_records("records_x_access", record['record_id'])
                self.delete_related_records("records_x_creators", record['record_id'])
                self.delete_related_records("records_x_publishers", record['record_id'])
                self.delete_related_records("records_x_rights", record['record_id'])
                self.delete_related_records("records_x_subjects", record['record_id'])
                self.delete_related_records("records_x_affiliations", record['record_id'])
                self.delete_related_records("records_x_tags", record['record_id'])
                self.delete_related_records("descriptions", record['record_id'])
                self.delete_related_records("geospatial", record['record_id'])
                self.delete_related_records("affiliations", record['record_id'])
                self.delete_related_records("domain_metadata", record['record_id'])
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

    def delete_related_records(self, crosstable, record_id):
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            try:
                sqlstring = "DELETE from {} where record_id=?".format(crosstable)
                cur.execute(self._prep(sqlstring), (record_id,))
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

    def create_new_record(self, rec, source_url, repo_id):
        returnvalue = None
        con = self.getConnection()
        with con:
            cur = self.getCursor(con)
            try:
                if self.dbtype == "postgres":
                    cur.execute(self._prep(
                        "INSERT INTO records (title, pub_date, contact, series, modified_timestamp, source_url, deleted, local_identifier, repository_id) VALUES(?,?,?,?,?,?,?,?,?) RETURNING record_id"),
                        (rec["title"], rec["pub_date"], rec["contact"], rec["series"], time.time(), source_url, 0,
                         rec["identifier"], repo_id))
                    returnvalue = int(cur.fetchone()['record_id'])
                if self.dbtype == "sqlite":
                    cur.execute(self._prep(
                        "INSERT INTO records (title, pub_date, contact, series, modified_timestamp, source_url, deleted, local_identifier, repository_id) VALUES(?,?,?,?,?,?,?,?,?)"),
                        (rec["title"], rec["pub_date"], rec["contact"], rec["series"], time.time(), source_url, 0,
                         rec["identifier"], repo_id))
                    returnvalue = int(cur.lastrowid)
            except self.dblayer.IntegrityError as e:
                self.logger.error("Record insertion problem: {}".format(e))

        return returnvalue

    def write_record(self, record, repo_id, metadata_prefix, domain_metadata):
        if record == None:
            return None
        record["record_id"] = self.get_single_record_id("records", record["identifier"],
                                                        "and repository_id=" + str(repo_id))

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
                record["record_id"] = self.create_new_record(record, source_url, repo_id)
            else:
                cur.execute(self._prep(
                    "UPDATE records set title=?, pub_date=?, contact=?, series=?, modified_timestamp=?, source_url=?, deleted=?, local_identifier=? WHERE record_id = ?"),
                    (record["title"], record["pub_date"], record["contact"], record["series"], time.time(),
                     source_url, 0, record["identifier"], record["record_id"]))

            if record["record_id"] is None:
                return None

            if "creator" in record:
                if not isinstance(record["creator"], list):
                    record["creator"] = [record["creator"]]
                existing_creator_recs = self.get_multiple_records("records_x_creators", "creator_id", "record_id",
                                                                  record["record_id"])
                existing_creator_ids = [e["creator_id"] for e in existing_creator_recs]
                for creator in record["creator"]:
                    creator_id = self.get_single_record_id("creators", creator)
                    if creator_id is None:
                        creator_id = self.insert_related_record("creators", creator)
                    if creator_id is not None:
                        if creator_id not in existing_creator_ids:
                            extras = {"is_contributor": 0}
                            self.insert_cross_record("records_x_creators", "creators", creator_id, record["record_id"],
                                                     **extras)

            if "contributor" in record:
                if not isinstance(record["contributor"], list):
                    record["contributor"] = [record["contributor"]]
                existing_creator_recs = self.get_multiple_records("records_x_creators", "creator_id", "record_id",
                                                                  record["record_id"])
                existing_creator_ids = [e["creator_id"] for e in existing_creator_recs]
                for creator in record["contributor"]:
                    creator_id = self.get_single_record_id("creators", creator)
                    if creator_id is None:
                        creator_id = self.insert_related_record("creators", creator)
                    if creator_id is not None:
                        if creator_id not in existing_creator_ids:
                            extras = {"is_contributor": 1}
                            self.insert_cross_record("records_x_creators", "creators", creator_id, record["record_id"],
                                                     **extras)

            if "subject" in record:
                if not isinstance(record["subject"], list):
                    record["subject"] = [record["subject"]]
                existing_subject_recs = self.get_multiple_records("records_x_subjects", "subject_id", "record_id",
                                                                  record["record_id"])
                existing_subject_ids = [e["subject_id"] for e in existing_subject_recs]
                for subject in record["subject"]:
                    subject_id = self.get_single_record_id("subjects", subject)
                    if subject_id is None:
                        subject_id = self.insert_related_record("subjects", subject)
                    if subject_id is not None:
                        if subject_id not in existing_subject_ids:
                            self.insert_cross_record("records_x_subjects", "subjects", subject_id, record["record_id"])

            if "publisher" in record:
                if not isinstance(record["publisher"], list):
                    record["publisher"] = [record["publisher"]]
                existing_publisher_recs = self.get_multiple_records("records_x_publishers", "publisher_id", "record_id",
                                                                    record["record_id"])
                existing_publisher_ids = [e["publisher_id"] for e in existing_publisher_recs]
                for publisher in record["publisher"]:
                    publisher_id = self.get_single_record_id("publishers", publisher)
                    if publisher_id is None:
                        publisher_id = self.insert_related_record("publishers", publisher)
                    if publisher_id is not None:
                        if publisher_id not in existing_publisher_ids:
                            self.insert_cross_record("records_x_publishers", "publishers", publisher_id,
                                                     record["record_id"])
            if "affiliation" in record:
                if not isinstance(record["affiliation"], list):
                    record["affiliation"] = [record["affiliation"]]
                existing_affiliation_recs = self.get_multiple_records("records_x_affiliations", "affiliation_id", "record_id",
                                                                    record["record_id"])
                existing_affiliation_ids = [e["affiliation_id"] for e in existing_affiliation_recs]
                for affil in record["affiliation"]:
                    affiliation_id = self.get_single_record_id("affiliations", affil)
                    if affiliation_id is None:
                        affiliation_id = self.insert_related_record("affiliations", affil)
                    if affiliation_id is not None:
                        if affiliation_id not in existing_affiliation_ids:
                            self.insert_cross_record("records_x_affiliations", "affiliations", affiliation_id,
                                                     record["record_id"])
            if "rights" in record:
                if not isinstance(record["rights"], list):
                    record["rights"] = [record["rights"]]
                existing_rights_recs = self.get_multiple_records("records_x_rights", "rights_id", "record_id",
                                                                 record["record_id"])
                existing_rights_ids = [e["rights_id"] for e in existing_rights_recs]
                for rights in record["rights"]:
                    # Use a hash for lookups so we don't need to maintain a full text index
                    sha1 = hashlib.sha1()
                    sha1.update(rights.encode('utf-8'))
                    rights_hash = sha1.hexdigest()
                    rights_id = self.get_single_record_id("rights", rights_hash)
                    if rights_id is None:
                        self.delete_related_records("records_x_rights", record[
                            "record_id"])  # Needed for transition, can be removed once all rights rows have hashes
                        extras = {"rights": rights}
                        rights_id = self.insert_related_record("rights", rights_hash, **extras)
                    if rights_id is not None:
                        if rights_id not in existing_rights_ids:
                            self.insert_cross_record("records_x_rights", "rights", rights_id, record["record_id"])

            if "description" in record:
                if not isinstance(record["description"], list):
                    record["description"] = [record["description"]]
                for description in record["description"]:
                    # Use a hash for lookups so we don't need to maintain a full text index
                    sha1 = hashlib.sha1()
                    sha1.update(description.encode('utf-8'))
                    description_hash = sha1.hexdigest()
                    description_id = self.get_single_record_id("descriptions", description_hash, "and record_id=" + str(
                        record["record_id"]) + " and language='en'")
                    if description_id is None:
                        extras = {"record_id": record["record_id"], "language": "en", "description": description}
                        self.insert_related_record("descriptions", description_hash, **extras)

            if "description_fr" in record:
                if not isinstance(record["description_fr"], list):
                    record["description_fr"] = [record["description_fr"]]
                for description in record["description_fr"]:
                    # Use a hash for lookups so we don't need to maintain a full text index
                    sha1 = hashlib.sha1()
                    sha1.update(description.encode('utf-8'))
                    description_hash = sha1.hexdigest()
                    description_id = self.get_single_record_id("descriptions", description_hash, "and record_id=" + str(
                        record["record_id"]) + " and language='fr'")
                    if description_id is None:
                        extras = {"record_id": record["record_id"], "language": "fr", "description": description}
                        self.insert_related_record("descriptions", description_hash, **extras)

            if "tags" in record:
                if not isinstance(record["tags"], list):
                    record["tags"] = [record["tags"]]
                existing_tag_recs = self.get_multiple_records("records_x_tags", "tag_id", "record_id",
                                                              record["record_id"])
                existing_tag_ids = [e["tag_id"] for e in existing_tag_recs]
                for tag in record["tags"]:
                    tag_id = self.get_single_record_id("tags", tag, "and language='en'")
                    if tag_id is None:
                        extras = {"language": "en"}
                        tag_id = self.insert_related_record("tags", tag, **extras)
                    if tag_id is not None:
                        if tag_id not in existing_tag_ids:
                            self.insert_cross_record("records_x_tags", "tags", tag_id, record["record_id"])

            if "tags_fr" in record:
                if not isinstance(record["tags_fr"], list):
                    record["tags_fr"] = [record["tags_fr"]]
                existing_tag_recs = self.get_multiple_records("records_x_tags", "tag_id", "record_id",
                                                              record["record_id"])
                existing_tag_ids = [e["tag_id"] for e in existing_tag_recs]
                for tag in record["tags_fr"]:
                    tag_id = self.get_single_record_id("tags", tag, "and language='fr'")
                    if tag_id is None:
                        extras = {"language": "fr"}
                        tag_id = self.insert_related_record("tags", tag, **extras)
                    if tag_id is not None:
                        if tag_id not in existing_tag_ids:
                            self.insert_cross_record("records_x_tags", "tags", tag_id, record["record_id"])

            if "access" in record:
                if not isinstance(record["access"], list):
                    record["access"] = [record["access"]]
                existing_access_recs = self.get_multiple_records("records_x_access", "access_id", "record_id",
                                                                 record["record_id"])
                existing_access_ids = [e["access_id"] for e in existing_access_recs]
                for access in record["access"]:
                    access_id = self.get_single_record_id("access", access)
                    if access_id is None:
                        access_id = self.insert_related_record("access", access)
                    if access_id is not None:
                        if access_id not in existing_access_ids:
                            self.insert_cross_record("records_x_access", "access", access_id, record["record_id"])

            if "geospatial" in record:
                existing_geospatial_ids = self.get_multiple_records("geospatial", "geospatial_id", "record_id",
                                                                    record["record_id"])
                if not existing_geospatial_ids:
                    for coordinates in record["geospatial"]["coordinates"][0]:
                        if len(coordinates) == 2:
                            extras = {"record_id": record["record_id"], "lat": coordinates[0], "lon": coordinates[1]}
                            self.insert_related_record("geospatial", record["geospatial"]["type"], **extras)

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
            cur.execute(self._prep("""SELECT recs.record_id, recs.title, recs.pub_date, recs.contact, recs.series, recs.modified_timestamp, recs.local_identifier, 
				repos.repository_id, repos.repository_type
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
                        "INSERT INTO records (title, pub_date, contact, series, modified_timestamp, local_identifier, repository_id) VALUES(?,?,?,?,?,?,?)"),
                        ("", "", "", "", 0, local_identifier, repo_id))
                except self.dblayer.IntegrityError as e:
                    self.logger.error("Error creating record header: {}".format(e))

        return None
