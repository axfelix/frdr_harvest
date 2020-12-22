import time
import re
from string import Template
import json
import os
import sys
import html

class Exporter(object):
    """ Read records from the database and export to given formats """

    __records_per_loop = 500

    def __init__(self, db, log, finalconfig):
        self.db = db
        self.logger = log
        self.export_limit = finalconfig.get('export_file_limit_mb', 10)
        if self.db.dbtype == "postgres":
            import psycopg2
            global DictCursor
            global DictRow
            from psycopg2.extras import DictCursor
            from psycopg2.extras import DictRow

    def _rows_to_dict(self, cursor):
        newdict = []
        if cursor:
            for r in cursor:
                if r:
                    if isinstance(r, list):
                        newdict.append(r[0])
                    else:
                        newdict.append(r)
        return newdict

    def _generate_gmeta(self, export_filepath, temp_filepath, only_new_records, start_time):
        self.logger.info("Exporter: generate_gmeta called")
        self.output_buffer = []
        deleted = []

        try:
            lastrun_timestamp = self.db.get_setting("last_run_timestamp")
        except:
            lastrun_timestamp = 0

        records_con = self.db.getConnection()
        with records_con:
            records_cursor = records_con.cursor()

        records_sql = """SELECT recs.record_id, recs.title, recs.title_fr, recs.pub_date, recs.series, recs.source_url,
            recs.deleted, recs.local_identifier, recs.item_url, recs.modified_timestamp,
            repos.repository_url, repos.repository_name, repos.repository_thumbnail, repos.item_url_pattern, repos.last_crawl_timestamp
            FROM records recs, repositories repos WHERE recs.repository_id = repos.repository_id"""
        records_args = ()

        if self.export_repository_id:
            records_sql += " AND repos.repository_id = ?"
            records_args = records_args + (int(self.export_repository_id),)

        if only_new_records:
            records_sql += " AND recs.modified_timestamp >= ?"
            records_args = records_args + (lastrun_timestamp,)

        if len(records_args):
            records_cursor.execute(self.db._prep(records_sql), records_args)
        else:
            records_cursor.execute(self.db._prep(records_sql))

        buffer_limit = int(self.export_limit) * 1024 * 1024
        self.logger.info("Exporter: output file size limited to {} MB each".format(int(self.export_limit)))

        records_assembled = 0
        self.batch_number = 1
        self.buffer_size = 0
        for row in records_cursor:
            if self.buffer_size > buffer_limit:
                self._write_batch(export_filepath, temp_filepath, start_time)

            record = (dict(zip(

                ['record_id', 'title', 'title_fr', 'pub_date', 'series', 'source_url', 'deleted', 'local_identifier',
                 'item_url', 'modified_timestamp',
                 'repository_url', 'repository_name', 'repository_thumbnail', 'item_url_pattern',
                 'last_crawl_timestamp'], row)))
            record["deleted"] = int(record["deleted"])

            if record["item_url"] == "" and record["modified_timestamp"] != 0:
                record["item_url"] = self.db.construct_local_url(record)

            if record.get("item_url") is None:
                continue
                
            if record["deleted"] == 1:
                deleted.append(record["item_url"])
                continue

            if ((record["title"] is None or len(record["title"]) == 0) and 
                (record["title_fr"] is None or len(record["title_fr"]) == 0)):
                continue

            con = self.db.getConnection()
            with con:
                if self.db.getType() == "sqlite":
                    from sqlite3 import Row
                    con.row_factory = Row
                    litecur = con.cursor()
                elif self.db.getType() == "postgres":
                    litecur = con.cursor(cursor_factory=DictCursor)

                litecur.execute(self.db._prep("""SELECT geobbox.westLon, geobbox.eastLon, geobbox.northLat, geobbox.southLat
                                    FROM geobbox WHERE geobbox.record_id=?"""), (record["record_id"],))
                geobboxes = litecur.fetchall()
                if len(geobboxes) > 0:
                    record["datacite_geoLocationBox"] = []
                    for geobbox in geobboxes:
                        record["datacite_geoLocationBox"].append({"westBoundLongitude": geobbox["westLon"],
                                                                  "eastBoundLongitude": geobbox["eastLon"],
                                                                  "northBoundLatitude": geobbox["northLat"],
                                                                  "southBoundLatitude": geobbox["southLat"]})


                litecur.execute(self.db._prep("""SELECT geopoint.lat, geopoint.lon FROM geopoint WHERE geopoint.record_id=?"""), (record["record_id"],))
                geopoints = litecur.fetchall()
                if len(geopoints) > 0:
                    record["datacite_geoLocationPoint"] = []
                    for geopoint in geopoints:
                        record["datacite_geoLocationPoint"].append({"pointLatitude": geopoint["lat"],
                                                                    "pointLongitude": geopoint["lon"]})

                litecur.execute(self.db._prep("""SELECT geoplace.country, geoplace.province_state, geoplace.city, geoplace.other, geoplace.place_name
                    FROM geoplace JOIN records_x_geoplace on records_x_geoplace.geoplace_id = geoplace.geoplace_id
                                    WHERE records_x_geoplace.record_id=?"""), (record["record_id"],))
                geoplaces = litecur.fetchall()
                if len(geoplaces) > 0:
                    record["datacite_geoLocationPlace"] = []
                    for geoplace in geoplaces:
                        if geoplace["place_name"]:
                            record["datacite_geoLocationPlace"].append({"place_name": geoplace["place_name"]})
                        elif geoplace["country"] or geoplace["province_state"] or geoplace["city"] or geoplace["other"]:
                            record["datacite_geoLocationPlace"].append({"country": geoplace["country"],
                                                                        "province_state": geoplace["province_state"],
                                                                        "city": geoplace["city"],
                                                                        "additional": geoplace["other"]})

            with con:
                if self.db.getType() == "sqlite":
                    con.row_factory = lambda cursor, row: row[0]
                    litecur = con.cursor()
                elif self.db.getType() == "postgres":
                    litecur = con.cursor(cursor_factory=DictCursor)

                # attach the other values to the dict
                litecur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id
                    WHERE records_x_creators.record_id=? AND records_x_creators.is_contributor=0 order by records_x_creators_id asc"""),
                                (record["record_id"],))
                record["dc_contributor_author"] = self._rows_to_dict(litecur)

                litecur.execute(self.db._prep("""SELECT affiliations.affiliation FROM affiliations JOIN records_x_affiliations on records_x_affiliations.affiliation_id = affiliations.affiliation_id
                    WHERE records_x_affiliations.record_id=?"""), (record["record_id"],))
                record["datacite_creatorAffiliation"] = self._rows_to_dict(litecur)

                litecur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id
                    WHERE records_x_creators.record_id=? AND records_x_creators.is_contributor=1 order by records_x_creators_id asc"""),
                                (record["record_id"],))
                record["dc_contributor"] = self._rows_to_dict(litecur)

                litecur.execute(self.db._prep("""SELECT subjects.subject FROM subjects JOIN records_x_subjects on records_x_subjects.subject_id = subjects.subject_id
                    WHERE records_x_subjects.record_id=? and subjects.language='en'"""), (record["record_id"],))
                record["frdr_category_en"] = self._rows_to_dict(litecur)


                litecur.execute(self.db._prep("""SELECT subjects.subject FROM subjects JOIN records_x_subjects on records_x_subjects.subject_id = subjects.subject_id
                    WHERE records_x_subjects.record_id=? and subjects.language='fr'"""), (record["record_id"],))
                record["frdr_category_fr"] = self._rows_to_dict(litecur)

                litecur.execute(self.db._prep("""SELECT publishers.publisher FROM publishers JOIN records_x_publishers on records_x_publishers.publisher_id = publishers.publisher_id
                    WHERE records_x_publishers.record_id=?"""), (record["record_id"],))
                record["dc_publisher"] = self._rows_to_dict(litecur)

                litecur.execute(self.db._prep("""SELECT rights.rights FROM rights JOIN records_x_rights on records_x_rights.rights_id = rights.rights_id
                                                       WHERE records_x_rights.record_id=?"""), (record["record_id"],))
                record["dc_rights"] = self._rows_to_dict(litecur)

                litecur.execute(
                    self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='en' "),
                    (record["record_id"],))
                record["dc_description_en"] = self._rows_to_dict(litecur)

                litecur.execute(
                    self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='fr' "),
                    (record["record_id"],))
                record["dc_description_fr"] = self._rows_to_dict(litecur)

                litecur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id
                    WHERE records_x_tags.record_id=? and tags.language = 'en' """), (record["record_id"],))
                record["frdr_keyword_en"] = self._rows_to_dict(litecur)

                litecur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id
                    WHERE records_x_tags.record_id=? and tags.language = 'fr' """), (record["record_id"],))
                record["frdr_keyword_fr"] = self._rows_to_dict(litecur)

                litecur.execute(self.db._prep("""SELECT access.access FROM access JOIN records_x_access on records_x_access.access_id = access.access_id
                    WHERE records_x_access.record_id=?"""), (record["record_id"],))
                record["frdr_access"] = self._rows_to_dict(litecur)

            with con:
                if self.db.getType() == "sqlite":
                    from sqlite3 import Row
                    con.row_factory = Row
                    litecur = con.cursor()
                elif self.db.getType() == "postgres":
                    litecur = con.cursor(cursor_factory=DictCursor)

                litecur.execute(self.db._prep(
                    "SELECT ds.namespace, dm.field_name, dm.field_value FROM domain_metadata dm, domain_schemas ds WHERE dm.schema_id=ds.schema_id and dm.record_id=?"),
                                (record["record_id"],))
                for row in litecur:
                    domain_namespace = str(row["namespace"])
                    field_name = str(row["field_name"])
                    field_value = str(row["field_value"])
                    if domain_namespace == "http://datacite.org/schema/kernel-4":
                        custom_label = "datacite_" + field_name
                    else:
                        custom_label = domain_namespace + "#" + field_name
                    if custom_label not in record:
                        record[custom_label] = field_value
                    else:
                        if not isinstance(record[custom_label], list):
                            record[custom_label] = [record[custom_label]]
                        record[custom_label].append(field_value)


            # Convert friendly column names into dc element names
            record["dc_title_en"] = record["title"]
            record["dc_title_fr"] = record["title_fr"]
            record["dc_date"] = record["pub_date"]
            record["frdr_series"] = record["series"]
            record["frdr_origin_id"] = record["repository_name"]
            record["frdr_origin_icon"] = record["repository_thumbnail"]

            # remove unneeded columns from output
            record.pop("contact", None)
            record.pop("deleted", None)
            record.pop("item_url_pattern", None)
            record.pop("last_crawl_timestamp", None)
            record.pop("local_identifier", None)
            record.pop("modified_timestamp", None)
            record.pop("pub_date", None)
            record.pop("record_id", None)
            record.pop("repository_name", None)
            record.pop("repository_thumbnail", None)
            record.pop("repository_url", None)
            record.pop("series", None)
            record.pop("title", None)
            record.pop("title_fr", None)

            record["datacite_resourceTypeGeneral"] = "dataset"
            gmeta_data = {"@datatype": "GMetaEntry", "@version": "2016-11-09",
                          "subject": record["item_url"], "visible_to": ["public"], "mimetype": "application/json",
                          "content": record}
            self.output_buffer.append(gmeta_data)

            self.buffer_size = self.buffer_size + len(json.dumps(gmeta_data))
            records_assembled += 1
            if (records_assembled % 1000 == 0):
                self.logger.info("Done processing {} records for export".format(records_assembled))

        if self.output_buffer:
            self._write_batch(export_filepath, temp_filepath, start_time)

        self.logger.info("Export complete: {} items in {} files".format(records_assembled, self.batch_number))
        return deleted

    def change_keys(self, obj, dropkeys, renamekeys):
        """ Recursively goes through the object and replaces keys """
        if self.db.dbtype == "postgres":
            if isinstance(obj, DictRow):
                return obj
        if isinstance(obj, (str, int, float)):
            return obj
        if isinstance(obj, dict):
            new = obj.__class__()
            for k, v in obj.items():
                if k in dropkeys:
                    continue
                strip_dc = re.sub("dc_", "", k)
                if strip_dc in renamekeys:
                    datacite_key = renamekeys[strip_dc]
                else:
                    datacite_key = strip_dc
                newkey = re.sub("[:\.]", "_", datacite_key)
                new[newkey] = self.change_keys(v, dropkeys, renamekeys)
        elif isinstance(obj, (list, set, tuple)):
            new = obj.__class__(self.change_keys(v, dropkeys, renamekeys) for v in obj)
        else:
            return obj
        return new

    def xml_child_namer(self, parent):
        child_keys = {
            "contributor": "contributor",
            "creators": "creator",
            "description": "description",
            "publisher": "publisher",
            "rightsList": "rights",
            "subjects": "subject",
            "frdr_access": "access",
            "frdr_tags": "tag",
            "frdr_tags_fr": "tag",
            "visible_to": "visibility"
        }
        if parent in child_keys:
            return child_keys[parent]
        return "item"

    def _wrap_xml_output(self, gmeta_dict, timestamp):
        import dicttoxml
        from lxml import etree
        keys_to_drop = ["@context", "subject"]
        rename_keys = {"rights": "rightsList", "contributor.author": "creators", "subject": "subjects", "frdr_geospatial": "geolocation", "datacite_resourceTypeGeneral": "resourceType"}

        parser = etree.XMLParser(remove_blank_text=True, recover=True)
        xml_tree = etree.parse("schema/stub.xml", parser=parser)
        root_tag = xml_tree.getroot()

        # context_block = gmeta_dict[0]["content"]["@context"]
        # context_xml = etree.fromstring(dicttoxml.dicttoxml(context_block, attr_type=False, custom_root='schema'),
        #                                parser=parser)
        # root_tag.insert(0, context_xml)

        control_block = {"timestamp": int(round(time.mktime(timestamp))),
                         "datestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", timestamp)}
        control_xml = etree.fromstring(dicttoxml.dicttoxml(control_block, attr_type=False, custom_root='generated'),
                                       parser=parser)
        root_tag.insert(0, control_xml)

        recordtag = xml_tree.find(".//records")
        for entry in gmeta_dict:
            xml_dict = self.change_keys(entry["content"], keys_to_drop, rename_keys)
            xml_dict["identifier"] = entry["id"]
            xml_dict["visible_to"] = entry["visible_to"]
            xml_dict["language"] = "eng"

            try:
                record_xml = etree.fromstring(
                    dicttoxml.dicttoxml(xml_dict, attr_type=False, custom_root='record', item_func=self.xml_child_namer), parser=parser)
                recordtag.append(record_xml)
                for abstract in xml_tree.findall(".//description/description"):
                    abstract.set("descriptionType", "Abstract")
            except:
                self.logger.debug("Error converting dict to XML: {}".format(entry["id"]))

        return xml_tree

    def _write_batch(self, export_filepath, temp_filepath, start_time):
        self.logger.debug("Writing batch {} to output file".format(self.batch_number))
        if self.export_format == "gmeta":
            output = json.dumps({"@datatype": "GIngest", "@version": "2016-11-09",
                                 "ingest_type": "GMetaList",
                                 "ingest_data": {"@datatype": "GMetaList", "@version": "2016-11-09",
                                                 "gmeta": self.output_buffer}})
        elif self.export_format == "xml":
            output = self._wrap_xml_output(self.output_buffer, start_time)
        if output:
            self._write_to_file(output, export_filepath, temp_filepath)
        self.output_buffer = []
        self.batch_number += 1
        self.buffer_size = 0

    def _write_to_file(self, output, export_filepath, temp_filepath):
        try:
            os.mkdir(temp_filepath)
        except:
            pass

        try:
            if self.export_format == "gmeta":
                export_basename = "gmeta_" + str(self.batch_number) + ".json"
                temp_filename = os.path.join(temp_filepath, export_basename)
                with open(temp_filename, "w") as tempfile:
                    tempfile.write(output)

            elif self.export_format == "xml":
                export_basename = "export_" + str(self.batch_number) + ".xml"
                temp_filename = os.path.join(temp_filepath, export_basename)
                output.write(temp_filename, pretty_print=True, xml_declaration=True, encoding='UTF-8')

            elif self.export_format == "delete":
                export_basename = "delete.txt"
                temp_filename = os.path.join(temp_filepath, export_basename)
                with open(temp_filename, "w") as tempfile:
                    tempfile.write(output)
        except:
            self.logger.error("Unable to write output data to temporary file: {}".format(temp_filename))

        try:
            os.remove(export_filepath)
        except:
            pass

        try:
            os.rename(temp_filename, os.path.join(export_filepath, export_basename))
        except:
            self.logger.error("Unable to move temp file: {} to output file: {}".format(temp_filename,
                                                                                       os.path.join(export_filepath,
                                                                                                    export_basename)))

    def _cleanup_previous_exports(self, dirname, basename):
        pattern = basename + '_?[\d]*\.(json|xml|txt)$'
        if basename == "xml":
            pattern = 'export_?[\d]*\.xml$'
        try:
            for f in os.listdir(dirname):
                if re.search(pattern, f):
                    os.remove(os.path.join(dirname, f))
        except:
            pass

    def export_to_file(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        output = None
        start_time = time.gmtime()

        if self.export_format not in ["gmeta", "xml"]:
            self.logger.error("Unknown export format: {}".format(self.export_format))
            return

        self._cleanup_previous_exports(self.export_filepath, self.export_format)
        self._cleanup_previous_exports(self.export_filepath, "delete")
        self._cleanup_previous_exports(self.temp_filepath, self.export_format)

        delete_list = self._generate_gmeta(self.export_filepath, self.temp_filepath, self.only_new_records, start_time)

        if len(delete_list) and self.export_format == "gmeta":
            try:
                output = "\n".join(delete_list)
                self.export_format = "delete"
                self._write_to_file(output, self.export_filepath, self.temp_filepath)
            except:
                pass
