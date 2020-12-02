import re
import json
import harvester.Exporter as Exporter


class ExporterGmeta(Exporter.Exporter):
    """ Read records from the database and export to gmeta """

    def __init__(self, db, log, finalconfig):
        self.export_format = "gmeta"
        super().__init__(db, log, finalconfig)

    def _generate(self, only_new_records):
        self.logger.info("Exporter: generate called for gmeta")
        self.output_buffer = []
        deleted = []

        try:
            lastrun_timestamp = self.db.get_setting("last_run_timestamp")
        except:
            lastrun_timestamp = 0

        records_con = self.db.getConnection()
        with records_con:
            records_cursor = self.db.getRowCursor()

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
                self._write_batch()

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
                litecur = self.db.getRowCursor()

                # TODO update this to export new geospatial elements
                litecur.execute(self.db._prep("SELECT coordinate_type, lat, lon FROM geospatial WHERE record_id=?"),
                                (record["record_id"],))
                geodata = litecur.fetchall()
                record["frdr_geospatial"] = []
                polycoordinates = []

                try:
                    for coordinate in geodata:
                        if coordinate[0] == "Polygon":
                            polycoordinates.append([float(coordinate[1]), float(coordinate[2])])
                        else:
                            record["frdr_geospatial"].append({"frdr_geospatial_type": "Feature",
                                                              "frdr_geospatial_geometry": {
                                                                  "frdr_geometry_type": coordinate[0],
                                                                  "frdr_geometry_coordinates": [float(coordinate[1]),
                                                                                                float(coordinate[2])]}})
                except:
                    pass

                if polycoordinates:
                    record["frdr_geospatial"].append({"frdr_geospatial_type": "Feature",
                                                      "frdr_geospatial_geometry": {"frdr_geometry_type": "Polygon",
                                                                                   "frdr_geometry_coordinates": polycoordinates}})

            with con:
                litecur = self.db.getLambdaCursor()

                # attach the other values to the dict
                litecur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id
                    WHERE records_x_creators.record_id=? AND records_x_creators.is_contributor=0 order by records_x_creators_id asc"""),
                                (record["record_id"],))
                record["dc_contributor_author"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT affiliations.affiliation FROM affiliations JOIN records_x_affiliations on records_x_affiliations.affiliation_id = affiliations.affiliation_id
                    WHERE records_x_affiliations.record_id=?"""), (record["record_id"],))
                record["datacite_creatorAffiliation"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id
                    WHERE records_x_creators.record_id=? AND records_x_creators.is_contributor=1 order by records_x_creators_id asc"""),
                                (record["record_id"],))
                record["dc_contributor"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT subjects.subject FROM subjects JOIN records_x_subjects on records_x_subjects.subject_id = subjects.subject_id
                    WHERE records_x_subjects.record_id=? and subjects.language='en'"""), (record["record_id"],))
                record["frdr_category_en"] = self._rows_to_list(litecur)


                litecur.execute(self.db._prep("""SELECT subjects.subject FROM subjects JOIN records_x_subjects on records_x_subjects.subject_id = subjects.subject_id
                    WHERE records_x_subjects.record_id=? and subjects.language='fr'"""), (record["record_id"],))
                record["frdr_category_fr"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT publishers.publisher FROM publishers JOIN records_x_publishers on records_x_publishers.publisher_id = publishers.publisher_id
                    WHERE records_x_publishers.record_id=?"""), (record["record_id"],))
                record["dc_publisher"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT rights.rights FROM rights JOIN records_x_rights on records_x_rights.rights_id = rights.rights_id
                                                       WHERE records_x_rights.record_id=?"""), (record["record_id"],))
                record["dc_rights"] = self._rows_to_list(litecur)

                litecur.execute(
                    self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='en' "),
                    (record["record_id"],))
                record["dc_description_en"] = self._rows_to_list(litecur)

                litecur.execute(
                    self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='fr' "),
                    (record["record_id"],))
                record["dc_description_fr"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id
                    WHERE records_x_tags.record_id=? and tags.language = 'en' """), (record["record_id"],))
                record["frdr_keyword_en"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id
                    WHERE records_x_tags.record_id=? and tags.language = 'fr' """), (record["record_id"],))
                record["frdr_keyword_fr"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT access.access FROM access JOIN records_x_access on records_x_access.access_id = access.access_id
                    WHERE records_x_access.record_id=?"""), (record["record_id"],))
                record["frdr_access"] = self._rows_to_list(litecur)

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
            self._write_batch()

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
