import json
import harvester.Exporter as Exporter


class DataverseJSONCreator(Exporter):

    # function gets all the records that are labeled as still needing Geodisy to harvest them
    def get_updated_records(self, export_filepath, temp_filepath, start_time, DictCursor):
        self.logger.info("Exporter: generate_dv_json called")
        self.output_buffer = []

        records_con = self.db.getConnection()
        with records_con:
            records_cursor = records_con.cursor()

        records_sql = """SELECT record_id FROM records WHERE geodisy_harvested = 0"""
        records_cursor.execute(self.db._prep(records_sql))

        buffer_limit = int(self.export_limit) * 1024 * 1024
        self.logger.info("Exporter: output file size limited to {} MB each".format(int(self.export_limit)))

        records_assembled = 0
        self.batch_number = 1
        self.buffer_size = 0
        records = []
        for row in records_cursor:
            if self.buffer_size > buffer_limit:
                self._write_batch(export_filepath, temp_filepath, start_time)

            record = (dict(zip(['record_id'], row)))
            records.append(record)

    # create json with batches of 500 records max
    def get_batch_record_metadata(self, start, last_rec_num, records, DictCursor):
        json_output = ""
        current = start
        stop = (start + 500) if (start + 500 < last_rec_num) else last_rec_num  # max 500 records per batch
        while current <= stop:
            json_output.join(self._generate_dv_json(DictCursor, records[current]))
            current += 1

        self.send_json(json_output) # send the json to Geodisy

        if current < last_rec_num:
            self.get_batch_record_metadata(current, last_rec_num, records, DictCursor)

    # make calls back to the FRDR Harvester's db to get the needed info to generate a DV-like json for Geodisy to parse
    def _generate_dv_json(self, DictCursor, record):
        json_representation = ""
        con = self.db.getConnection()
        with con:
            if self.db.getType() == "sqlite":
                from sqlite3 import Row
                con.row_factory = Row
                litecur = con.cursor()
            elif self.db.getType() == "postgres":
                litecur = con.cursor(cursor_factory=None)

            litecur.execute(self.db._prep("SELECT * FROM records WHERE record_id=?"),
                            (record["record_id"],))

            # _________________________________________ end of my code, need to edit below______________________________
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
                                                      WHERE records_x_rights.record_id=?"""),
                            (record["record_id"],))
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

        domain_schemas = {}
        with con:
            if self.db.getType() == "sqlite":
                from sqlite3 import Row
                con.row_factory = Row
                litecur = con.cursor()
            elif self.db.getType() == "postgres":
                litecur = con.cursor(cursor_factory=None)

            litecur.execute(self.db._prep(
                "SELECT ds.namespace, dm.field_name, dm.field_value FROM domain_metadata dm, domain_schemas ds WHERE dm.schema_id=ds.schema_id and dm.record_id=?"),
                (record["record_id"],))
            for row in litecur:
                domain_namespace = str(row[0])
                if domain_namespace not in domain_schemas.keys():
                    current_count = len(domain_schemas)
                    domain_schemas[domain_namespace] = "frdrcust" + str(current_count + 1)
                custom_label = domain_schemas[domain_namespace] + ":" + str(row[1])
                record[custom_label] = str(row[2])

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

        # record["@context"] = {
        #     "dc": "http://dublincore.org/documents/dcmi-terms",
        #     "frdr": "https://frdr.ca/schema/1.0",
        #     "datacite": "https://schema.labs.datacite.org/meta/kernel-4.0/metadata.xsd"
        # }
        # for custom_schema in domain_schemas:
        #     short_label = domain_schemas[custom_schema]
        #     record["@context"].update({short_label: custom_schema})
        record["datacite_resourceTypeGeneral"] = "dataset"
        gmeta_data = {"@datatype": "GMetaEntry", "@version": "2016-11-09",
                      "subject": record["item_url"], "visible_to": ["public"], "mimetype": "application/json",
                      "content": record}
        self.output_buffer.append(gmeta_data)

        self.buffer_size = self.buffer_size + len(json.dumps(gmeta_data))
        return json_representation

    def json_dict(self, type_name, multiple, type_class, value):
        answer = ("{ " + self.json_pair("typeName", type_name) + ", " + self.json_pair("multiple", multiple) + ", "
                  + self.json_pair("typeClass", type_class) + ", " + self.json_pair("value", value)) + " }"
        return answer

    @staticmethod
    def stringed(val):
        return "\"" + val + "\""

    def json_pair(self, type_name, name):
        return self.stringed(type_name) + ": " + self.stringed(name)

    def compounded(self, list_object):
        compounded_string = "[  "
        for tupleObj in list_object:
            compounded_string = compounded_string.join(self.json_dict(tupleObj.get("name"), tupleObj.get("multiple"), tupleObj.get("typeClass"), tupleObj.get("value")))
            compounded_string = compounded_string.join(', ')
        compounded_string = compounded_string[:-2]
        return compounded_string.join(" ]")