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
        self.get_batch_record_metadata(0, len(records), records, DictCursor)

    # create json with batches of 500 records max
    def get_batch_record_metadata(self, start, last_rec_num, records, DictCursor):
        json_output = []
        current = start
        stop = (start + 500) if (start + 500 < last_rec_num) else last_rec_num  # max 500 records per batch
        while current <= stop:
            json_output.append(self._generate_dv_json(DictCursor, records[current]))
            current += 1

        done = current == last_rec_num
        self.send_json(json_output, done)  # send the json to Geodisy

    # make calls back to the FRDR Harvester's db to get the needed info to generate a DV-like json for Geodisy to parse
    def _generate_dv_json(self, DictCursor, record):
        json_representation = {}
        record = self.get_base_metadata_fields(record)
        json_representation["id"] = record["id"]
        json_representation["persistentUrl"] = record["item_url"]
        json_representation["publicationDate"] = record["pub_date"]
        json_representation["license"] = self.get_license(record)
        dataset = {}
        metadata = {}
        metadata_blocks = {self.stringed("citation"): self.get_citation_metadata_field(record),
                           self.stringed("geospatial"): self.get_geospatial_metadata(record)}
        metadata[self.stringed("metadataBlocks")] = metadata_blocks
        metadata[self.stringed("files")] = self.get_files(record)
        dataset[self.stringed("datasetVersion")] = metadata

        return json_representation

    def send_json(self, json_string, done):
        data = {"records": json_string, "finished": done}
        # TODO how to get file to Geodisy
        with open('data.txt', 'w') as outfile:
            json.dump(data, outfile)

    def json_dict_simple(self, type_name, multiple, type_class, value):
        answer = {self.stringed("typeName"): self.stringed(type_name),
                  self.stringed("multiple"): multiple,
                  self.stringed("typeClass"): self.stringed(type_class),
                  self.stringed("value"): self.stringed(value)}

        return answer

    def json_dict_compound(self, type_name, multiple, value):
        answer = {self.stringed("typeName"): self.stringed(type_name),
                  self.stringed("multiple"): multiple,
                  self.stringed("typeClass"): self.stringed("compound"),
                  self.stringed("value"): value}

        return answer

    @staticmethod
    def stringed(val):
        return "\"" + val + "\""

    def json_pair(self, type_name, name):
        return self.stringed(type_name) + ": " + self.stringed(name)

    def compounded(self, list_object):
        compounded_string = {}
        for tupleObj in list_object:
            compounded_string = compounded_string.join(
                self.json_dict(tupleObj.get("name"), tupleObj.get("multiple"), tupleObj.get("typeClass"),
                               tupleObj.get("value")))
            compounded_string = compounded_string.join(', ')
        compounded_string = compounded_string[:-2]
        return compounded_string.join(" ]")

    def get_base_metadata_fields(self, record):
        # TODO
        con = self.db.getConnection()
        with con:
            basecur = self.get_connection(con)
            basecur.execute(self.db._prep(
                "SELECT rec.record_id, rec.record_url, rec.pub_date, rec.title, rec.item_url, rec.series "
                "FROM records WHERE record_id=?"),
                (record["record_id"],))
            for row in basecur:
                record = (dict(zip(['id', "persistentUrl", "publicationDate", "title", "pidURL", "seriesName"], row)))

        return record

    def get_citation_metadata_field(self, record):
        citations = {"displayName": "Citation Metadata"}
        fields = [self.json_dict_simple("title", "false", "primary", record["title"]),
                  self.json_dict_compound("author", "true", self.get_authors(record)),
                  self.json_dict_compound("dsDescription", "true", self.get_descriptions(record)),
                  self.json_dict_compound("subject", "true", self.get_subjects(record)),
                  self.json_dict_compound("keyword", "true", self.get_keywords(record)),
                  self.json_dict_compound("series", "false", [record["series"]])]
        citations["fields"] = fields

        return citations

    def get_metadata_blocks(self, record):
        answer = {self.stringed("citation"): self.getCitationMetadataFields(record),
                  self.stringed("geospatial"): self.get_geospatial_metadata(record)}
        if answer["geospatial"] is None:
            return None
        return answer

    def get_record_x_field(self, record_id, table_name, field_name):
        con = self.db.getConnection()
        with con:
            citcur = self.get_connection(con)
            citcur.execute(self.db._prep(
                "SELECT ? FROM ? WHERE record_id=?"),
                (field_name, table_name, record_id))
            ids = []
            for row in citcur:
                record = (dict(zip([field_name], row)))
                ids.append(record)
        return ids

    def get_record_x_creator(self, record_id):
        con = self.db.getConnection()
        with con:
            citcur = self.get_connection(con)
            citcur.execute(self.db._prep(
                "SELECT creator_id FROM records_x_creators WHERE record_id=? AND is_contributor = O"),
                record_id)
            ids = []
            for row in citcur:
                record = (dict(zip(["creator_id"], row)))
                ids.append(record)
        return ids

    def get_authors(self, record):
        con = self.db.getConnection()
        ids = self.get_record_x_creator(record["id"])
        authors = []
        with con:
            citcur = self.get_connection(con)
            for id in ids:
                citcur.execute(self.db._prep(
                    "SELECT creator FROM creators WHERE creator_id=?"),
                    id)
                for row in citcur:
                    creator = (dict(zip(["creator"], row)))
                    authors.append(self.json_dict_simple("authorName", "false", "primary", creator["creator"]))
            return authors

    def get_descriptions(self, record):
        con = self.db.getConnection()
        descrips = []
        with con:
            citcur = self.get_connection(con)

            citcur.execute(self.db._prep(
                "SELECT description FROM descriptions WHERE record_id=?"),
                record["record_id"])
            for row in citcur:
                description = (dict(zip(["description"], row)))
                descrips.append(
                    self.json_dict_simple("dsDescriptionValue", "false", "primary", description["description"]))
            return descrips

    def get_subjects(self, record):
        con = self.db.getConnection()
        ids = self.get_record_x_field(record["id"], "records_x_subjects", "subject_id")
        subjects = []
        with con:
            citcur = self.get_connection(con)
            for id in ids:
                citcur.execute(self.db._prep(
                    "SELECT subject FROM subjects WHERE subject_id=?"),
                    id)
                for row in citcur:
                    subject = (dict(zip(["subject"], row)))
                    subjects.append(self.stringed(subject["subject"]))
            return subjects

    def get_keywords(self, record):
        con = self.db.getConnection()
        ids = self.get_record_x_field(record["id"], "records_x_tags", "tag_id")
        tags = []
        with con:
            citcur = self.get_connection(con)
            for id in ids:
                citcur.execute(self.db._prep(
                    "SELECT tag FROM tags WHERE tag_id=?"),
                    id)
                for row in citcur:
                    tag = (dict(zip(["tag"], row)))
                    tags.append(self.json_dict_simple("keywordValue", "false", "primary", tag["tag"]))
            return tags

    def get_license(self, record):
        ids = self.get_record_x_field(record["id"], "records_x_access", "access_id")
        id = ids[0]
        return self.get_single_value_from_table("access", "access", "access_id", id["access_id"])

    def get_single_value_from_table(self, table, field, id_field, id_field_value):
        con = self.db.getConnection()
        with con:
            citcur = self.get_connection(con)
            citcur.execute(self.db._prep(
                "SELECT ? FROM ? WHERE ?=?"), (
                field, table, id_field, id_field_value))
            for row in citcur:
                val = (dict(zip([field], row)))
        return val[field]

    def get_geospatial_metadata(self, record):
        geospatial = {}
        geographic_coverage = {self.json_dict_compound("geographicCoverage", "true", self.get_geo_coverage(record))}
        geographic_bounding_box = {self.json_dict_compound("geographicBoundingBox", "true", self.get_geo_bbox(record))}
        geospatial[self.stringed("displayName")] = self.stringed("Geospatial Metadata")
        geospatial_groups = [geographic_coverage, geographic_bounding_box]
        geospatial[self.stringed("field")] = geospatial_groups

        return geospatial

    def get_geo_coverage(self, record):
        con = self.db.getConnection()
        with con:
            geocur = self.get_connection(con)
            geocur.execute(self.db._prep(
                "SELECT country, province, city, other, place_name FROM geoplace WHERE record_id=?"),
                record["id"])
            vals = []
            for row in geocur:
                val = (dict(zip(["country, province, city, other, place_name"], row)))
                vals.append(val)
            geos_coverage = []
            for row_val in vals:
                country = {}
                country_deets = self.json_dict_simple("country", "false", "controlledVocabulary", row_val["country"])
                country[self.stringed("country")] = country_deets
                province_deets = self.json_dict_simple("state", "false", "primative", row_val["province"])
                province = {self.stringed("state"): province_deets}
                city_deets = self.json_dict_simple("city", "false", "primative", row_val["city"])
                city = {self.stringed("city"): city_deets}
                other_deets = self.json_dict_simple("other", "false", "primative", row_val["other"])
                other = {self.stringed("other"): other_deets}
                location = {country, province, city, other}
                geos_coverage.append(location)
            return geos_coverage

    def get_geo_bbox(self, record):
        con = self.db.getConnection()
        with con:
            cur = self.get_connection(con)
            cur.execute(self.db._prep(
                "SELECT westLon, eastLon, northLat, southLat FROM geobbox WHERE record_id=?"),
                record["id"])
            coords = []
            for row in cur:
                val = (dict(zip(["westLon, eastLon, northLat, southLat"], row)))
                coords.append(self.get_bbox(val))

            return self.json_dict_compound("geographicBoundingBox", "true", coords)

    def get_bbox(self, bbox_dict):
        return {self.stringed("westLongitude"):
                    self.json_dict_simple("westLongitude", "false", "primative", bbox_dict["westLon"]),
                self.stringed("eastLongitude"):
                    self.json_dict_simple("eastLongitude", "false", "primative", bbox_dict["eastLon"]),
                self.stringed("northLatitude"):
                    self.json_dict_simple("northLatitude", "false", "primative", bbox_dict["northLat"]),
                self.stringed("southLatitude"):
                    self.json_dict_simple("southLatitude", "false", "primative", bbox_dict["southLat"])
                }

    def get_connection(self, con):
        if self.db.getType() == "sqlite":
            from sqlite3 import Row
            con.row_factory = Row
            cur = con.cursor()
        elif self.db.getType() == "postgres":
            cur = con.cursor(cursor_factory=None)

        return cur

    def get_files(self, record):
        con = self.db.getConnection()
        with con:
            cur = self.get_connection(con)
            cur.execute(self.db._prep(
                "SELECT filename FROM geofile WHERE record_id=?"),
                record["id"])
            files = []
            for row in cur:
                val = (dict(zip(["filename"], row)))
                files.append(self.get_file_info(val, record))
            file = {"files": files}
            return file

    def get_file_info(self, filename, record):
        return {"label": filename, "id": record["id"], "pidURL": record["item_url"], "filename": filename}
