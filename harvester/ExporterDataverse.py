import json
import harvester.Exporter as Exporter
import pdb


class ExporterDataverse(Exporter.Exporter):
    """ Read records from the database and export to dataverse JSON """

    def __init__(self, db, log, finalconfig):
        self.export_format = "dataverse"
        super().__init__(db, log, finalconfig)

    # function gets all the records that are labeled as still needing Geodisy to harvest them
    def _generate(self, only_new_records):
        self.logger.info("Exporter: generate called for dataverse")
        self.logger.info("Maximum {} records per batch".format(self.records_per_loop))

        records_con = self.db.getConnection()
        with records_con:
            records_cursor = records_con.cursor()

        records_sql = """SELECT recs.record_id, recs.item_url, recs.pub_date, recs.title, recs.item_url, recs.series, recs.repository_id, reps.repository_url
            FROM records recs
            JOIN repositories reps on reps.repository_id = recs.repository_id
            WHERE geodisy_harvested = 0 LIMIT ?"""
        records_cursor.execute(self.db._prep(records_sql), (self.records_per_loop,))

        records = []
        for row in records_cursor:
            record = (dict(zip(['record_id','item_url','pub_date','title','item_url','series','repository_id','repository_url'], row)))
            records.append(record)
        self.get_batch_record_metadata(0, len(records), records)

    # create json in batches
    def get_batch_record_metadata(self, start, last_rec_num, records):
        self.output_buffer = []
        current = start
        self.batch_number = 1
        rec_nums = self.records_per_loop
        stop = (start + rec_nums) if (start + rec_nums < last_rec_num) else last_rec_num
        while current < stop:
            self.output_buffer.append(self._generate_dv_json(records[current]))
            current += 1
        done = current == last_rec_num
        self._write_batch(done)

    # make calls back to the FRDR Harvester's db to get the needed info to generate a DV-like json for Geodisy to parse
    def _generate_dv_json(self, record):
        if self.get_citation_metadata_field(record) is None:
            return {}

        record_dv_data = {
            "id": record["record_id"],
            "persistentUrl": record["item_url"],
            "publicationDate": record["pub_date"],
            "license": self.get_license(record),
            "repo_base_url": record["repository_url"]
        }

        metadata = {
            "metadataBlocks": {
                "citation": self.get_citation_metadata_field(record),
                "geospatial": self.get_geospatial_metadata(record)            
            }
        }
        record_dv_data["datasetVersion"] = metadata

        return record_dv_data

    def send_json(self, json_string, done):
        data = {"records": json_string, "finished": done}
        try:
            with open('data.txt', 'w') as outfile:
                json.dump(data, outfile)
        except:
            self.logger.error("Unable to write output data to data.txt for Geodisy")

    def get_citation_metadata_field(self, record):
        citations = {"displayName": "Citation Metadata"}
        fields = [self.json_dv_dict("title", "false", "primary", record["title"]),
                  self.json_dv_dict("author", "true", "compound", self.get_authors(record)),
                  self.json_dv_dict("dsDescription", "true", "compound", self.get_descriptions(record)),
                  self.json_dv_dict("subject", "true", "compound", self.get_subjects(record)),
                  self.json_dv_dict("keyword", "true", "compound", self.get_keywords(record)),
                  self.json_dv_dict("series", "false", "compound", [record["series"]])]

        if fields:
            citations["fields"] = fields
            return citations

    def get_authors(self, record):
        cur = self.db.getLambdaCursor()
        retlist = []
        try:
            cur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id
                WHERE records_x_creators.record_id=? AND records_x_creators.is_contributor=0 order by records_x_creators_id asc"""),
                            (record["record_id"],))
            vals = self._rows_to_list(cur)
            for val in vals:
                retlist.append({"authorName": self.json_dv_dict("authorName", "false", "primary", val)})
        except:
            self.logger.error("Unable to get author metadata field for creating Dataverse JSON")
        return retlist

    def get_descriptions(self, record):
        cur = self.db.getLambdaCursor()
        retlist = []
        try:
            cur.execute(
                self.db._prep("SELECT description FROM descriptions WHERE record_id=? and language='en' "),
                (record["record_id"],))
            vals = self._rows_to_list(cur)
            for val in vals:
                retlist.append({"dsDescriptionValue": self.json_dv_dict("dsDescriptionValue", "false", "primary", val)})
        except:
            self.logger.error("Unable to get description metadata field for creating Dataverse JSON")
        return retlist

    def get_subjects(self, record):
        cur = self.db.getLambdaCursor()
        retlist = []
        try:
            cur.execute(self.db._prep("""SELECT subjects.subject FROM subjects JOIN records_x_subjects on records_x_subjects.subject_id = subjects.subject_id
                WHERE records_x_subjects.record_id=? and subjects.language='en'"""), (record["record_id"],))
            vals = self._rows_to_list(cur)
            for val in vals:
                retlist.append(val)
        except:
            self.logger.error("Unable to get subject metadata field for creating Dataverse JSON")
        return retlist

    def get_keywords(self, record):
        cur = self.db.getLambdaCursor()
        retlist = []
        try:
            cur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id
                WHERE records_x_tags.record_id=? and tags.language = 'en' """), (record["record_id"],))
            vals = self._rows_to_list(cur)
            for val in vals:
                retlist.append({"keywordValue": self.json_dv_dict("keywordValue", "false", "primary", val)})
        except:
            self.logger.error("Unable to get keyword metadata field for creating Dataverse JSON")
        return retlist

    def get_license(self, record):
        cur = self.db.getLambdaCursor()
        retval = ""
        try:
            cur.execute(self.db._prep("""SELECT access.access FROM access JOIN records_x_access on records_x_access.access_id = access.access_id
                WHERE records_x_access.record_id=?"""), (record["record_id"],))
            vals = self._rows_to_list(cur)
            if vals:
                retval = vals[0]
        except:
            self.logger.error("Unable to get license metadata field for creating Dataverse JSON")
        return retval                

    def get_geospatial_metadata(self, record):
        geospatial = {}
        geographic_coverage = self.json_dv_dict("geographicCoverage", "true", "compound", self.get_geo_coverage(record))
        geographic_bounding_box = self.json_dv_dict("geographicBoundingBox", "true", "compound", self.get_geo_bbox(record))
        geospatial["displayName"] = "Geospatial Metadata"
        geospatial_groups = []
        if geographic_coverage is not None:
            geospatial_groups.append(geographic_coverage)
        if geographic_bounding_box is not None:
            geospatial_groups.append(geographic_bounding_box)
        if geospatial_groups:
            geospatial["field"] = geospatial_groups
            return geospatial

    def get_geo_coverage(self, record):
        geos_coverage = []

        try:
            geocur = self.db.getDictCursor()
            geo_places_sql = """SELECT gp.country, gp.province_state, gp.city, gp.other, gp.place_name 
                FROM geoplace gp
                JOIN records_x_geoplace rxg ON rxg.geoplace_id = gp.geoplace_id
                WHERE rxg.record_id=?"""
            geocur.execute(self.db._prep(geo_places_sql), (record["record_id"],))

            for row in geocur:
                val = (dict(zip(['country', 'province_state', 'city', 'other', 'place_name'], row)))
                # What happened to place_name? It does not appear in the location dict below
                location = {
                    "country": self.json_dv_dict("country", "false", "controlledVocabulary", row["country"]), 
                    "state":   self.json_dv_dict("state", "false", "primative", row["province_state"]), 
                    "city":    self.json_dv_dict("city", "false", "primative", row["city"]), 
                    "other":   self.json_dv_dict("other", "false", "primative", row["other"])
                }
                geos_coverage.append(location)
        except:
            self.logger.error("Unable to get geoplace metadata fields for record: {}".format(record["record_id"]))

        return geos_coverage

    def get_geo_bbox(self, record):
        cur = self.db.getDictCursor()
        try:
            cur.execute(self.db._prep(
                """SELECT westLon, eastLon, northLat, southLat FROM geobbox WHERE record_id=?"""),
                (record["record_id"],))
            coords = []
            for row in cur:
                val = (dict(zip(["westLon", "eastLon", "northLat", "southLat"], row)))
                coords.append(self.get_bbox(val))
            if coords:
                return self.json_dv_dict("geographicBoundingBox", "true", "compound", coords)
        except:
            self.logger.error("Unable to get geobbox metadata fields for creating json for Geodisy")

    def get_bbox(self, bbox_dict):
        return {
            "westLongitude": self.json_dv_dict("westLongitude", "false", "primative", bbox_dict["westLon"]),
            "eastLongitude": self.json_dv_dict("eastLongitude", "false", "primative", bbox_dict["eastLon"]),
            "northLatitude": self.json_dv_dict("northLatitude", "false", "primative", bbox_dict["northLat"]),
            "southLatitude": self.json_dv_dict("southLatitude", "false", "primative", bbox_dict["southLat"])
        }

    def get_files(self, record):
        cur = self.db.getLambdaCursor()
        try:
            cur.execute(self.db._prep(
                    """SELECT filename, uri FROM geofile WHERE record_id=?"""),(record["record_id"],))
            files = []
            for row in cur:
                val = (dict(zip(["filename", "uri"], row)))
                files.append(self.get_file_info(val, record))
            if files:
                return {"files": files}
        except:
            self.logger.error("Unable to get geo file metadata fields for creating json for Geodisy")

    def get_file_info(self, file_info, record):
        full = file_info["uri"]
        try:
            file_id_start = full.index("/datafile/", [0, ]) + 10
            file_id = full[file_id_start:]
            return {
                "label": file_info["filename"],
                "dataFile": {
                    "record_id": file_id, 
                    "pidURL": record["item_url"],
                    "filename": file_info["filename"]
                    }
                }
        except IndexError:
            self.logger.error("Unable to get geofile info for creating json for Geodisy, "
                              "index somehow went out of bounds")
        except ValueError:
            self.logger.error("Couldn't find 'datafile' in file uri in record {} for creating json for "
                              "Geodisy, index somehow went out of bounds".format(record["record_id"]))


    # Utility Functions____________________________________________

    def json_dv_dict(self, type_name, multiple, type_class, value):
        default_value = ""
        if type_class == "compound":
            default_value = []
        answer = {"typeName": type_name,
                  "multiple": multiple,
                  "typeClass": type_class,
                  "value": value or default_value}
        return answer

    def compounded(self, list_object):
        compounded_string = {}
        for tupleObj in list_object:
            compounded_string = compounded_string.join(
                self.json_dict(tupleObj.get("name"), tupleObj.get("multiple"), tupleObj.get("typeClass"),
                               tupleObj.get("value")))
            compounded_string = compounded_string.join(', ')
        compounded_string = compounded_string[:-2]
        return compounded_string.join(" ]")
