import json
import harvester.Exporter as Exporter
import re


def check_dd(decimal_string):
    return re.search('^[-]?((\d+(\.\d+)?)|(\.\d+))$', decimal_string) is not None


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
        records_sql = """SELECT recs.record_id, recs.item_url, recs.pub_date, recs.title, recs.title_fr, recs.item_url, recs.series, recs.repository_id, reps.repository_url, reps.repository_name, reps.repository_type
            FROM records recs
            JOIN repositories reps on reps.repository_id = recs.repository_id
            WHERE recs.geodisy_harvested = 0 AND recs.deleted = 0 AND (recs.title <>''OR recs.title_fr <> '') LIMIT ?"""
        records_cursor.execute(self.db._prep(records_sql), (self.records_per_loop,))

        records = []
        for row in records_cursor:
            record = (dict(zip(['record_id','item_url','pub_date','title', 'title_fr','item_url','series','repository_id','repository_url', 'repository_name','repository_type'], row)))
            records.append(record)
        cur = self.db.getLambdaCursor()
        records_sql = """SELECT count(*)
                            FROM records recs
                            JOIN repositories reps on reps.repository_id = recs.repository_id
                            WHERE recs.geodisy_harvested = 0 AND recs.deleted = 0"""
        cur.execute(records_sql)
        vals = self._rows_to_list(cur)
        for val in vals:
            total_records = val
            break


        # TODO finish generating a list of deleted items for Geodisy
        deleted_sql = """SELECT recs.record_id, recs.item_url, recs.item_url, recs.repository_id, reps.repository_url
                    FROM records recs
                    JOIN repositories reps on reps.repository_id = recs.repository_id
                    WHERE geodisy_harvested = 0 AND deleted = 1 LIMIT ?"""
        records_cursor.execute(self.db._prep(deleted_sql), (self.records_per_loop,))

        deleted = []
        for row in records_cursor:
            deleted_record = (
                dict(zip(['record_id', 'item_url', 'item_url', 'repository_id', 'repository_url'], row)))
            deleted.append(deleted_record)
        self.get_batch_deleted_records(0, len(records), deleted)

        # self.get_batch_record_metadata(0, len(records), records)
        return self.get_batch_record_metadata(0, total_records, records)
        



    # create json in batches
    def get_batch_record_metadata(self, start, last_rec_num, records):
        self.output_buffer = []
        current = start
        self.batch_number = 1
        rec_nums = self.records_per_loop
        stop = (start + rec_nums) if (start + rec_nums < last_rec_num) else last_rec_num
        while current < stop:
            r = self._generate_dv_json(records[current])
            if r != "":
                self.output_buffer.append(r)
            current += 1
        done = current == last_rec_num
        #self._write_batch(done)
        return self._write_batch(done)

    # TODO create list of deleted records to add to json being sent to Geodisy
    def get_batch_deleted_records(self,start, last_rec_num, deleted_records):
        pass

    # make calls back to the FRDR Harvester's db to get the needed info to generate a DV-like json for Geodisy to parse
    def _generate_dv_json(self, record):
        if self.get_citation_metadata_field(record) is None:
            return {}

        record_dv_data = {
            "id": record["record_id"],
            "persistentUrl": record["item_url"],
            "publicationDate": record["pub_date"],
            "license": self.get_license(record),
            "repo_base_url": record["repository_url"],
            "publisher": record["repository_name"]
        }
        geo = self.get_geospatial_metadata(record)
        files = self.get_files(record)
        metadata_blocks = {"citation": self.get_citation_metadata_field(record)}
        if geo:
            metadata_blocks["geospatial"] = geo
        if files:
            metadata_blocks["files"] = files
        metadata = {
            "metadataBlocks": metadata_blocks
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
        title = record["title"] if not record["title"] == '' else record["title_fr"]
        fields = [self.json_dv_dict("title", "false", "primitive", title),
                  self.json_dv_dict("author", "true", "compound", self.get_authors(record)),
                  self.json_dv_dict("dsDescription", "true", "compound", self.get_descriptions(record)),
                  self.json_dv_dict("subject", "true", "compound", self.get_subjects(record)),
                  self.json_dv_dict("keyword", "true", "compound", self.get_keywords(record)),
                  self.json_dv_dict("series", "false", "compound", self.get_series(record))]

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
                retlist.append({"authorName": self.json_dv_dict("authorName", "false", "primitive", val)})
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
                retlist.append({"dsDescriptionValue": self.json_dv_dict("dsDescriptionValue", "false", "primitive", val)})
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
                retlist.append({"keywordValue": self.json_dv_dict("keywordValue", "false", "primitive", val)})
        except:
            self.logger.error("Unable to get keyword metadata field for creating Dataverse JSON")
        return retlist

    def get_series(self, record):
        return {"seriesName": self.json_dv_dict("seriesName", "false", "primitive", record["series"])}

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
        geo_cov = self.get_geo_coverage(record)
        if geo_cov:
            geographic_coverage = self.json_dv_dict("geographicCoverage", "true", "compound", geo_cov)
        else:
            geographic_coverage = ""
        geo_bb = self.get_geo_bbox(record)
        if geo_bb:
            geographic_bounding_box = self.json_dv_dict("geographicBoundingBox", "true", "compound", geo_bb)
        else:
            geographic_bounding_box = ""
        geospatial_groups = []
        if geographic_coverage is not None and geographic_coverage != "":
            geospatial_groups.append(geographic_coverage)
        if geographic_bounding_box is not None and geographic_bounding_box != "":
            geospatial_groups.append(geographic_bounding_box)
        if geospatial_groups:
            geospatial["displayName"] = "Geospatial Metadata"
            geospatial["fields"] = geospatial_groups
            return geospatial

    def get_geo_coverage(self, record):
        geos_coverage = []

        try:
            geocur = self.db.getDictCursor()
            geo_places_sql = """SELECT geoplace.country, geoplace.province_state, geoplace.city, geoplace.other, geoplace.place_name 
                FROM geoplace
                JOIN records_x_geoplace ON records_x_geoplace.geoplace_id = geoplace.geoplace_id
                WHERE records_x_geoplace.record_id=?"""
            geocur.execute(self.db._prep(geo_places_sql), (record["record_id"],))

            for row in geocur:
                # What happened to place_name? It does not appear in the location dict below
                country = row["country"]
                state = row["province_state"]
                city = row["city"]
                other = row["other"]
                location = {
                    "country": self.json_dv_dict("country", "false", "controlledVocabulary", country),
                    "state": self.json_dv_dict("state", "false", "primative", state),
                    "city": self.json_dv_dict("city", "false", "primative", city),
                    "otherGeographicCoverage": self.json_dv_dict("otherGeographicCoverage", "false", "primative", other)
                }
                if country != "" or state != "" or city != "" or other != "":
                    geos_coverage.append(location)
        except:
            self.logger.error("Unable to get geoplace metadata fields for record: {}".format(record["record_id"]))
        if not geos_coverage:
            return ""
        return geos_coverage

    def get_geo_bbox(self, record):
        cur = self.db.getDictCursor()
        try:
            cur.execute(self.db._prep(
                """SELECT westLon, eastLon, northLat, southLat FROM geobbox WHERE record_id=?"""),
                (record["record_id"],))
            coords = []
            for row in cur:
                west = str(row["westlon"])
                east = str(row["eastlon"])
                north = str(row["northlat"])
                south = str(row["southlat"])
                if check_dd(west) and check_dd(east) and check_dd(north) and check_dd(south):
                    val = {"westLon": row["westlon"], "eastLon": row["eastlon"], "northLat": row["northlat"], "southLat": row["southlat"]}
                    coords.append(self.get_bbox(val))
            if coords:
                return coords
        except:
            self.logger.error("Unable to get geobbox metadata fields for creating json for Geodisy")
            return ""

    def get_bbox(self, bbox_dict):
        return {
            "westLongitude": self.json_dv_dict("westLongitude", "false", "primative", str(bbox_dict["westLon"])),
            "eastLongitude": self.json_dv_dict("eastLongitude", "false", "primative", str(bbox_dict["eastLon"])),
            "northLatitude": self.json_dv_dict("northLatitude", "false", "primative", str(bbox_dict["northLat"])),
            "southLatitude": self.json_dv_dict("southLatitude", "false", "primative", str(bbox_dict["southLat"]))
        }

    def get_files(self, record):
        files = []
        cur = self.db.getDictCursor()
        try:
            cur.execute(self.db._prep(
                    """SELECT filename, uri FROM geofile WHERE record_id=?"""),(record["record_id"],))
            for row in cur:
                val = {"filename": row["filename"], "uri": row["uri"]}
                files.append(self.get_file_info(val, record))
            if files:
                return files
        except:
            self.logger.error("Unable to get geo file metadata fields for creating json for Geodisy")

    def get_file_info(self, file_info, record):
        file_metadata = {
            "frdr_harvester": True,
            "restricted": False,
            "dataFile": {
                "fileURI": file_info["uri"],
                "filename": file_info["filename"]
            }
        }
        return file_metadata

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
