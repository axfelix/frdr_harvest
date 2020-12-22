import re
import json
import os
import sys

class Exporter(object):
    """ Read records from the database and export to given formats """

    def __init__(self, db, log, finalconfig):
        self.db = db
        self.logger = log
        self.destination = finalconfig.get('destination', "file")
        self.export_limit = finalconfig.get('export_file_limit_mb', 10)
        self.records_per_loop = 25
        if self.db.dbtype == "postgres":
            import psycopg2
            global DictRow
            from psycopg2.extras import DictRow

    def _rows_to_list(self, cursor):
        newlist = []
        if cursor:
            for r in cursor:
                if r:
                    if isinstance(r, list):
                        newlist.append(r[0])
                    else:
                        newlist.append(r)
        return newlist

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

    def _write_batch(self, finished = False):
        """ Writes the output buffer out to a file """
        self.logger.debug("Writing batch {} to output file".format(self.batch_number))
        if self.export_format == "gmeta":
            output = json.dumps({"@datatype": "GIngest", "@version": "2016-11-09",
                                 "ingest_type": "GMetaList",
                                 "ingest_data": {"@datatype": "GMetaList", "@version": "2016-11-09",
                                                 "gmeta": self.output_buffer}})
        elif self.export_format == "dataverse":
            #print(self.output_buffer)
            output = json.dumps({"records": self.output_buffer, "finished": finished})
        if output and self.destination == "file":
            self._write_to_file(output, self.export_filepath, self.temp_filepath)
        self.output_buffer = []
        self.batch_number += 1
        self.buffer_size = 0
        if output and self.destination == "stream":
            #print(output)
            return output

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

            if self.export_format == "dataverse":
                export_basename = "dv_" + str(self.batch_number) + ".json"
                temp_filename = os.path.join(temp_filepath, export_basename)
                with open(temp_filename, "w") as tempfile:
                    tempfile.write(output)

            elif self.export_format == "delete":
                export_basename = "delete.txt"
                temp_filename = os.path.join(temp_filepath, export_basename)
                with open(temp_filename, "w") as tempfile:
                    tempfile.write(output)
        except:
            self.logger.error("Unable to write output data to temporary file: {}".format(temp_filename))

        try:
            os.remove(os.path.join(export_filepath, export_basename))
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
        try:
            for f in os.listdir(dirname):
                if re.search(pattern, f):
                    os.remove(os.path.join(dirname, f))
        except:
            pass

    def export(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        output = None

        self._cleanup_previous_exports(self.export_filepath, self.export_format)
        self._cleanup_previous_exports(self.export_filepath, "delete")
        self._cleanup_previous_exports(self.temp_filepath, self.export_format)

        delete_list = self._generate(self.only_new_records)

        if delete_list is not None and len(delete_list) and self.export_format == "gmeta":
            try:
                output = "\n".join(delete_list)
                self.export_format = "delete"
                self._write_to_file(output, self.export_filepath, self.temp_filepath)
            except:
                pass
