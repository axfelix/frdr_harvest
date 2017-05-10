#!/usr/bin/env python

"""Create the Gmeta to delete items

After the gmeta is created, then it will need to be sent to Globus Datasearch API
For example, via the update_search.py script in /opt/rdm

Usage:
  delete_items.py file_of_ids
  cat list_of_ids | delete_items.py

"""

import json
import sys
import copy

# Where to put the gemta files
out_prefix = "data/gmeta_delete"

# Number of bytes to accumulate before dumping to file
buffer_limit = 5000000

gIngestTemplate = {
    "@datatype": "GIngest",
    "@version": "2016-11-09",
    "ingest_type": "GMetaList",
    "source_id": "ComputeCanada",
    "ingest_data": {
        "@datatype": "GMetaList",
        "@version": "2016-11-09",
        "gmeta": []
    }
}

gIngest = copy.deepcopy(gIngestTemplate)

buffer_size = len(json.dumps(gIngest,indent=4))
batchnum = 1

def dump_to_file(outname):
    with open(outname, "w") as f:
        json.dump(gIngest, f, indent=4)
    print("Wrote file: %s" % outname)
    return

with open(sys.argv[1], 'r') if len(sys.argv) > 1 else sys.stdin as f:
    myline = f.readline()
    while myline:
        id = myline.rstrip('\n')
        item = {
            "@datatype": "GMetaEntry", "@version": "2016-11-09", "content": {},
            "id": id,
            "mimetype": "application/json",
            "subject": id,
            "visible_to": [ "public" ]
        }
        gIngest["ingest_data"]["gmeta"].append(item)
        buffer_size = buffer_size + len(json.dumps(item,indent=4)) + 135
        if (buffer_size > buffer_limit):
            dump_to_file(out_prefix + "_" + str(batchnum) + ".json")
            gIngest = None
            gIngest = copy.deepcopy(gIngestTemplate)
            buffer_size = len(json.dumps(gIngest,indent=4))
            batchnum += 1
        myline = f.readline()
    dump_to_file(out_prefix + "_" + str(batchnum) + ".json")
