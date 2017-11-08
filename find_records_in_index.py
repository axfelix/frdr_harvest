#!/usr/bin/env python

"""Find all items currently in the Globus search index for a repo
The list of records is output to stdout

Usage:
  find_records_in_index.pl "repo name"

"""

import json
import sys
import re
import os

querylimit = 400
cmd = "/opt/rdm/search_client/search-client"
host = "search.api.globus.org"
# Globus has changed from index NAMES to index UUIDs. Use one of the following for the index:
# frdr:      "9be6dd95-48f0-48bb-82aa-c6577a988775"
# frdr-test: "5f262052-d6cc-4a2d-8fee-79678088af04"
# frdr-demo: "f317467b-98d4-43d1-b73a-fbeb6f0fd7d6"
# frdr-dev:  "37169d3a-beff-4367-bc6e-78f7e39e902c"
index = "f317467b-98d4-43d1-b73a-fbeb6f0fd7d6"
queryobj = {"@datatype":"GSearchRequest","@version":"2016-11-09","advanced":True,"offset":0,"limit":querylimit,"q":"*","filters":[{"@datatype":"GFilter","@version":"2016-11-09","type":"match_any","field_name":"https://frdr\\.ca/schema/1\\.0#origin\\.id","values":[""]}]}

if len(sys.argv) > 1:
    queryobj["filters"][0]["values"][0] = sys.argv[1]
    offset = 0
    found = 1
    while found > 0:
        command = [ cmd, "--host " + host, "--index " + index, "structured-query '" + json.dumps(queryobj) + "'" ]
        ret = os.popen(" ".join(command) ).read()
        found = 0
        for line in ret.splitlines():
            if re.search('http://dublincore.org/documents/dcmi-terms#source',line):
                handle = line.strip().split("\"")[3]
                found = 1
                print ("%s" % (handle))
        offset = offset + querylimit
        queryobj["offset"] = offset
else:
    print("Repo name must be the first argument to this script")