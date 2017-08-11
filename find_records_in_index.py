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
cmd = "/opt/rdm/datasearch_client/datasearch-client"
host = "search.api.globus.org"
index = "frdr-test"
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