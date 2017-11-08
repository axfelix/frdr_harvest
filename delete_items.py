#!/usr/bin/env python

"""Create the Gmeta to delete items

After the gmeta is created, then it will need to be sent to Globus Datasearch API
For example, via the update_search.py script in /opt/rdm

Usage:
  delete_items.py file_of_ids

  or

  cat list_of_ids | delete_items.py

"""

import json
import sys
import copy
import os

cmd = "/opt/rdm/search_client/search-client"
host = "search.api.globus.org"

# Globus has changed from index NAMES to index UUIDs. Use one of the following for the index:
# frdr:      "9be6dd95-48f0-48bb-82aa-c6577a988775"
# frdr-test: "5f262052-d6cc-4a2d-8fee-79678088af04"
# frdr-demo: "f317467b-98d4-43d1-b73a-fbeb6f0fd7d6"
# frdr-dev:  "37169d3a-beff-4367-bc6e-78f7e39e902c"
index = "5f262052-d6cc-4a2d-8fee-79678088af04"

with open(sys.argv[1], 'r') if len(sys.argv) > 1 else sys.stdin as f:
    myline = f.readline()
    while myline:
        id = myline.rstrip('\n')
        print("Deleting item: %s" % (id) )
        command = [ cmd, "--host " + host, "--index " + index, "subject delete " + id ]
        ret = os.popen(" ".join(command) ).read()
        myline = f.readline()