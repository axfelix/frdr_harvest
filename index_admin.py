import configparser
import argparse
import json
import logging
import sys
import logging.config
import os
import time
import traceback
import requests

"""
Utility application to manage Globus Search Indexes for FRDR.

If running on a new host you will likely have to authorize it by running a query using the globus search-client app.
"""

_cmd = "/opt/rdm/search_client/search-client"
_api_host = "search.api.globus.org"
_tokens_filepath = "/home/harvest/.globus_search_client_tokens.json"


def get_index_config(config_file="conf/globus-indexes.conf"):
    '''
    Read ini-formatted list of Globus search indexes from disk
    :param config_file: Filename of config file
    :return: configparser-style config file
    '''

    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def get_repos_config(repos_json="conf/repos.json"):
    """
    Read json-formatted list of Globus search indexes from disk
    :param repos_json: Filename of config file
    :return: configparser-style config file
    """

    configdict = {}
    with open(repos_json, 'r') as jsonfile:
        configdict = json.load(jsonfile)

    return configdict


def query_repository(repo_name, index_uuid, display_results=False):
    """
    Display the ids  ('subjects') of all items indexed in a repository.
    :param repo_name: Textual name of repository to query, corresponds to 'name' field in conf file.
    :param index_name: Name of index, mapped by us to a UUID.
    :param display_results: Print ids to standard output
    :return: List of result ids
    """

    LOGGER.info("Querying index %s for repository %s" % (index_uuid, repo_name))
    querylimit = 20
    queryobj = {"@datatype": "GSearchRequest", "@version": "2016-11-09", "advanced": True, "offset": 0,
                "limit": querylimit, "q": "*", "filters": [
            {"@datatype": "GFilter", "@version": "2016-11-09", "type": "match_any",
             "field_name": "https://frdr\\.ca/schema/1\\.0#origin\\.id", "values": [""]}]}

    result_ids = []
    # Write 'structured query' file for use by external search client.
    with open('query.txt', 'w') as outfile:
        json.dump(queryobj, outfile)
    queryobj["filters"][0]["values"][0] = repo_name
    offset = 0
    while True:
        command = [_cmd, "--index " + index_uuid, "structured-query query.txt"]
        ret = os.popen(" ".join(command)).read()
        search_results = json.loads(ret)
        results_count = search_results['count']
        LOGGER.info("Got %i results" % (results_count))
        if results_count == 0:
            break
        for result in search_results['gmeta']:
            result_ids.append(result['subject'])
        offset = offset + querylimit
        queryobj["offset"] = offset
        with open('query.txt', 'w') as outfile:
            json.dump(queryobj, outfile)

    if display_results:
        print('\n'.join(result_ids))

    return result_ids


def delete_items(delete_id_list, index_uuid):
    """
    Delete items from a search index
    :param delete_id_list: List of ids to delete
    :param index_uuid: uuid of index to use
    :return:
    """
    LOGGER.info("Got %i items to delete:" % (len(delete_id_list)))

    for item in delete_id_list:
        LOGGER.info("Deleting item: %s" % (item))
        command = [_cmd, "--host " + _api_host, "--index " + index_uuid, "subject delete " + item]
        ret = os.popen(" ".join(command)).read()


def delete_items_by_curl(delete_id_list, index_uuid, token):
    """
    Delete items from a search index using new API methods
    :param delete_id_list: List of ids to delete
    :param index_uuid: uuid of index to use
    :return:
    """  
    LOGGER.info("Got %i items to delete:" % (len(delete_id_list)))

    for item in delete_id_list:
        LOGGER.info("Deleting item: %s" % (item))
        headers = {'Authorization' : ('Bearer ' + token), 'Content-Type' : 'application/json'}
        r = requests.delete(('https://search.api.globus.org/v1/index/' + index_uuid + '/' + item), headers=headers)
        if r.text != '{ "removed": true }':
            LOGGER.info("Unexpected response when deleting item: %s" % (item))


def main():
    global LOGGER
    syslog_handler = logging.handlers.SysLogHandler()
    stderr_handler = logging.StreamHandler()
    log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[syslog_handler, stderr_handler])
    LOGGER = logging.getLogger('__name__')

    cl_parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    try:
        with open(_tokens_filepath, 'r') as tokens_file:
            tokens = json.loads(tokens_file.read())
        index_config = get_index_config()
        repos_config = get_repos_config()
        cl_parser.add_argument('-r', '--repository', help='Choose repository (list in conf/repos.json)')
        cl_parser.add_argument('-d', '--deleteitems', help='List of search record ids for deletion',
                               action="store_true")
        cl_parser.add_argument('-i', '--index', help='Choose index (list in conf/globus-indexes.conf) Required',
                               required=True)
        args = cl_parser.parse_args()
        index_name = args.index
        if index_name not in index_config["indexes"].keys():
            LOGGER.error("Index not found in configuration list, exiting. Check conf/globus-indexes.conf")
            sys.exit(1)
        index_uuid = index_config["indexes"][index_name].strip()
        if args.deleteitems:
            delete_id_list = query_repository(args.repository, index_uuid)
            delete_items(delete_id_list, index_uuid)
            #delete_items_by_curl(delete_id_list, index_uuid, tokens["access_token"])
        elif args.repository:
            query_repository(args.repository, index_uuid, display_results=True)
        else:
            cl_parser.print_usage()
    except Exception as e:
        LOGGER.error("Parsing error: %s" % (e))
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
