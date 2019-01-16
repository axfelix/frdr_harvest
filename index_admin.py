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


def query_repository(repo_name, index_uuid, token, display_results=False):
    """
    Display the ids  ('subjects') of all items indexed in a repository.
    :param repo_name: Textual name of repository to query, corresponds to 'name' field in conf file.
    :param index_name: Name of index, mapped by us to a UUID.
    :param display_results: Print ids to standard output
    :return: List of result ids
    """

    LOGGER.info("Querying index %s for repository %s" % (index_uuid, repo_name))
    querylimit = 20
    headers = {'Authorization' : ('Bearer ' + token), 'Content-Type' : 'application/json'}
    queryobj = {"@datatype": "GSearchRequest", "@version": "2016-11-09", "advanced": True, "offset": 0,
                "limit": querylimit, "q": "*", "filters": [
            {"@datatype": "GFilter", "@version": "2016-11-09", "type": "match_any",
             "field_name": "https://frdr\\.ca/schema/1\\.0#origin\\.id", "values": [""]}]}

    result_ids = []
    queryobj["filters"][0]["values"][0] = repo_name
    offset = 0
    while True:
        r = requests.post('https://' + _api_host + '/v1/index/' + index_uuid + '/search', headers=headers, json=queryobj)
        search_results = json.loads(r.text)
        results_count = search_results['count']
        LOGGER.info("Got %i results" % (results_count))
        if results_count == 0:
            break
        for result in search_results['gmeta']:
            result_ids.append(result['subject'])
        offset = offset + querylimit
        queryobj["offset"] = offset

    if display_results:
        print('\n'.join(result_ids))

    return result_ids

def delete_items_by_curl(delete_id_list, index_uuid, token):
    """
    Delete items from a search index using new API methods
    :param delete_id_list: List of ids to delete
    :param index_uuid: uuid of index to use
    :return:
    """
    LOGGER.info("Got %i items to delete:" % (len(delete_id_list)))
    headers = {'Authorization' : ('Bearer ' + token), 'Content-Type' : 'application/json'}
    queryobj = {"@datatype": "GSearchRequest", "@version": "2016-11-09", "advanced": True, "limit": 1, "q": "*", "filters": [
            {"@datatype": "GFilter", "@version": "2016-11-09", "type": "match_all",
             "field_name": "http://dublincore\\.org/documents/dcmi-terms#source", "values": [""]}]}

    for item in delete_id_list:
        LOGGER.info("Deleting item: %s" % (item))
        queryobj["filters"][0]["values"][0] = item
        r = requests.post('https://' + _api_host + '/v1/index/' + index_uuid + '/delete_by_query', headers=headers, json=queryobj)
        results = json.loads(r.text)
        if "num_subjects_deleted" in results:
            LOGGER.info("Deleted {} item(s)".format(results.get('num_subjects_deleted')))
        else:
            LOGGER.info("Error deleting item: {}\n{}".format(item, r.text))


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
        cl_parser.add_argument('-r', '--repository', help='Repository name (list in conf/repos.json)')
        cl_parser.add_argument('-p', '--purgefile', help='File name with list of items to delete. Not used with deleteall.')
        cl_parser.add_argument('-d', '--deleteall', help='Add this argument to delete all items from a repo. Not used with purgefile.',
                               action="store_true")
        cl_parser.add_argument('-i', '--index', help='Globus index name (list in conf/globus-indexes.conf) Required',
                               required=True)
        args = cl_parser.parse_args()
        index_name = args.index
        if index_name not in index_config["indexes"].keys():
            LOGGER.error("Index name not found in configuration list, exiting. Check conf/globus-indexes.conf")
            sys.exit(1)
        index_uuid = index_config["indexes"][index_name].strip()
        if args.purgefile:
            with open(args.purgefile) as f:
                delete_id_list = f.readlines()
                delete_id_list = [x.strip() for x in delete_id_list]
                delete_items_by_curl(delete_id_list, index_uuid, tokens["access_token"])
        elif args.deleteall:
            delete_id_list = query_repository(args.repository, index_uuid, tokens["access_token"])
            delete_items_by_curl(delete_id_list, index_uuid, tokens["access_token"])
        elif args.repository:
            query_repository(args.repository, index_uuid, tokens["access_token"], display_results=True)
        else:
            cl_parser.print_usage()
    except Exception as e:
        LOGGER.error("Parsing error: %s" % (e))
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
