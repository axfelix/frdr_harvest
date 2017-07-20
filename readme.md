This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently supports OAI and CKAN repositories.

Configuration, including which repos are to be crawled, should be placed in data/config.json, in a structure like to this:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ json
{
    "db": {
        "type": "sqlite",
        "dbname": "data/globus_oai.db",
        "host": "",
        "schema": "",
        "user": "",
        "pass": ""
    },
    "export_filepath": "data",
    "export_format": "gmeta",
    "logging": {
        "filename": "logs/log.txt",
        "maxbytes": 10485760,
        "keep": 7,
        "level": "DEBUG"
    },
    "update_log_after_numitems": 1000,
    "abort_after_numerrors": 5,
    "repo_refresh_days": 7,
    "record_refresh_days": 30,
    "max_records_updated_per_run": 2000,
    "prune_non_dataset_items": false,
    "repos": [
        {
            "name": "Some OAI Repository",
            "url": "http://someoairepository.edu/oai2",
            "set": "",
            "thumbnail": "http://someoairepository.edu/logo.png",
            "type": "oai",
            "update_log_after_numitems": 50,
            "enabled": false
        },
        {
            "name": "Some CKAN Repository",
            "url": "http://someckanrepository.edu/data",
            "set": "",
            "thumbnail": "http://someckanrepository.edu/logo.png",
            "type": "ckan",
            "repo_refresh_days": 7,
            "update_log_after_numitems": 2000,
            "enabled": false
        }
    ]
}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Right now, supported OAI metadata types are Dublin Core ("OAI-DC" is assumed by default and does not need to be specified), DDI, and FGDC.

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `globus_harvester.py`. You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively. You can also use `--only-new-records` to only export records that have changed since the last run. Support database types are "sqlite" and "postgres"; the `psycopg2` library is required for postgres support.

Requires the Python libraries `docopt`, `sickle` and `ckanapi`. Should work on 2.7+ and 3.x.
