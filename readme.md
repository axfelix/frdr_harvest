This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently supports OAI, CKAN, and MarkLogic repositories.

Configuration is split into two files. The first controls the operation of the indexer, and is located in conf/harvester.conf.

The list of repositories to be crawled is in conf/repos.json, in a structure like to this:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ json
{
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
        },
        {
            "name": "Some MarkLogic Repository",
            "url": "https://search2.odesi.ca/search?requestURL=((*))%2520AND%2520(coll:cora)%26options%3Dodesi-opts2%26format%3Djson%26start%3D0%26pageLength%3D10",
            "contact": "contact@person.ca",
            "type": "marklogic",
            "enabled": true
        }
    ]
}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Right now, supported OAI metadata types are Dublin Core ("OAI-DC" is assumed by default and does not need to be specified), DDI, and FGDC.

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `globus_harvester.py`. You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively. You can also use `--only-new-records` to only export records that have changed since the last run. Support database types are "sqlite" and "postgres"; the `psycopg2` library is required for postgres support.

Requires the Python libraries `docopt`, `sickle`, `requests` and `ckanapi`. Should work on 2.7+ and 3.x.
