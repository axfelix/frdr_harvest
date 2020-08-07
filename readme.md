This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently supports OAI, CKAN, MarkLogic, Socrata, CSW, DataStream, and OpenDataSoft repositories.

Configuration is split into two files. The first controls the operation of the indexer, and is located in conf/harvester.conf.

The list of repositories to be crawled is in conf/repos.json, in a structure like to this:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ json
{
    "repos": [
        {
            "name": "Some OAI Repository",
            "url": "http://someoairepository.edu/oai2",
            "homepage_url": "http://someoairepository.edu",
            "set": "",
            "thumbnail": "http://someoairepository.edu/logo.png",
            "type": "oai",
            "update_log_after_numitems": 50,
            "enabled": true
        },
        {
            "name": "Some CKAN Repository",
            "url": "http://someckanrepository.edu/data",
            "homepage_url": "http://someckanrepository.edu",
            "set": "",
            "thumbnail": "http://someckanrepository.edu/logo.png",
            "type": "ckan",
            "repo_refresh_days": 7,
            "update_log_after_numitems": 2000,
            "item_url_pattern": "https://someckanrepository.edu/dataset/%id%",
            "enabled": true
        },
        {
            "name": "Some MarkLogic Repository",
            "url": "https://somemarklogicrepository.ca/search",
            "homepage_url": "https://search2.somemarklogicrepository.ca/",
            "item_url_pattern": "https://search2.somemarklogicrepository.ca/#/details?uri=%2Fodesi%2F%id%",
            "thumbnail": "http://somemarklogicrepository.ca/logo.png",
            "collection": "cora",
            "options": "odesi-opts2",
            "type": "marklogic",
            "enabled": true
        },
        {
            "name": "Some Socrata Repository",
            "url": "data.somesocratarepository.ca",
            "homepage_url": "https://somesocratarepository.ca",
            "set": "",
            "thumbnail": "http://somesocratarepository.ca/logo.png",
            "item_url_pattern": "https://data.somesocratarepository.ca/d/%id%",
            "type": "socrata",
            "enabled": true
        },
        {
            "name": "Some CSW Repository",
            "url": "https://somecswrepository.edu/geonetwork/srv/eng/csw",
            "homepage_url": "https://somecswrepository.edu",
            "item_url_pattern": "https://somecswrepository.edu/geonetwork/srv/eng/catalog.search#/metadata/%id%",
            "type": "csw",
            "enabled": true
        },
        {
            "name": "Some DataStream Repository",
            "url": "https://somedatastreamrepository.org/dataset/sitemap.xml",
            "homepage_url": "https://somedatastreamrepository.org/",
            "item_url_pattern": "https://somedatastreamrepository.org/dataset/%id%",
            "type": "datastream",
            "enabled": true
        },
        {
            "name": "Some OpenDataSoft Repository",
            "url": "https://someopendatasoftrepository.ca/api/datasets/1.0/search",
            "homepage_url": "someopendatasoftrepository.ca",
            "type": "opendatasoft",
            "item_url_pattern": "https://someopendatasoftrepository.ca/explore/dataset/%id%",
            "enabled": true
        }
    ]
}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Right now, supported OAI metadata types are Dublin Core ("OAI-DC" is assumed by default and does not need to be specified), DDI, and FGDC.

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `globus_harvester.py`. You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively. You can also use `--only-new-records` to only export records that have changed since the last run. Supported database types are "sqlite" and "postgres"; the `psycopg2` library is required for postgres support. Supported export formats in the config file are `gmeta` and `xml`; XML export requires the `dicttoxml` library.

Requires the Python libraries `docopt`, `sickle`, `requests`, `owslib`, `sodapy`, and `ckanapi`. Should work on 2.7+ and 3.x.
