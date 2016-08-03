This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently only supports Dublin Core records.

Configuration, including which repos are to be crawled, should be placed in data/config.json, in a structure like to this:

```json
{
    "db": {
        "type": "sqlite",
        "filename": "data/globus_oai.db",
        "host": "",
        "schema": "",
        "user": "",
        "pass": ""
    },
    "globus_rest_url": "",
    "logging": {
        "filename": "logs/log.txt",
        "daysperfile": 1,
        "keep": 7
    },
    "update_log_after_numitems": 100,
    "abort_repo_after_numerrors": 5,
    "repos": [
        {
            "name": "SFU Radar",
            "url": "http://researchdata.sfu.ca/oai2",
            "set": "",
            "thumbnail": "",
            "type": "oai",
            "enabled": true,
            "update_log_after_numitems": 50
        },
        {
            "name": "Scholars Portal",
            "url": "http://dataverse.scholarsportal.info/dvn/OAIHandler",
            "set": "ugrdr",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": true
        },
        {
            "name": "UBC Circle",
            "url": "http://circle.library.ubc.ca/oai/request",
            "set": "com_2429_287",
            "thumbnail": "https://circle-23jan2015.sites.olt.ubc.ca/files/2015/01/circle-logo-inverted.png",
            "type": "oai",
            "enabled": true
        },
        {
            "name": "Open Data Canada",
            "url": "http://open.canada.ca/data",
            "set": "",
            "thumbnail": "",
            "type": "ckan",
            "enabled": false
        },
        {
            "name": "Canadian Polar Data Network",
            "url": "http://www.polardata.ca/oai/provider",
            "set": "",
            "thumbnail": "https://polardata.ca/images/ccin-hori.gif",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "NRDR",
            "url": "https://rdmtest1.computecanada.ca/oai/request",
            "set": "",
            "thumbnail": "",
            "type": "oai",
            "enabled": false
        }
    ]
}
```

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `globus_harvester.py`. It requires you to specify a database backend as an argument; currently only `sqlite` is supported. You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively.

If you want to run the crawler via cron, using a persistent database which will record deletions in the target repositories and remove them from the output, use `update_crawl.sh`.

Requires the Python libraries `docopt`, `sickle` and `ckanapi`. Should work on 2.7+ and 3.x.
