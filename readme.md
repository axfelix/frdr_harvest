This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently only supports OAI-DC and CKAN repositories.

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
    "export_filepath": "data",
    "export_format": "gmeta",
    "logging": {
        "filename": "logs/log.txt",
        "daysperfile": 1,
        "keep": 7,
        "level": "DEBUG"
    },
    "update_log_after_numitems": 10,
    "abort_after_numerrors": 5,
    "repo_refresh_days": 1,
    "record_refresh_days": 30,
    "max_records_updated_per_run": 40,
    "repos": [
        {
            "name": "SFU Radar",
            "url": "http://researchdata.sfu.ca/oai2",
            "set": "",
            "thumbnail": "http://static.lib.sfu.ca/clf2013/sfu-logo.png",
            "type": "oai",
            "update_log_after_numitems": 50,
            "item_url_pattern": "http://researchdata.sfu.ca/islandora/object/%id%",
            "enabled": false
        },
        {
            "name": "Scholars Portal",
            "url": "http://dataverse.scholarsportal.info/dvn/OAIHandler",
            "set": "ugrdr",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "UBC Circle",
            "url": "http://circle.library.ubc.ca/oai/request",
            "set": "com_2429_287",
            "thumbnail": "https://circle-23jan2015.sites.olt.ubc.ca/files/2015/01/circle-logo-inverted.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Open Data Canada",
            "url": "http://open.canada.ca/data",
            "set": "",
            "thumbnail": "http://open.canada.ca/data/static/img/leaves/fivestar.png",
            "type": "ckan",
            "repo_refresh_days": 7,
            "update_log_after_numitems": 2000,
            "enabled": false
        },
        {
            "name": "Polar Data Network",
            "url": "http://www.polardata.ca/oai/provider",
            "set": "",
            "thumbnail": "https://polardata.ca/images/ccin-hori.gif",
            "type": "oai",
            "metadataprefix": "fgdc",
            "enabled": false
        },
        {
            "name": "FRDR",
            "url": "https://rdmtest1.computecanada.ca/oai/request",
            "set": "",
            "thumbnail": "https://rdmtest1.computecanada.ca/jspui/image/logo.png",
            "type": "oai",
            "metadataprefix": "frdr",
            "enabled": false
        },
        {
            "name": "U of A Dataverse",
            "url": "https://dataverse.library.ualberta.ca/dvn/OAIHandler",
            "set": "",
            "thumbnail": "https://dataverse.library.ualberta.ca/dvn/resources/images/ua-lib-logo.png",
            "type": "oai",
            "metadataprefix": "ddi",
            "enabled": false
        },
        {
            "name": "Concordia Spectrum Research Repository",
            "url": "http://spectrum.library.concordia.ca/cgi/oai2",
            "set": "74797065733D64617461736574",
            "thumbnail": "http://spectrum.library.concordia.ca/images/custom-logo.jpg",
            "type": "oai",
            "enabled": false
        }
    ]
}
```

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `globus_harvester.py`. It requires you to specify a database backend as an argument; currently only `sqlite` is supported. You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively.

If you want to run the crawler via cron, using a persistent database which will record deletions in the target repositories and remove them from the output, use `update_crawl.sh`.

Requires the Python libraries `docopt`, `sickle` and `ckanapi`. Should work on 2.7+ and 3.x.
