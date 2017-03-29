This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently only supports OAI and CKAN repositories.

Configuration, including which repos are to be crawled, should be placed in data/config.json, in a structure like to this:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ json
{
    "db": {
        "type": "sqlite",
        "dbname": "data/globus_oai.db"
,        "host": "",
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
            "name": "Scholars Portal Algoma University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "algoma_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Brock University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "brock_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Carleton University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "Carleton-University-Set",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Lakehead University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "lakehead_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Laurentian University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "laurentian_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal McMaster University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "mcmaster_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Nipissing University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "nipissing_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal OCAD Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "ocad_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal ODESI Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "ODESI",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Queen's University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "queens_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Ryerson University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "ryerson_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Trent University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "trent_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal University of Guelph Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "university_of_guelph_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal University of Ottawa Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "university_of_ottawa_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal UOIT Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "uoit_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal University of Toronto Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "u_toronto_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Waterloo University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "waterloo_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Western University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "western_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal Wilfrid Laurier University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "wilfrid_laurier_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal University of Windsor Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "windsor_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Scholars Portal York University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "york_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "UBC Circle",
            "url": "http://circle.library.ubc.ca/oai/request",
            "set": "",
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
            "url": "https://frdr-alpha.computecanada.ca/oai/request",
            "set": "",
            "thumbnail": "https://frdr-alpha.computecanada.ca/jspui/image/logo.png",
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
            "name": "Abacus Dataverse Network",
            "url": "http://dvn.library.ubc.ca/dvn/OAIHandler",
            "set": "abacus_open",
            "thumbnail": "http://dvn.library.ubc.ca/dvn/resources/images/DVN/ABACUS/AbacusLogoDVN.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "York University Digital Library",
            "url": "https://digital.library.yorku.ca/oai2",
            "set": "yul_232039",
            "thumbnail": "https://digital.library.yorku.ca/YorkULogo_Hor_rgb-bootstrap_transparent.png",
            "type": "oai",
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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Right now, supported OAI metadata types are Dublin Core ("OAI-DC" is assumed by default and does not need to be specified), DDI, and FGDC. If you want to harvest metadata from another OAI schema, you can create a file at `metadata/schemaname` containing one metadata value name per line.

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `globus_harvester.py`. You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively. You can also use `--only-new-records` to only export records that have changed since the last run. Support database types are "sqlite" and "postgres"; the `psycopg2` library is required for postgres support.

Requires the Python libraries `docopt`, `sickle` and `ckanapi`. Should work on 2.7+ and 3.x.
