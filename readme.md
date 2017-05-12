This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently only supports OAI and CKAN repositories.

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
    "globus_rest_url": "",
    "export_filepath": "data",
    "export_format": "gmeta",
    "logging": {
        "filename": "logs/log.txt",
        "maxbytes": 10485760,
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
            "name": "Algoma University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "algoma_university_dataverse",
            "thumbnail": "https://www.algomau.ca/wp-content/themes/algoma/favicon.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Brock University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "brock_university_dataverse",
            "thumbnail": "https://discover.brocku.ca/wp-content/themes/brocku/images/brocku-rectangle-logo-280.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Carleton University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "Carleton-University-Set",
            "thumbnail": "https://cmsframework.s3.amazonaws.com/theme-carleton-cms/assets/images/favicons/apple-icon-precomposed.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Lakehead University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "lakehead_university_dataverse",
            "thumbnail": "http://cou.on.ca/wp-content/uploads/2015/04/lakehead-icon.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Laurentian University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "laurentian_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "McMaster University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "mcmaster_university_dataverse",
            "thumbnail": "http://future.mcmaster.ca/wp-content/uploads/FS_maroon3_logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Nipissing University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "nipissing_university_dataverse",
            "thumbnail": "http://images.scholarsportal.info/dataverse/logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "OCAD Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "ocad_university_dataverse",
            "thumbnail": "https://upload.wikimedia.org/wikipedia/commons/0/02/OCAD_University_Logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "ODESI Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "ODESI",
            "thumbnail": "http://s3.amazonaws.com/libapps/accounts/37287/images/odesi_logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Queen's University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "queens_university_dataverse",
            "thumbnail": "http://www.queensu.ca/mc_administrator/sites/default/files/assets/pages/QueensLogo_red.jpg",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Ryerson University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "ryerson_university_dataverse",
            "thumbnail": "http://www.ryerson.ca/brand/index/jcr:content/center/uiwcolumns_0/conContainer_2_0/uiwimage.img.png/1448296969404.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Trent University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "trent_university_dataverse",
            "thumbnail": "http://media.zuza.com/a/7/a70985c3-688f-4e32-8a18-5f87b6b877c3/trent___Content.jpg",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "University of Guelph Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "university_of_guelph_dataverse",
            "thumbnail": "http://www.uoguelph.ca/polisci/sites/default/files/images/University-of-Guelph.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "University of Ottawa Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "university_of_ottawa_dataverse",
            "thumbnail": "https://www.uottawa.ca/marque/sites/www.uottawa.ca.marque/files/updated_logo_0.jpg",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "UOIT Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "uoit_dataverse",
            "thumbnail": "https://shared.uoit.ca/site-images/UOITlogo-SocialMediaShare.jpg",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "University of Toronto Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "u_toronto_dataverse",
            "thumbnail": "http://pie.med.utoronto.ca/LapChole/content/assets/images/UofTlogo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Waterloo University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "waterloo_dataverse",
            "thumbnail": "http://chemeng.uwaterloo.ca/mtam/Funding/image2328.jpg",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Western University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "western_university_dataverse",
            "thumbnail": "http://communications.uwo.ca/comms/img/logo_teasers/Stacked.gif",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "Wilfrid Laurier University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "wilfrid_laurier_dataverse",
            "thumbnail": "https://students.wlu.ca/common/images/site/logos/Laurier_PURPLE_rgb_LRG.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "University of Windsor Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "windsor_dataverse",
            "thumbnail": "http://www1.uwindsor.ca/tifs/system/files/Uwindsor%20logo.png",
            "type": "oai",
            "enabled": false
        },
        {
            "name": "York University Dataverse",
            "url": "http://dataverse.scholarsportal.info/oai",
            "set": "york_university_dataverse",
            "thumbnail": "https://digital.library.yorku.ca/YorkULogo_Hor_rgb-bootstrap_transparent.png",
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
            "url": "https://beta.frdr.ca/oai/request",
            "set": "",
            "thumbnail": "https://beta.frdr.ca/repo/image/sitelogo_en.png",
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
