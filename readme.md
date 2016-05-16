This is a first version of an OAI record crawler which outputs gmeta.json files for indexing by Globus. It currently only supports Dublin Core records.

Repos to be crawled should be placed in data/repos.csv, in a two column CSV with the structure `OAI base URL, [OAI record set]`, as in this example:

    http://researchdata.sfu.ca/oai2,
    http://dataverse.scholarsportal.info/dvn/OAIHandler,ugrdr
    http://circle.library.ubc.ca/oai/request,com_2429_622
    http://www.polardata.ca/oai/provider,

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `globus_harvester.py`. It requires you to specify a database backend as an argument; currently only `sqlite` is supported. You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively.

If you want to run the crawler via cron, using a persistent database which will record deletions in the target repositories and remove them from the output, use `update_crawl.sh`.

Requires the Python libraries `docopt` and `sickle`. Should work on 2.7+ and 3.x.