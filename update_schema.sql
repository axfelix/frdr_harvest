create table repositories_temp (repository_id INTEGER PRIMARY KEY NOT NULL,repository_url TEXT NOT NULL, repository_set TEXT NOT NULL DEFAULT '', repository_name TEXT, repository_thumbnail TEXT,
  repository_type TEXT,last_crawl_timestamp INTEGER,item_url_pattern TEXT,abort_after_numerrors INTEGER,max_records_updated_per_run INTEGER,update_log_after_numitems INTEGER,
  record_refresh_days INTEGER, repo_refresh_days INTEGER, enabled INTEGER);
begin transaction;
insert into repositories_temp (repository_url,repository_name,repository_thumbnail,repository_type,last_crawl_timestamp,item_url_pattern,enabled)
select repository_url,repository_name,repository_thumbnail,repository_type,last_crawl_timestamp,item_url_pattern,1 from repositories;
commit;

create table records_temp (record_id INTEGER PRIMARY KEY NOT NULL,repository_id INTEGER NOT NULL,title TEXT,pub_date TEXT,modified_timestamp INTEGER DEFAULT 0,source_url TEXT,deleted INTEGER DEFAULT 0,local_identifier TEXT,series TEXT,contact TEXT);
begin transaction;
insert into records_temp (repository_id,title,pub_date,modified_timestamp,source_url,deleted,local_identifier,series,contact)
select rep1.repository_id,rec1.title,rec1.date,rec1.modified_timestamp,rec1.source_url,rec1.deleted,rec1.local_identifier,rec1.series,rec1.contact 
from records rec1
inner join repositories_temp rep1 on rep1.repository_url = rec1.repository_url;
commit;

create table creators_temp (creator_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, creator TEXT, is_contributor INTEGER);
begin transaction;
insert into creators_temp (record_id,creator,is_contributor)
select rec1.record_id, cre1.creator,cre1.is_contributor
from records_temp rec1
inner join creators cre1 on cre1.local_identifier = rec1.local_identifier;
commit;

create table descriptions_temp (description_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, description TEXT, language TEXT);
begin transaction;
insert into descriptions_temp (record_id,description,language)
select rec1.record_id, dis1.description, 'en'
from records_temp rec1
inner join descriptions dis1 on dis1.local_identifier = rec1.local_identifier;
commit;

begin transaction;
insert into descriptions_temp (record_id,description,language)
select rec1.record_id, dis1.description, 'fr'
from records_temp rec1
inner join fra_descriptions dis1 on dis1.local_identifier = rec1.local_identifier;
commit;

create table tags_temp (tag_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, tag TEXT, language TEXT);
begin transaction;
insert into tags_temp (record_id,tag,language)
select rec1.record_id, tag1.tag, tag1.language
from records_temp rec1
inner join tags tag1 on tag1.local_identifier = rec1.local_identifier;
commit;

create table subjects_temp (subject_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, subject TEXT);
begin transaction;
insert into subjects_temp (record_id,subject)
select rec1.record_id, sub1.subject
from records_temp rec1
inner join subjects sub1 on sub1.local_identifier = rec1.local_identifier;
commit;

create table rights_temp (rights_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, rights TEXT);
begin transaction;
insert into rights_temp (record_id,rights)
select rec1.record_id, rig1.rights
from records_temp rec1
inner join rights rig1 on rig1.local_identifier = rec1.local_identifier;
commit;

create table geospatial_temp (geospatial_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, coordinate_type TEXT, lat NUMERIC, lon NUMERIC);
begin transaction;
insert into geospatial_temp (record_id,coordinate_type,lat,lon)
select rec1.record_id, geo1.coordinate_type, geo1.lat, geo1.lon
from records_temp rec1
inner join geospatial geo1 on geo1.local_identifier = rec1.local_identifier;
commit;

create table domain_metadata_temp (metadata_id INTEGER PRIMARY KEY NOT NULL,record_id INTEGER NOT NULL, field_name TEXT, field_value TEXT);
begin transaction;
insert into domain_metadata_temp (record_id,field_name, field_value)
select rec1.record_id, met1.field_name, met1.field_value
from records_temp rec1
inner join domain_metadata met1 on met1.local_identifier = rec1.local_identifier;
commit;

begin transaction;
drop table repositories;
alter table repositories_temp rename to repositories;
commit;

begin transaction;
drop index identifier_plus_creator;
drop table creators;
alter table creators_temp rename to creators;
create index creators_by_record on creators(record_id);
commit;

begin transaction;
drop index identifier_plus_description;
drop table descriptions;
drop table fra_descriptions;
alter table descriptions_temp rename to descriptions;
create index descriptions_by_record on descriptions(record_id,language);
commit;

begin transaction;
drop index identifier_plus_tag;
drop table tags;
alter table tags_temp rename to tags;
create index tags_by_record on tags(record_id,language);
commit;

begin transaction;
drop index identifier_plus_subject;
drop table subjects;
alter table subjects_temp rename to subjects;
create index subjects_by_record on subjects(record_id);
commit;

begin transaction;
drop index identifier_plus_rights;
drop table rights;
alter table rights_temp rename to rights;
create index rights_by_record on rights(record_id);
commit;

begin transaction;
drop index identifier_plus_lat_lon;
drop table geospatial;
alter table geospatial_temp rename to geospatial;
create index geospatial_by_record on geospatial(record_id);
commit;

begin transaction;
drop index domain_metadata_value;
drop table domain_metadata;
alter table domain_metadata_temp rename to domain_metadata;
create index domain_metadata_by_record on domain_metadata(record_id);
commit;

begin transaction;
drop table records;
alter table records_temp rename to records;
create unique index records_by_repository on records (repository_id, local_identifier);
commit;

vacuum;