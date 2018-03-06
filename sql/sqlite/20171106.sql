create table if not exists creators (
	creator_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	creator TEXT, 
	is_contributor INTEGER
	);

create table if not exists descriptions (
	description_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	description TEXT, 
	language TEXT);

create table if not exists domain_metadata (
	metadata_id INTEGER PRIMARY KEY NOT NULL,
	schema_id INTEGER NOT NULL, 
	record_id INTEGER NOT NULL, 
	field_name TEXT, 
	field_value TEXT);

create table if not exists domain_schemas (
	schema_id INTEGER PRIMARY KEY NOT NULL, 
	namespace TEXT);

create table if not exists geospatial (
	geospatial_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	coordinate_type TEXT, 
	lat NUMERIC, 
	lon NUMERIC);

create table if not exists records (
	record_id INTEGER PRIMARY KEY NOT NULL,
	repository_id INTEGER NOT NULL,
	title TEXT,pub_date TEXT,
	modified_timestamp INTEGER DEFAULT 0,
	source_url TEXT,
	deleted NUMERIC DEFAULT 0,
	local_identifier TEXT,
	series TEXT,
	contact TEXT);

create table if not exists repositories (
	repository_id INTEGER PRIMARY KEY NOT NULL,
	repository_set TEXT NOT NULL DEFAULT '',
	repository_url TEXT,
	repository_name TEXT,
	repository_thumbnail TEXT,
	repository_type TEXT,
	last_crawl_timestamp INTEGER,
	item_url_pattern TEXT,
	abort_after_numerrors INTEGER,
	max_records_updated_per_run INTEGER,
	update_log_after_numitems INTEGER,
	record_refresh_days INTEGER,
	repo_refresh_days INTEGER,
	enabled TEXT);

create table if not exists publishers (
	publisher_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	publisher TEXT);

create table if not exists rights (
	rights_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	rights TEXT);

create table if not exists subjects (
	subject_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	subject TEXT);

create table if not exists tags (
	tag_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	tag TEXT, 
	language TEXT);

create table if not exists settings (
	setting_id INTEGER PRIMARY KEY NOT NULL, 
	setting_name TEXT, 
	setting_value TEXT);

create table if not exists access (
	access_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	access TEXT);

create index IF NOT EXISTS creators_by_record on creators(record_id);
create index IF NOT EXISTS descriptions_by_record on descriptions(record_id,language);
create index IF NOT EXISTS tags_by_record on tags(record_id,language);
create index IF NOT EXISTS subjects_by_record on subjects(record_id);
create index IF NOT EXISTS publishers_by_record on publishers(record_id);
create index IF NOT EXISTS rights_by_record on rights(record_id);
create index IF NOT EXISTS geospatial_by_record on geospatial(record_id);
create index IF NOT EXISTS access_by_record on access(record_id);
create index IF NOT EXISTS domain_metadata_by_record on domain_metadata(record_id,schema_id);
create index IF NOT EXISTS domain_schemas_by_schema_id on domain_schemas(schema_id);
create unique index IF NOT EXISTS records_by_repository on records (repository_id, local_identifier);