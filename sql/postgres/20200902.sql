CREATE TABLE geobbox (
	geobbox_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	westLon NUMERIC NOT NULL,
	eastLon NUMERIC NOT NULL,
    northLat NUMERIC NOT NULL,
    southLat NUMERIC NOT NULL);
CREATE INDEX geobbox_by_record on geobbox(record_id);
CREATE SEQUENCE IF NOT EXISTS geobbox_id_sequence;
ALTER TABLE geobbox ALTER geobbox_id SET DEFAULT NEXTVAL('geobbox_id_sequence');

CREATE TABLE geopoint (
	geopoint_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	lat NUMERIC NOT NULL,
	lon NUMERIC NOT NULL);
CREATE INDEX geopoint_by_record on geopoint(record_id);
CREATE SEQUENCE IF NOT EXISTS geopoint_id_sequence;
ALTER TABLE geopoint ALTER geopoint_id SET DEFAULT NEXTVAL('geopoint_id_sequence');

CREATE TABLE geoplace (
	geoplace_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	country TEXT,
  province_state TEXT,
  city TEXT,
  other TEXT,
  place_name TEXT);
CREATE INDEX geoplace_by_record on  geoplace(record_id);
CREATE SEQUENCE IF NOT EXISTS geoplace_id_sequence;
ALTER TABLE geoplace ALTER geoplace_id SET DEFAULT NEXTVAL('geoplace_id_sequence');

CREATE TABLE records_x_geoplace (
	records_x_geoplace_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	geoplace_id INTEGER NOT NULL);

CREATE SEQUENCE IF NOT EXISTS records_x_geoplace_seq;
ALTER TABLE records_x_geoplace ALTER records_x_geoplace_id SET DEFAULT NEXTVAL('records_x_geoplace_seq');

CREATE TABLE geofile (
	geofile_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	server_name TEXT NOT NULL,
	format TEXT,
	filename TEXT NOT NULL,
    uri TEXT NOT NULL);

CREATE INDEX  geofile_by_record on   geofile(record_id);
CREATE SEQUENCE IF NOT EXISTS  geofile_id_sequence;
ALTER TABLE geofile ALTER  geofile_id SET DEFAULT NEXTVAL('geofile_id_sequence');
