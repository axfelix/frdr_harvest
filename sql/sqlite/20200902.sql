CREATE TABLE geobbox(
	geobbox_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	westLon NUMERIC NOT NULL,
	eastLon NUMERIC NOT NULL,
    northLat NUMERIC NOT NULL,
    southLat NUMERIC NOT NULL);
CREATE INDEX geobbox_by_record on geobbox(record_id);

CREATE TABLE geopoint (
	geopoint_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	lat NUMERIC NOT NULL,
	lon NUMERIC NOT NULL);
CREATE INDEX geopoint_by_record on geopoint(record_id);

CREATE TABLE geoplace (
	geoplace_id INTEGER PRIMARY KEY NOT NULL,
	country TEXT,
    province_state TEXT,
    city TEXT,
    other TEXT,
    place_name TEXT);

CREATE TABLE records_x_geoplace (
	records_x_geoplace_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	geoplace_id INTEGER NOT NULL);
CREATE INDEX records_x_geoplace_by_record on records_x_geoplace(record_id);
CREATE INDEX records_x_geoplace_by_geoplace on records_x_geoplace(geoplace_id);

CREATE TABLE geofile (
	geofile_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	server_name TEXT NOT NULL,
	format TEXT,
	filename TEXT NOT NULL,
    uri TEXT NOT NULL);
CREATE INDEX  geofileby_record on   geofile(record_id);
