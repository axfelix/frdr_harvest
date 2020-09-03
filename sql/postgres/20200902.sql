CREATE TABLE geospatial_boundingbox (
	geospatial_boundingbox_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	westLon NUMERIC NOT NULL,
	eastLon NUMERIC NOT NULL,
    northLat NUMERIC NOT NULL,
    southLat NUMERIC NOT NULL);
CREATE INDEX geospatial_boundingbox_by_record on geospatial_boundingbox(record_id);
CREATE SEQUENCE IF NOT EXISTS geospatial_boundingbox_id_sequence;
ALTER TABLE geospatial_boundingbox ALTER geospatial_boundingbox_id SET DEFAULT NEXTVAL('geospatial_boundingbox_id_sequence');


CREATE TABLE geospatial_point (
	geospatial_point_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	lat NUMERIC NOT NULL,
	lon NUMERIC NOT NULL);
CREATE INDEX geospatial_point_by_record on geospatial_point(record_id);
CREATE SEQUENCE IF NOT EXISTS geospatial_point_id_sequence;
ALTER TABLE geospatial_point ALTER geospatial_point_id SET DEFAULT NEXTVAL('geospatial_point_id_sequence');

CREATE TABLE geospatial_location (
	geospatial_location_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	country TEXT,
    province_state TEXT,
    city TEXT,
    other TEXT,
    place_name TEXT);
CREATE INDEX geospatial_location_by_record on  geospatial_location(record_id);
CREATE SEQUENCE IF NOT EXISTS geospatial_location_id_sequence;
ALTER TABLE geospatial_location ALTER geospatial_location_id SET DEFAULT NEXTVAL('geospatial_location_id_sequence');

CREATE TABLE geospatial_file (
	geospatial_file_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	server_name TEXT NOT NULL,
	format TEXT,
	filename TEXT NOT NULL,
    uri TEXT NOT NULL);
CREATE INDEX  geospatial_file_by_record on   geospatial_file(record_id);
CREATE SEQUENCE IF NOT EXISTS  geospatial_file_id_sequence;
ALTER TABLE geospatial_file ALTER  geospatial_file_id SET DEFAULT NEXTVAL( geospatial_file_id_sequence');
