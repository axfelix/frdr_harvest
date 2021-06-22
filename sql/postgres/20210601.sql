CREATE TABLE crdc (
    crdc_id INTEGER PRIMARY KEY NOT NULL,
    crdc_code VARCHAR(100) NOT NULL UNIQUE,
    crdc_group_en TEXT NOT NULL,
    crdc_group_fr TEXT NOT NULL,
    crdc_class_en TEXT NOT NULL,
    crdc_class_fr TEXT NOT NULL,
    crdc_field_en TEXT NOT NULL,
    crdc_field_fr TEXT NOT NULL);
CREATE SEQUENCE IF NOT EXISTS crdc_id_sequence;
ALTER TABLE crdc ALTER crdc_id SET DEFAULT NEXTVAL('crdc_id_sequence');

CREATE TABLE records_x_crdc (
    records_x_crdc_id INTEGER PRIMARY KEY NOT NULL,
    record_id INTEGER NOT NULL,
    crdc_id INTEGER NOT NULL);
CREATE SEQUENCE IF NOT EXISTS records_x_crdc_seq;
ALTER TABLE records_x_crdc ALTER records_x_crdc_id SET DEFAULT NEXTVAL('records_x_crdc_seq');
