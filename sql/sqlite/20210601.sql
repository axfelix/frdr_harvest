CREATE TABLE crdc (
    crdc_id INTEGER PRIMARY KEY NOT NULL,
    crdc_code TEXT NOT NULL UNIQUE,
    crdc_group_en TEXT NOT NULL,
    crdc_group_fr TEXT NOT NULL,
    crdc_class_en TEXT NOT NULL,
    crdc_class_fr TEXT NOT NULL,
    crdc_field_en TEXT NOT NULL,
    crdc_field_fr TEXT NOT NULL);

CREATE TABLE records_x_crdc (
    records_x_crdc_id INTEGER PRIMARY KEY NOT NULL,
    record_id INTEGER NOT NULL,
    crdc_id INTEGER NOT NULL);

CREATE INDEX records_x_crdc_by_record on records_x_crdc(record_id);
CREATE INDEX records_x_crdc_by_crdc on records_x_crdc(crdc_id);
