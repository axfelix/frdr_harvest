create table if not exists reconciliations (
	reconciliation_id INTEGER PRIMARY KEY NOT NULL,
	tag_id INTEGER NOT NULL, 
	reconciliation TEXT, 
	language TEXT);

CREATE SEQUENCE IF NOT EXISTS reconciliations_id_sequence;
ALTER TABLE reconciliations ALTER reconciliation_id SET DEFAULT NEXTVAL('reconciliations_id_sequence');

create unique index IF NOT EXISTS reconciliations_by_tag on reconciliations (tag_id, reconciliation);