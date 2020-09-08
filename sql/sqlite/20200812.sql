create table if not exists reconciliations (
	reconciliation_id INTEGER PRIMARY KEY NOT NULL,
	tag_id INTEGER NOT NULL, 
	reconciliation TEXT, 
	language TEXT);

create unique index IF NOT EXISTS reconciliations_by_tag on reconciliations (tag_id, reconciliation);