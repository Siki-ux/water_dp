INSERT INTO ingest_type (name)
VALUES ('sftp'::varchar(200)),
       ('mqtt'::varchar(200)),
       ('extsftp'::varchar(200)),
       ('extapi'::varchar(200));

INSERT INTO file_parser_type (name)
VALUES ('csvparser'::varchar(200));

INSERT INTO mqtt_device_type (name)
VALUES ('campbell_cr6'::varchar(200)),
       ('brightsky_dwd_api'::varchar(200)),
       ('ydoc_ml417'::varchar(200)),
       ('sine_dummy'::varchar(200));

INSERT INTO external_api_type (name)
VALUES ('ttn'::varchar(200)),
       ('dwd'::varchar(200)),
       ('nm'::varchar(200)),
       ('bosch'::varchar(200));
