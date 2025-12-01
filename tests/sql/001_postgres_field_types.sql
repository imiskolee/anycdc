
TRUNCATE TABLE field_types;
INSERT INTO field_types ("field_uuid")  VALUES
                                            ('00000000-0000-0000-0000-000000001'),
                                            ('00000000-0000-0000-0000-000000002');

INSERT INTO field_types ("field_uuid","field_smallint",
                         "field_integer","field_bigint",
                         "field_decimal",
                         "field_char",
                         "field_varchar",
                         "field_text",
                         "field_date",
                         "field_time",
                         "field_timestamp",
                         "field_boolean"
) VALUES (
             '00000000-0000-0000-0000-000000003',
             0,
             0,
             0.01,
             "name",
             "description",
             "content",
             "2025-01-01",
             "00:01",
             1764569614000,
             true
         );
update field_types set "field_smallint" = 1,field_char = 'new_value' where "field_uuid" = '00000000-0000-0000-0000-000000001';

delete field_types where "field_uuid" = '00000000-0000-0000-0000-000000002';