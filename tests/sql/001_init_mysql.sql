CREATE TABLE IF NOT EXISTS field_types (
  field_uuid CHAR(36) PRIMARY KEY,
  field_smallint SMALLINT,
  field_integer INT,
  field_bigint BIGINT,
  field_decimal DECIMAL(8, 3),
  field_char CHAR(10),
  field_varchar VARCHAR(255),
  field_text TEXT,
  field_date DATE,
  field_time TIME(3),
  field_timestamp TIMESTAMP(3),
  field_timestamp_without_tz DATETIME(3),
  field_boolean TINYINT(1),
  field_json JSON,
  field_uuidv4 CHAR(36)
);