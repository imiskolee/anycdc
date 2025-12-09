DROP table IF EXISTS field_types;
CREATE TABLE IF NOT EXISTS field_types (
  field_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  field_smallint SMALLINT,                        
  field_integer INTEGER,                          
  field_bigint BIGINT,                            
  field_decimal DECIMAL(8, 3),                    
  field_char CHAR(10),                            
  field_varchar VARCHAR(255),                      
  field_text TEXT,                                
  field_date DATE,                                
  field_time TIME(3),                             
  field_timestamp TIMESTAMP(3) WITH TIME ZONE,    
  field_timestamp_without_tz TIMESTAMP(3) WITHOUT TIME ZONE,   
  field_boolean BOOLEAN,                          
  field_json JSON,                                
  field_uuidv4 UUID                              
);
