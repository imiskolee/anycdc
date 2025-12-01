CREATE TABLE IF NOT EXISTS field_types (
                                           field_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                           field_smallint SMALLINT,                      -- 2字节整数 (-32768 至 32767)
                                           field_integer INTEGER,                        -- 4字节整数 (-2147483648 至 2147483647)
                                           field_bigint BIGINT,                          -- 8字节整数 (-9223372036854775808 至 9223372036854775807)
                                           field_decimal DECIMAL(8, 3),                  -- 与 numeric 同义，精确小数
                                           field_char CHAR(10),                          -- 固定长度字符串（不足补空格）
                                           field_varchar VARCHAR(255),                    -- 可变长度字符串（最大50字符）
                                           field_text TEXT,                              -- 无长度限制文本
                                           field_date DATE,                              -- 日期（格式：YYYY-MM-DD）
                                           field_time TIME(3),                           -- 时间（含3位毫秒，格式：HH:MI:SS.ms）
                                           field_timestamp TIMESTAMP(3) WITH TIME ZONE,  -- 带时区时间戳（含3位毫秒）
                                           field_timestamp_without_tz TIMESTAMP(3) WITHOUT TIME ZONE, -- 无时区时间戳
                                           field_boolean BOOLEAN,                        -- 布尔值（true/false/NULL）
                                           field_json JSON,                              -- 原始JSON（不验证结构，查询效率低）
                                           field_uuidv4 UUID                            -- 额外UUID字段（可测试多个UUID）
);