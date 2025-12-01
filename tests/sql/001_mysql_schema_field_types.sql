CREATE TABLE IF NOT EXISTS field_types (
    -- 主键：UUID 类型（MySQL 8.0+ 原生支持 UUID，低版本需用 VARCHAR(36) 模拟）
                                           field_uuid CHAR(36) PRIMARY KEY DEFAULT (UUID()),

    -- 数值类型映射：PostgreSQL -> MySQL（保持字节长度和取值范围一致）
                                           field_smallint SMALLINT,                      -- 对应 PostgreSQL SMALLINT（2字节，-32768 至 32767）
                                           field_integer INT,                            -- 对应 PostgreSQL INTEGER（4字节，-2147483648 至 2147483647）
                                           field_bigint BIGINT,                          -- 对应 PostgreSQL BIGINT（8字节，-9223372036854775808 至 9223372036854775807）
                                           field_decimal DECIMAL(8, 3),                  -- 与 PostgreSQL DECIMAL 完全兼容（8位总长度，3位小数）

    -- 字符类型映射：保持长度语义一致
                                           field_char CHAR(10),                          -- 对应 PostgreSQL CHAR(10)（固定长度，不足补空格）
                                           field_varchar VARCHAR(255),                    -- 对应 PostgreSQL VARCHAR(255)（修正原注释“最大50字符”为255，与字段定义一致）
                                           field_text TEXT,                              -- 对应 PostgreSQL TEXT（无长度限制，MySQL TEXT 最大65535字节）

    -- 日期时间类型映射：保持精度和时区语义
                                           field_date DATE,                              -- 对应 PostgreSQL DATE（YYYY-MM-DD）
                                           field_time TIME(3),                           -- 对应 PostgreSQL TIME(3)（支持3位毫秒，格式 HH:MI:SS.ms）
                                           field_timestamp TIMESTAMP(3),                 -- 对应 PostgreSQL TIMESTAMP(3) WITH TIME ZONE：
    -- MySQL TIMESTAMP 本身带时区（存储UTC，查询时转换为会话时区），与PG带时区语义一致
                                           field_timestamp_without_tz DATETIME(3),       -- 对应 PostgreSQL TIMESTAMP(3) WITHOUT TIME ZONE：
    -- MySQL DATETIME 不带时区，直接存储原始时间
                                           field_boolean TINYINT(1),                     -- 对应 PostgreSQL BOOLEAN：MySQL 无原生BOOLEAN，用 TINYINT(1) 模拟（1=true/0=false/NULL=未知）

    -- JSON 类型映射：MySQL 5.7+ 支持原生 JSON 类型
                                           field_json JSON,                              -- 对应 PostgreSQL JSON（MySQL JSON 会验证结构，功能优于PG JSON，语义一致）

    -- 额外 UUID 字段：与主键类型一致
                                           field_uuidv4 CHAR(36) DEFAULT (UUID())        -- 对应 PostgreSQL UUID，用 CHAR(36) 存储标准UUID字符串
);