## Why AnyCDC

![Intro](docs/intro.png)

AnyCDC is a high-performance cross-database synchronization tool that supports data replication across 
a wide range of heterogeneous databases, with the following core features:

* **Schema Sync:** Supports automatic table structure migration, enabling fully unattended long-term data synchronization. The synchronization process can still run normally if this feature is disabled, but non-existent columns will be ignored on the target side.

* **Full Dump:** Supports full-table bulk data synchronization and allows on-demand re-synchronization at any time.

* **Realtime CDC Sync:** Establishes a CDC subscription to perform near-real-time incremental data synchronization when the target database supports CDC technology.

* **Cron Sync:** Implements scheduled batch synchronization via cron jobs based on incremental keys. This feature is related to CDC but offers lower real-time performance.

* **Web Portal:** Comes with a lightweight built-in dashboard for connection and task management.

* **UDF:** Supports custom data transformation through user-defined functions (UDFs) written in JavaScript.

## Supports Data Sources

Here are the types of databases we plan to support:

| Database | Reader | Writer |
| --- |--------| --- |
|PostgresSQL | Y      | Y |
|MySQL | Y      | Y |
|MariaDB  | Y      | Y |
|Elasticsearch | N      | Y |
|StarRocks | N      | Y |
|Clickhouse | N      | Y |
|Kafka | Y      | Y |
|Redis | Y      | Y |
|RabbitMQ | Y | Y |

