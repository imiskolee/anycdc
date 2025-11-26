import json
import logging

from anycdc.core.reader import Reader
import psycopg2
from psycopg2.extras import LogicalReplicationConnection


class PostgresReader(Reader):

    def __init__(self, connector, task):
        super().__init__(connector, task)
        self.connection = None
        self.pk_in_tables = {}

    def start(self):
        self.connect()
        self.sync_publication()
        self.sync_slot()
        cur = self.connection.cursor()
        cur.start_replication(
            slot_name=self.task['slot_name'],
            decode=True,
            options={
                "proto_version": "1",
                "publication_names": self.task['publication_name']
            },
            start_lsn=None,
        )
        cur.consume_stream(self.handler)

    def stop(self):
        pass

    def connect(self):
        dsn = "postgresql://{}:{}@{}:{}}/{}}".format(self.connector['user'],
                                                     self.connector['password'],
                                                     self.connector['host'],
                                                     self.connector['port'],
                                                     self.connector['database']
                                                     )
        self.connection = psycopg2.connect(dsn=dsn)
        self.connection.autocommit = True
        self.writer_connections = []

    def sync_publication(self):
        cur = self.connection.cursor()
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s;", (self.task['publication_name']))
        if cur.fetchone():
            logging.info("[PostgresReader.sync_publication] the publication name already exists.")
            return True
        formatted_tables = [f'"{table}"' if '.' not in table else table for table in self.task['tables']]
        tables_sql = ", ".join(formatted_tables)
        create_sql = f"""
                   CREATE PUBLICATION {self.task['publication_name']}
                   FOR TABLE {tables_sql};
               """
        cur.execute(create_sql)
        logging.info("[PostgresReader.sync_publication] successful created publication:")
        cur.close()

    def sync_slot(self):
        cur = self.connection.cursor()
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s;", (self.task['slot_name'],))
        if not cur.fetchone():
            cur.execute(f"SELECT pg_create_logical_replication_slot(%s, %s);", (self.task['slot_name'], 'pgoutput'))
            logging.info("[PostgresReader.sync_slot] successful created slot")
        else:
            logging.info("[PostgresReader.sync_slot] the slot name already exists.")
        cur.close()

    def handler(self, msg):
        if not msg.payload:
            logging.warning("[PostgresReader.handler] empty payload msg")
            return
        payload = json.loads(msg.payload)
        change_type = payload.get("change_type")
        schema = payload.get("schema", "public")
        table = payload.get("table")
        data = payload.get("data", {})
        old_data = payload.get("old_data", {})

        if not self.is_dml(payload):
            logging.debug("[PostgresReader.handler] skip msg, it's not DML, change_type={}".format(change_type))
            return
        sql = self.dml_msg_to_sql(payload)

        for _, writer in enumerate(self.writer_connections):
            cursor = writer.cursor()
            cursor.autocommit = True
            cursor.execute(sql)

    def dml_msg_to_sql(self, payload):
        change_type = payload.get("change_type")
        schema = payload.get("schema", "public")
        table = payload.get("table")
        data = payload.get("data", {})
        old_data = payload.get("old_data", {})
        pk = self.pk_in_tables[table] or "id"
        sql = ""
        if change_type == "INSERT":
            columns = ", ".join([f'"{k}"' for k in data.keys()])
            values = ", ".join([_format_sql_value(v) for v in data.values()])
            sql = f"INSERT INTO {table} ({columns}) VALUES ({values});"
        elif change_type == "UPDATE":
            set_clause = ", ".join([f'"{k}" = {_format_sql_value(v)}' for k, v in data.items()])
            where_clause = f'"{pk}" = {_format_sql_value(data[pk])}'
            sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause};"

        elif change_type == "DELETE":
            where_clause = f'"{pk}" = {_format_sql_value(data[pk])}'
            sql = f"DELETE FROM {table} WHERE {where_clause};"

        return sql

    def is_dml(self, payload):
        if payload['change_type'] in ["INSERT", "UPDATE", "DELETE", "TRUNCATE"]:
            return True
        return False


def _format_sql_value(value) -> str:
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        # 转义单引号
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, (list, dict)):
        # 处理JSON/数组类型
        return f"'{json.dumps(value)}'::jsonb"
    else:
        # 其他类型直接转字符串
        return str(value)
