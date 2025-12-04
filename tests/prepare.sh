#!/bin/bash

go build -v

./tests --config-dir=../conf --connector test_pg_1 --sql sql/001_init_postgres.sql
./tests --config-dir=../conf --connector test_pg_2 --sql sql/001_init_postgres.sql
./tests --config-dir=../conf --connector test_mysql_1 --sql sql/001_init_mysql.sql
./tests --config-dir=../conf --connector test_mysql_1 --sql sql/001_init_mysql.sql



