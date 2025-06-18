.read ../../sqls/create.sql

.read ../../sqls/load_ldbc10.sql

LOAD './extension/relgo/relgo.duckdb_extension';

.read ../../sqls/index.sql

CALL init_graph_stats();

set threads to 1;