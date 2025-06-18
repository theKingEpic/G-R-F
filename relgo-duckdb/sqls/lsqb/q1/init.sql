.read ../../sqls/create.sql

.read ../../sqls/load_ldbc10.sql

LOAD './extension/relgo/relgo.duckdb_extension';

.read ../../sqls/index_ldbc10.sql

CALL init_graph_stats();