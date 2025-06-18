.read ../../sqls/create.sql

.read ../../sqls/load.sql

LOAD './extension/relgo/relgo.duckdb_extension';

.read ../../sqls/index.sql

CALL init_graph_stats();
