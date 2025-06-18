.read ../../sqls/create.sql

COPY person            FROM '../../resource/graph/sf01_merge/dynamic/person_0_0.csv'                      (DELIMITER '|', HEADER);

COPY knows (k_person1id, k_person2id, k_creationdate) FROM '../../resource/graph/sf01_merge/dynamic/person_knows_person_0_0.csv' (DELIMITER '|', HEADER);
COPY knows (k_person2id, k_person1id, k_creationdate) FROM '../../resource/graph/sf01_merge/dynamic/person_knows_person_0_0.csv' (DELIMITER '|', HEADER);

LOAD './extension/relgo/relgo.duckdb_extension';

CALL relgo_create_graph_index('CREATE SELF RAI knows_r ON knows (FROM k_person1id REFERENCES person.p_personid, TO k_person2id REFERENCES person.p_personid);');

CALL init_graph_stats();