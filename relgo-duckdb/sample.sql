set threads to 1;

create table person (
    p_personid bigint not null PRIMARY KEY,
    p_firstname varchar not null,
    p_lastname varchar not null,
    p_gender varchar not null,
    p_birthday varchar not null,
    p_creationdate bigint not null,
    p_locationip varchar not null,
    p_browserused varchar not null
 );

 create table knows (
    k_person1id bigint not null,
    k_person2id bigint not null,
    k_creationdate bigint not null,
    FOREIGN KEY (k_person1id) REFERENCES person(p_personid),
    FOREIGN KEY (k_person2id) REFERENCES person(p_personid)
);

COPY person            FROM '../../resource/graph/sf01_merge/dynamic/person_0_0.csv'                      (DELIMITER '|', HEADER);

COPY knows (k_person1id, k_person2id, k_creationdate) FROM '../../resource/graph/sf01_merge/dynamic/person_knows_person_0_0.csv' (DELIMITER '|', HEADER);

COPY knows (k_person2id, k_person1id, k_creationdate) FROM '../../resource/graph/sf01_merge/dynamic/person_knows_person_0_0.csv' (DELIMITER '|', HEADER);

LOAD './extension/relgo/relgo.duckdb_extension';

CALL relgo_create_graph_index('CREATE SELF RAI knows_r ON knows (FROM k_person1id REFERENCES person.p_personid, TO k_person2id REFERENCES person.p_personid);');


CALL init_graph_stats();

CALL execute_graph_query('SELECT g.p2_id, g.p2_lastname, g.p2_birthday, g.p2_creationdate, g.p2_gender, g.p2_browserused, g.p2_locationip FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person) WHERE p1.p_personid = 933L AND p2.p_firstname = "Karl" COLUMNS ( p2.p_personid as p2_id, p2.p_lastname as p2_lastname, p2.p_birthday as p2_birthday, p2.p_creationdate as p2_creationdate, p2.p_gender as p2_gender, p2.p_browserused as p2_browserused, p2.p_locationip as p2_locationip ) ) g;');