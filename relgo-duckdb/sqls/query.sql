CALL execute_graph_query('SELECT g.p2_id, g.p2_lastname, g.p2_birthday, g.p2_creationdate, g.p2_gender, g.p2_browserused, g.p2_locationip, g.pl_name FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person)-[:person_islocatedin]->(pl:place) WHERE p1.p_personid = 933L AND p2.p_firstname = "Karl" COLUMNS (p2.p_personid as p2_id, p2.p_lastname as p2_lastname, p2.p_birthday as p2_birthday, p2.p_creationdate as p2_creationdate, p2.p_gender as p2_gender, p2.p_browserused as p2_browserused, p2.p_locationip as p2_locationip, pl.pl_name as pl_name )) g;');


CALL execute_graph_query('SELECT g.p2_id, g.p2_lastname, g.p2_birthday, g.p2_creationdate, g.p2_gender, g.p2_browserused, g.p2_locationip, g.pl_name FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_islocatedin]->(pl:place) WHERE p1.p_personid = 933L AND p2.p_firstname = "Boris" COLUMNS (p2.p_personid as p2_id, p2.p_lastname as p2_lastname, p2.p_birthday as p2_birthday, p2.p_creationdate as p2_creationdate, p2.p_gender as p2_gender, p2.p_browserused as p2_browserused, p2.p_locationip as p2_locationip, pl.pl_name as pl_name )) g;');

CALL execute_graph_query('SELECT g.p2_id, g.p2_lastname, g.p2_birthday, g.p2_creationdate, g.p2_gender, g.p2_browserused, g.p2_locationip, g.pl_name FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_islocatedin]->(pl:place) WHERE p1.p_personid = 933L AND p2.p_firstname = "Abdullah" COLUMNS (p2.p_personid as p2_id, p2.p_lastname as p2_lastname, p2.p_birthday as p2_birthday, p2.p_creationdate as p2_creationdate, p2.p_gender as p2_gender, p2.p_browserused as p2_browserused, p2.p_locationip as p2_locationip, pl.pl_name as pl_name )) g;');

CALL execute_graph_query('SELECT g.p2_id, g.p2_firstname, g.p2_lastname, g.c_messageid, g.c_content, g.c_creationdate FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person)<-[:comment_hascreator]-(c:comment) WHERE p1.p_personid = 933L AND c.m_creationdate < 1342766586311L COLUMNS (p2.p_personid as p2_id, p2.p_firstname as p2_firstname, p2.p_lastname as p2_lastname, c.m_messageid as c_messageid, c.m_content as c_content, c.m_creationdate as c_creationdate )) g;');

CALL execute_graph_query('SELECT g.p2_id, g.p2_firstname, g.p2_lastname FROM GRAPH_TABLE (graph MATCH (p1:person)-[k1:knows]-(p2:person)<-[:comment_hascreator]-(m1:comment)-[:comment_islocatedin]->(pl1:place), (p2)<-[:comment_hascreator]-(m2:comment)-[:comment_islocatedin]->(pl2:place) WHERE p1.p_personid = 933L AND m1.m_creationdate >= 1346770795431L AND m1.m_creationdate < 1348066795431L AND m2.m_creationdate >= 1346770795431L AND m2.m_creationdate < 1348066795431L AND pl1.pl_name = "Turkey" AND pl2.pl_name = "Turkey" COLUMNS (p2.p_personid as p2_id, p2.p_firstname as p2_firstname, p2.p_lastname as p2_lastname )) g;');

CALL execute_graph_query('SELECT g.p2_id, g.p2_firstname, g.p2_lastname FROM GRAPH_TABLE (graph MATCH (p1:person)-[k1:knows]-(:person)-[:knows]-(p2:person)<-[:comment_hascreator]-(m1:comment)-[:comment_islocatedin]->(pl1:place), (p2)<-[:comment_hascreator]-(m2:comment)-[:comment_islocatedin]->(pl2:place) WHERE p1.p_personid = 26388279067479L AND m1.m_creationdate >= 1296242799012L AND m1.m_creationdate < 1301426799012L AND m2.m_creationdate >= 1296242799012L AND m2.m_creationdate < 1301426799012L AND pl1.pl_name = "Germany" AND pl2.pl_name = "Germany" COLUMNS (p2.p_personid as p2_id, p2.p_firstname as p2_firstname, p2.p_lastname as p2_lastname )) g;');

CALL execute_graph_query('SELECT g.t_name FROM GRAPH_TABLE (graph MATCH (:person)-[k1:knows]-(p1:person)-[k2:knows]-(p2:person)<-[:post_hascreator]-(ps:post)-[:post_tag]->(t:tag) WHERE p1.p_personid = 933L AND ps.m_creationdate >= 1334934943597L AND ps.m_creationdate < 1337526943597L COLUMNS (t.t_name as t_name )) g;');

CALL execute_graph_query('SELECT g.f_title FROM GRAPH_TABLE (graph MATCH (p1:person)-[k1:knows]-(p2:person)-[fp:forum_person]->(f:forum)-[:containerof_post]->(p:post), (p)-[:post_hascreator]->(p2) WHERE p1.p_personid = 30786325577877L AND fp.fp_joindate >= 1334156663989L COLUMNS (f.f_title AS f_title )) g;');

CALL execute_graph_query('SELECT g.f_title FROM GRAPH_TABLE (graph MATCH (p1:person)-[k1:knows]-(:person)-[:knows]-(p2:person)-[fp:forum_person]->(f:forum)-[:containerof_post]->(p:post), (p)-[:post_hascreator]->(p2) WHERE p1.p_personid = 30786325578724L AND fp.fp_joindate >= 1330764256538L COLUMNS (f.f_title AS f_title )) g;');

CALL execute_graph_query('SELECT g.t_name FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person)<-[:post_hascreator]-(m:post)-[:post_tag]->(t1:tag), (m)-[:post_tag]->(t2:tag) WHERE p1.p_personid = 933L AND t1.t_name = "Perry_Como" AND t2.t_name <> "Perry_Como" COLUMNS (t2.t_name as t_name )) g;');

CALL execute_graph_query('SELECT g.t_name FROM GRAPH_TABLE (graph MATCH (p1:person)-[k1:knows]-(:person)-[:knows]-(p2:person)<-[:post_hascreator]-(m:post)-[:post_tag]->(t1:tag), (m)-[:post_tag]->(t2:tag) WHERE p1.p_personid = 2199023256077L AND t1.t_name = "Perry_Como" AND t2.t_name <> "Perry_Como" COLUMNS (t2.t_name as t_name )) g;');

CALL execute_graph_query('SELECT g.p_personid, g.p_firstname, g.p_lastname, g.m_content FROM GRAPH_TABLE (graph MATCH (p2:person)-[:likes_comment]->(c:comment)-[:comment_hascreator]->(p1:person), (p1)-[:knows]-(p2) WHERE p1.p_personid = 17592186044519L COLUMNS (p2.p_personid AS p_personid, p2.p_firstname AS p_firstname, p2.p_lastname AS p_lastname, c.m_content AS m_content )) g;');

CALL execute_graph_query('SELECT g.p_personid, g.p_firstname, g.p_lastname, g.m_creationdate, g.m_messageid, g.m_content FROM GRAPH_TABLE (graph MATCH (p2:person)<-[:comment_hascreator]-(c:comment)-[:comment_replyof_post]->(ps:post)-[:post_hascreator]->(p1:person) WHERE p1.p_personid = 933L COLUMNS (p2.p_personid AS p_personid, p2.p_firstname AS p_firstname, p2.p_lastname AS p_lastname, c.m_creationdate AS m_creationdate, c.m_messageid AS m_messageid, c.m_content AS m_content )) g;');

CALL execute_graph_query('SELECT g.p_firstname, g.p_lastname, g.m_creationdate FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person)<-[:comment_hascreator]-(c:comment) WHERE p1.p_personid = 28587302323430L AND c.m_creationdate < 1313592219961L COLUMNS (p2.p_firstname AS p_firstname, p2.p_lastname AS p_lastname, c.m_creationdate AS m_creationdate )) g;');

CALL execute_graph_query('SELECT g.p_firstname, g.p_lastname, g.m_creationdate FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)<-[:comment_hascreator]-(c:comment) WHERE p1.p_personid = 933L AND c.m_creationdate < 1329013353050L COLUMNS (p2.p_firstname AS p_firstname, p2.p_lastname AS p_lastname, c.m_creationdate AS m_creationdate )) g;');

CALL execute_graph_query('SELECT g.p_personid, g.p_firstname, g.p_lastname, g.o_name, g.pc_workfrom FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person)-[pc:person_company]->(o:organisation)-[:org_islocatedin]->(pl:place) WHERE p1.p_personid = 933L AND pc.pc_workfrom < 2010 AND pl.pl_name = "Niger" COLUMNS (p2.p_personid AS p_personid, p2.p_firstname AS p_firstname, p2.p_lastname AS p_lastname, o.o_name AS o_name, pc.pc_workfrom AS pc_workfrom )) g;');

CALL execute_graph_query('SELECT g.p_personid, g.p_firstname, g.p_lastname, g.o_name, g.pc_workfrom FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[pc:person_company]->(o:organisation)-[:org_islocatedin]->(pl:place) WHERE p1.p_personid = 2199023256077L AND pc.pc_workfrom < 2010 AND pl.pl_name = "Niger" COLUMNS (p2.p_personid AS p_personid, p2.p_firstname AS p_firstname, p2.p_lastname AS p_lastname, o.o_name AS o_name, pc.pc_workfrom AS pc_workfrom )) g;');

CALL execute_graph_query('SELECT g.p_personid, g.p_firstname, g.p_lastname FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(f:person)<-[:comment_hascreator]-(c:comment)-[:comment_replyof_post]->(:post)-[:post_tag]->(t:tag)-[:tag_hastype]->(tc1:tagclass)-[:issubclassof]->(tc2:tagclass) WHERE p1.p_personid = 28587302323430L AND tc2.tc_name = "Agent" COLUMNS (f.p_personid AS p_personid, f.p_firstname AS p_firstname, f.p_lastname AS p_lastname )) g;');


CALL explain_graph_query('SELECT g.p2_id, g.p2_lastname, g.p2_birthday, g.p2_creationdate, g.p2_gender, g.p2_browserused, g.p2_locationip FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person) WHERE p1.p_personid = 933L AND p2.p_firstname = "Karl" COLUMNS ( p2.p_personid as p2_id, p2.p_lastname as p2_lastname, p2.p_birthday as p2_birthday, p2.p_creationdate as p2_creationdate, p2.p_gender as p2_gender, p2.p_browserused as p2_browserused, p2.p_locationip as p2_locationip ) ) g;');



CALL explain_graph_query('SELECT count FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person) WHERE p1.p_personid = 933L AND p2.p_firstname = "Karl" COLUMNS ( count(*) as count ) ) g;');

CALL explain_graph_query('SELECT count FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(p2:person) WHERE p1.p_personid = 933L AND p2.p_firstname = "Karl" COLUMNS ( count(p1.p_personid) as count ) ) g;');

CALL execute_graph_query('SELECT count FROM GRAPH_TABLE (graph MATCH (p1:person) COLUMNS (count(*) AS count)) g;');

CALL execute_graph_query('SELECT p1id, p2id FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]->(p2:person) COLUMNS (max(p1.p_personid) AS p1id, min(p2.p_personid) AS p2id)) g;');


CALL execute_cypher_query("MATCH (p1:person) RETURN p1.p_personid;");


CALL execute_graph_query('SELECT p1id, p2id FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]->(p2:person) WHERE p1.p_personid <> p2.p_personid AND (p1.p_firstname = p2.p_firstname or p1.p_lastname <> p2.p_lastname) COLUMNS (max(p1.p_personid) AS p1id, min(p2.p_personid) AS p2id)) g;');


-- lsqb:

call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (country:place)<-[:ispartof]-(city:place)<-[:person_islocatedin]-(p:person)-[:forum_person]-(f:forum)-[:containerof_post]->(:post)<-[:comment_replyof_post]-(c:comment)-[:comment_tag]-(t:tag)-[:tag_hastype]->(:tagclass) COLUMNS ( count(*) as r_count ) ) g;');

call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (c:comment)-[:comment_hascreator]->(:person)-[:knows]-(:person)<-[:post_hascreator]-(p:post), (c)-[:comment_replyof_post]->(p) COLUMNS ( count(*) as r_count ) ) g; ');


call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (pa:person)-[:person_islocatedin]->(ca:place)-[:ispartof]->(country:place)<-[:ispartof]-(cb:place)<-[:person_islocatedin]-(pb:person), (pa)-[:knows]-(pc:person)-[:person_islocatedin]->(cc:place)-[:ispartof]->(country), (pa)-[:knows]-(pb)-[:knows]-(pc) COLUMNS ( count(*) as r_count ) ) g; ');


call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (:tag)<-[:post_tag]-(po:post)-[:post_hascreator]->(:person), (:comment)-[:comment_replyof_post]->(po)<-[:likes_post]-(:person) COLUMNS ( count(*) as r_count ) ) g; ');


call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (pt:tag)<-[:post_tag]-(po:post)<-[:comment_replyof_post]-(:comment)-[:comment_tag]->(ct:tag) COLUMNS ( count(*) as r_count ) ) g;');


call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_tag]->(:tag) WHERE p1.p_personid <> p2.p_personid COLUMNS ( count(*) as r_count ) ) g; ');


call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (pt:tag)<-[:comment_tag]-(co:comment)-[:comment_hascreator]->(:person), (:comment)-[:comment_replyof_comment]->(co)<-[:likes_comment]-(:person) COLUMNS ( count(*) as r_count ) ) g;');


call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (pt:tag)<-[:post_tag]-(:post)<-[:comment_replyof_post]-(c:comment)-[:comment_tag]->(ct:tag), (c)-[:comment_tag]->(pt) WHERE pt.t_tagid <> ct.t_tagid COLUMNS ( count(*) as r_count ) ) g; ');


call execute_graph_query('SELECT r_count FROM GRAPH_TABLE (graph MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_tag]->(:tag), (p1)-[:knows]-(p2) WHERE p1.p_personid <> p2.p_personid COLUMNS ( count(*) as r_count ) ) g;');


-- lsqb cypher

CALL execute_cypher_query("MATCH (country:place)<-[:ispartof]-(city:place)<-[:person_islocatedin]-(p:person)-[:forum_person]-(f:forum)-[:containerof_post]->(:post)<-[:comment_replyof_post]-(c:comment)-[:comment_tag]-(t:tag)-[:tag_hastype]->(:tagclass) RETURN count(*) as r_count;");

CALL execute_cypher_query("MATCH (c:comment)-[:comment_hascreator]->(:person)-[:knows]-(:person)<-[:post_hascreator]-(p:post), (c)-[:comment_replyof_post]->(p) RETURN count(*) as r_count;");


CALL execute_cypher_query("MATCH (pa:person)-[:person_islocatedin]->(ca:place)-[:ispartof]->(country:place)<-[:ispartof]-(cb:place)<-[:person_islocatedin]-(pb:person), (pa)-[:knows]-(pc:person)-[:person_islocatedin]->(cc:place)-[:ispartof]->(country), (pa)-[:knows]-(pb)-[:knows]-(pc) RETURN count(*) as r_count;");


CALL execute_cypher_query("MATCH (:tag)<-[:message_tag]-(m:message)-[:message_hascreator]->(:person), (:comment)-[:comment_replyof_message]->(m)<-[:likes_message]-(:person) RETURN count(*) as r_count;");


CALL execute_cypher_query("MATCH (pt:tag)<-[:message_tag]-(m:message)<-[:comment_replyof_message]-(:comment)-[:comment_tag]->(ct:tag) WHERE pt.t_tagid <> ct.t_tagid RETURN count(*) as r_count;");

call execute_cypher_query("MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_tag]->(:tag) WHERE p1.p_personid <> p2.p_personid  RETURN count(*) as r_count;");


CALL execute_cypher_query("MATCH (:tag)<-[:message_tag]-(m:message)-[:message_hascreator]->(creator:person) OPTIONAL MATCH (m)<-[:likes_message]-(liker:person) OPTIONAL MATCH (m)<-[:comment_replyof_message]-(c:comment) RETURN count(*) AS count;");


