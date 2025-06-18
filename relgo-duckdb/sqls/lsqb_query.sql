CALL execute_cypher_query("MATCH (country:place)<-[:ispartof]-(city:place)<-[:person_islocatedin]-(p:person)-[:forum_person]-(f:forum)-[:containerof_post]->(:post)<-[:comment_replyof_post]-(c:comment)-[:comment_tag]-(t:tag)-[:tag_hastype]->(:tagclass) RETURN count(*) as r_count;");

CALL execute_cypher_query("MATCH (c:comment)-[:comment_hascreator]->(:person)-[:knows]-(:person)<-[:post_hascreator]-(p:post), (c)-[:comment_replyof_post]->(p) RETURN count(*) as r_count;");

CALL execute_cypher_query("MATCH (pa:person)-[:person_islocatedin]->(ca:place)-[:ispartof]->(country:place)<-[:ispartof]-(cb:place)<-[:person_islocatedin]-(pb:person), (pa)-[:knows]-(pc:person)-[:person_islocatedin]->(cc:place)-[:ispartof]->(country), (pa)-[:knows]-(pb)-[:knows]-(pc) RETURN count(*) as r_count;");


CALL execute_cypher_query("MATCH (:tag)<-[:message_tag]-(m:message)-[:message_hascreator]->(:person), (:comment)-[:comment_replyof_message]->(m)<-[:likes_message]-(:person) RETURN count(*) as r_count;");


CALL execute_cypher_query("MATCH (pt:tag)<-[:message_tag]-(m:message)<-[:comment_replyof_message]-(:comment)-[:comment_tag]->(ct:tag) WHERE pt.t_tagid <> ct.t_tagid RETURN count(*) as r_count;");


CALL execute_cypher_query("MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_tag]->(:tag) WHERE p1.p_personid <> p2.p_personid RETURN count(*) as r_count;");


CALL execute_cypher_query("MATCH (:tag)<-[:message_tag]-(m:message)-[:message_hascreator]->(creator:person) OPTIONAL MATCH (m)<-[:likes_message]-(liker:person) OPTIONAL MATCH (m)<-[:comment_replyof_message]-(c:comment) RETURN count(*) AS count;");