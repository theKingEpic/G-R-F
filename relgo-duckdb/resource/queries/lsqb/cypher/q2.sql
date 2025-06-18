MATCH
    (c:comment)-[:comment_hascreator]->(:person)-[:knows]-(:person)<-[:post_hascreator]-(p:post),
    (c)-[:comment_replyof_post]->(p)
RETURN
    count(*) as r_count;