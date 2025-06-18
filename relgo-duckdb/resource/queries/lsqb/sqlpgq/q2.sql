SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH
    (c:comment)-[:comment_hascreator]->(:person)-[:knows]-(:person)<-[:post_hascreator]-(p:post),
    (c)-[:comment_replyof_post]->(p)
  COLUMNS (
    count(*) as r_count
  )
) g;
