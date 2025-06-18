SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH
  (:tag)<-[:post_tag]-(po:post)-[:post_hascreator]->(:person),
  (:comment)-[:comment_replyof_post]->(po)<-[:likes_post]-(:person)
  COLUMNS (
    count(*) as r_count
  )
) g;
