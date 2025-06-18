SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH
  (pt:tag)<-[:comment_tag]-(co:comment)-[:comment_hascreator]->(:person),
  (:comment)-[:comment_replyof_comment]->(co)<-[:likes_comment]-(:person)
  COLUMNS (
    count(*) as r_count
  )
) g;
