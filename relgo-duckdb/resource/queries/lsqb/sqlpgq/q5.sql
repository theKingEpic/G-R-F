SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH
  (pt:tag)<-[:post_tag]-(po:post)<-[:comment_replyof_post]-(:comment)-[:comment_tag]->(ct:tag)
  WHERE pt.t_tagid <> ct.t_tagid
  COLUMNS (
    count(*) as r_count
  )
) g;
