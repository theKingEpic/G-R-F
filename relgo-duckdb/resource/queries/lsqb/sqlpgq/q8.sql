SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH
  (pt:tag)<-[:post_tag]-(:post)<-[:comment_replyof_post]-(c:comment)-[:comment_tag]->(ct:tag),
  (c)-[:comment_tag]->(pt)
  WHERE pt.t_tagid <> ct.t_tagid
    AND pt.t_tagid IS NULL
  COLUMNS (
    count(*) as r_count
  )
) g;
