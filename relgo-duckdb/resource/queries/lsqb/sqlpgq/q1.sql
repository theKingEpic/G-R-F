SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH
    (country:place)<-[:ispartof]-(city:place)<-[:person_islocatedin]-(p:person)-[:forum_person]-(f:forum)-[:containerof_post]->(:post)<-[:comment_replyof_post]-(c:comment)-[:comment_tag]-(t:tag)-[:tag_hastype]->(:tagclass)
  COLUMNS (
    count(*) as r_count
  )
) g;
