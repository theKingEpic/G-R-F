SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_tag]->(:tag)
  WHERE p1.p_personid <> p2.p_personid 
  COLUMNS (
    count(*) as r_count
  )
) g;
