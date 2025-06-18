SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH
  (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_tag]->(:tag),
  (p1)-[:knows]-(p2)
  WHERE p1.p_personid <> p2.p_personid
    AND p1.p_personid IS NULL
  COLUMNS (
    count(*) as r_count
  )
) g;
