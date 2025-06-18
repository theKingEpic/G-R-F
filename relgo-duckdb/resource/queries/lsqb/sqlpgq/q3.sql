SELECT r_count
FROM GRAPH_TABLE (graph
  MATCH
    (pa:person)-[:person_islocatedin]->(ca:place)-[:ispartof]->(country:place)<-[:ispartof]-(cb:place)<-[:person_islocatedin]-(pb:person),
    (pa)-[:knows]-(pc:person)-[:person_islocatedin]->(cc:place)-[:ispartof]->(country),
    (pa)-[:knows]-(pb)-[:knows]-(pc)
  COLUMNS (
    count(*) as r_count
  )
) g;
