MATCH
    (pa:person)-[:person_islocatedin]->(ca:place)-[:ispartof]->(country:place)<-[:ispartof]-(cb:place)<-[:person_islocatedin]-(pb:person),
    (pa)-[:knows]-(pc:person)-[:person_islocatedin]->(cc:place)-[:ispartof]->(country),
    (pa)-[:knows]-(pb)-[:knows]-(pc)
RETURN
    count(*) as r_count;