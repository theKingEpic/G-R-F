MATCH (p1:person)-[:knows]-(:person)-[:knows]-(p2:person)-[:person_tag]->(:tag)
  WHERE p1.p_personid <> p2.p_personid 
RETURN
    count(*) as r_count;