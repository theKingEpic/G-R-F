MATCH (person1:person)-[:knows]-(person2:person)-[:knows]-(person3:person)-[:person_tag]->(tag:tag)
WHERE NOT (person1)-[:knows]-(person3)
  AND person1 <> person3
RETURN count(*) AS count
