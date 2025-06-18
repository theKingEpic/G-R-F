MATCH
    (country:place)<-[:ispartof]-(city:place)<-[:person_islocatedin]-(p:person)-[:forum_person]-(f:forum)-[:containerof_post]->(:post)<-[:comment_replyof_post]-(c:comment)-[:comment_tag]-(t:tag)-[:tag_hastype]->(:tagclass)
RETURN
    count(*) as r_count;



EXPLAIN SELECT count(*)
FROM Person_knows_Person pkp1
JOIN Person_knows_Person pkp2
  ON pkp1.Person2Id = pkp2.Person1Id
 AND pkp1.Person1Id != pkp2.Person2Id
JOIN Person_hasInterest_Tag
  ON pkp2.Person2Id = Person_hasInterest_Tag.PersonId
LEFT JOIN Person_knows_Person pkp3
       ON pkp3.Person1Id = pkp1.Person1Id
      AND pkp3.Person2Id = pkp2.Person2Id
    WHERE pkp3.Person1Id IS NULL;