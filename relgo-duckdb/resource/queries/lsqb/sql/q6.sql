SELECT COUNT(*)
FROM knows AS pkp1
JOIN knows AS pkp2
  ON pkp1.k_person2id = pkp2.k_person1id
 AND pkp1.k_person1id != pkp2.k_person2id
JOIN person_tag
  ON person_tag.pt_personid = pkp2.k_person2id;

