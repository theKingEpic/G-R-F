SELECT COUNT(*)
FROM knows AS pkp1
JOIN knows AS pkp2
  ON pkp1.k_person2id = pkp2.k_person1id
 AND pkp1.k_person1id != pkp2.k_person2id
JOIN person_tag
  ON pkp2.k_person2id = person_tag.pt_personid
LEFT JOIN knows AS pkp3
       ON pkp3.k_person1id = pkp1.k_person1id
      AND pkp3.k_person2id = pkp2.k_person2id
WHERE pkp3.k_person1id IS NULL;
