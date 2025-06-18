MATCH
  (:tag)<-[:message_tag]-(m:message)-[:message_hascreator]->(:person),
  (:comment)-[:comment_replyof_message]->(m)<-[:likes_message]-(:person)
RETURN
    count(*) as r_count;
