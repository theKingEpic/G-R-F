MATCH (tag1:tag)<-[:message_tag]-(message:message)<-[:comment_replyof_message]-(comment:comment)-[:comment_tag]->(tag2:tag)
WHERE NOT (comment)-[:comment_tag]->(tag1)
  AND tag1 <> tag2
RETURN count(*) AS count
