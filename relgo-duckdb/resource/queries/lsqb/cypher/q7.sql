MATCH (:tag)<-[:message_tag]-(m:message)-[:message_hascreator]->(creator:person)
OPTIONAL MATCH (m)<-[:likes_message]-(liker:person)
OPTIONAL MATCH (m)<-[:comment_replyof_message]-(c:comment)
RETURN count(*) AS count