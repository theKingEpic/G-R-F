MATCH
  (pt:tag)<-[:message_tag]-(m:message)<-[:comment_replyof_message]-(:comment)-[:comment_tag]->(ct:tag)
  WHERE pt.t_tagid <> ct.t_tagid
RETURN
    count(*) as r_count;