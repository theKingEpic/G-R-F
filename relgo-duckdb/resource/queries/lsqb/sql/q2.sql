SELECT COUNT(*)
FROM knows
JOIN comment_hascreator
  ON knows.k_person1id = comment_hascreator.chc_personid
JOIN comment
  ON comment.m_messageid = comment_hascreator.chc_messageid
JOIN comment_replyof_post
  ON comment_replyof_post.crp_message1id = comment.m_messageid
JOIN post_hascreator
  ON knows.k_person2id = post_hascreator.phc_personid
JOIN post
  ON post.m_messageid = post_hascreator.phc_messageid
 AND comment_replyof_post.crp_message2id = post.m_messageid;
