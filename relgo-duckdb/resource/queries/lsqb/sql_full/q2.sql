SELECT COUNT(*)
FROM person p1
JOIN knows
  ON p1.p_personid = knows.k_person1id
JOIN person p2
  ON knows.k_person2id = p2.p_personid
JOIN comment_hascreator
  ON p1.p_personid = comment_hascreator.chc_personid
JOIN comment
  ON comment.m_messageid = comment_hascreator.chc_messageid
JOIN comment_replyof_post
  ON comment_replyof_post.crp_message1id = comment.m_messageid
JOIN post_hascreator
  ON p2.p_personid = post_hascreator.phc_personid
JOIN post
  ON post.m_messageid = post_hascreator.phc_messageid
 AND comment_replyof_post.crp_message2id = post.m_messageid;
