SELECT COUNT(*)
FROM tag AS t
JOIN message_tag AS mt
  ON t.t_tagid = mt.mt_tagid
JOIN message
  ON mt.mt_messageid = message.m_messageid
JOIN message_hascreator AS hc 
  ON message.m_messageid = hc.mhc_messageid
JOIN person p1
  ON hc.mhc_personid = p1.p_personid
JOIN comment_replyof_message AS crm 
  ON crm.crm_message2id = message.m_messageid
JOIN comment
  ON comment.m_messageid = crm.crm_message1id
JOIN likes_message AS lm 
  ON lm.l_messageid = message.m_messageid
JOIN person p2
  ON p2.p_personid = lm.l_personid;
