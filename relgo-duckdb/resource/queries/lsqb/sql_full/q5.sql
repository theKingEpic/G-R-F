SELECT COUNT(*)
FROM tag AS pt
JOIN message_tag AS mht
  ON pt.t_tagid = mht.mt_tagid
JOIN message AS m
  ON mht.mt_messageid = m.m_messageid
JOIN comment_replyof_message AS crm
  ON m.m_messageid = crm.crm_message2id
JOIN comment
  ON crm.crm_message1id = comment.m_messageid
JOIN comment_tag AS cht
  ON comment.m_messageid = cht.mt_messageid
JOIN tag AS ct
  ON cht.mt_tagid = ct.t_tagid
WHERE mht.mt_tagid != cht.mt_tagid;
