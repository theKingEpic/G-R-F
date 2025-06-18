SELECT COUNT(*)
FROM message_tag AS mht
JOIN comment_replyof_message AS crm
  ON mht.mt_messageid = crm.crm_message2id
JOIN comment_tag AS cht
  ON crm.crm_message1id = cht.mt_messageid
WHERE mht.mt_tagid != cht.mt_tagid;
