SELECT COUNT(*)
FROM message_tag AS mht
JOIN comment_replyof_message AS crm
  ON mht.mt_messageid = crm.crm_message2id
JOIN comment_tag AS cht1
  ON crm.crm_message1id = cht1.mt_messageid
LEFT JOIN comment_tag AS cht2
       ON mht.mt_tagid = cht2.mt_tagid
      AND crm.crm_message1id = cht2.mt_messageid
WHERE mht.mt_tagid != cht1.mt_tagid
  AND cht2.mt_tagid IS NULL;
