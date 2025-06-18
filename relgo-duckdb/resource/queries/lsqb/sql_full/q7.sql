SELECT COUNT(*)
FROM message_tag AS mht
JOIN message_hascreator AS mhc
  ON mht.mt_messageid = mhc.mhc_messageid
LEFT JOIN comment_replyof_message AS crm
  ON crm.crm_message2id = mht.mt_messageid
LEFT JOIN likes_message AS plm
  ON plm.l_messageid = mht.mt_messageid;
