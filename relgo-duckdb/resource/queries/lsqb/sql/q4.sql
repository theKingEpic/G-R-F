SELECT COUNT(*)
FROM message_tag AS mt
JOIN message_hascreator AS hc 
  ON mt.mt_messageid = hc.mhc_messageid
JOIN comment_replyof_message AS crm 
  ON crm.crm_message2id = mt.mt_messageid
JOIN likes_message AS lm 
  ON lm.l_messageid = mt.mt_messageid;
