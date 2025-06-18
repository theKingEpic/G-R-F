COPY person            FROM '../../resource/graph/sf1_merge/dynamic/person_0_0.csv'                      (DELIMITER '|', HEADER);
COPY place            FROM '../../resource/graph/sf1_merge/static/place_0_0.csv'                      (DELIMITER '|', HEADER);
COPY tag            FROM '../../resource/graph/sf1_merge/static/tag_0_0.csv'                      (DELIMITER '|', HEADER);

COPY ispartof            FROM '../../resource/graph/sf1_merge/static/place_isPartOf_place_0_0.csv'                      (DELIMITER '|', HEADER);

COPY tagclass            FROM '../../resource/graph/sf1_merge/static/tagclass_0_0.csv'                      (DELIMITER '|', HEADER);

COPY forum             FROM '../../resource/graph/sf1_merge/dynamic/forum_0_0.csv'                       (DELIMITER '|', HEADER);

COPY comment             FROM '../../resource/graph/sf1_merge/dynamic/comment_0_0.csv'                       (DELIMITER '|', HEADER);

COPY post             FROM '../../resource/graph/sf1_merge/dynamic/post_0_0.csv'                       (DELIMITER '|', HEADER);

COPY organisation             FROM '../../resource/graph/sf1_merge/static/organisation_0_0.csv'                       (DELIMITER '|', HEADER);


COPY forum_person      FROM '../../resource/graph/sf1_merge/dynamic/forum_hasMember_person_0_0.csv'      (DELIMITER '|', HEADER);


COPY person_islocatedin            FROM '../../resource/graph/sf1_merge/dynamic/person_isLocatedIn_place_0_0.csv'                      (DELIMITER '|', HEADER);


COPY person_tag            FROM '../../resource/graph/sf1_merge/dynamic/person_hasInterest_tag_0_0.csv'                      (DELIMITER '|', HEADER);


COPY likes_comment            FROM '../../resource/graph/sf1_merge/dynamic/person_likes_comment_0_0.csv'                      (DELIMITER '|', HEADER);

COPY likes_post            FROM '../../resource/graph/sf1_merge/dynamic/person_likes_post_0_0.csv'                      (DELIMITER '|', HEADER);

COPY comment_hascreator            FROM '../../resource/graph/sf1_merge/dynamic/comment_hasCreator_person_0_0.csv'                      (DELIMITER '|', HEADER);

COPY comment_islocatedin            FROM '../../resource/graph/sf01_merge/dynamic/comment_isLocatedIn_place_0_0.csv'                      (DELIMITER '|', HEADER);

COPY comment_replyof_post            FROM '../../resource/graph/sf1_merge/dynamic/comment_replyOf_post_0_0.csv'                      (DELIMITER '|', HEADER);

COPY comment_replyof_comment            FROM '../../resource/graph/sf1_merge/dynamic/comment_replyOf_comment_0_0.csv'                      (DELIMITER '|', HEADER);


COPY comment_tag            FROM '../../resource/graph/sf1_merge/dynamic/comment_hasTag_tag_0_0.csv'                      (DELIMITER '|', HEADER);

COPY post_hascreator            FROM '../../resource/graph/sf1_merge/dynamic/post_hasCreator_person_0_0.csv'                      (DELIMITER '|', HEADER);

COPY post_tag            FROM '../../resource/graph/sf1_merge/dynamic/post_hasTag_tag_0_0.csv'                      (DELIMITER '|', HEADER);


COPY containerof_post            FROM '../../resource/graph/sf1_merge/dynamic/forum_containerOf_post_0_0.csv'                      (DELIMITER '|', HEADER);


COPY tag_hastype            FROM '../../resource/graph/sf1_merge/static/tag_hasType_tagclass_0_0.csv'                      (DELIMITER '|', HEADER);


COPY knows (k_person1id, k_person2id, k_creationdate) FROM '../../resource/graph/sf1_merge/dynamic/person_knows_person_0_0.csv' (DELIMITER '|', HEADER);
COPY knows (k_person2id, k_person1id, k_creationdate) FROM '../../resource/graph/sf1_merge/dynamic/person_knows_person_0_0.csv' (DELIMITER '|', HEADER);


create table message AS
  SELECT m_messageid AS m_messageid FROM comment
  UNION ALL
  SELECT m_messageid AS m_messageid FROM post;

create table comment_replyof_message AS
  SELECT crc_message1id AS crm_message1id, crc_message2id AS crm_message2id FROM comment_replyof_comment
  UNION ALL
  SELECT crp_message1id AS crm_message1id, crp_message2id AS crm_message2id FROM comment_replyof_post;

create table message_hascreator AS
  SELECT chc_messageid AS mhc_messageid, chc_personid AS mhc_personid FROM comment_hascreator
  UNION ALL
  SELECT phc_messageid AS mhc_messageid, phc_personid AS mhc_personid FROM post_hascreator;


create table message_tag AS
  SELECT mt_messageid AS mt_messageid, mt_tagid AS mt_tagid FROM comment_tag
  UNION ALL
  SELECT mt_messageid AS mt_messageid, mt_tagid AS mt_tagid FROM post_tag;

 
create table message_islocatedin AS
  SELECT ci_messageid AS mi_messageid, ci_placeid AS mi_placeid FROM comment_islocatedin
  UNION ALL
  SELECT pi_messageid AS mi_messageid, pi_placeid AS mi_placeid FROM post_islocatedin;


create table likes_message AS
  SELECT l_personid, l_messageid FROM likes_comment
  UNION ALL
  SELECT l_personid, l_messageid FROM likes_post;