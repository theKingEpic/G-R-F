SELECT COUNT(*)
FROM place AS Country
JOIN ispartof
  ON ispartof.ipo_place2id = Country.pl_placeid
JOIN place AS City
  ON ispartof.ipo_place1id = City.pl_placeid
JOIN person_islocatedin
  ON person_islocatedin.pi_placeid = City.pl_placeid
JOIN person
  ON person.p_personid = person_islocatedin.pi_personid
JOIN forum_person
  ON forum_person.fp_personid = person.p_personid
JOIN forum
  ON forum.f_forumid = forum_person.fp_forumid
JOIN containerof_post
  ON containerof_post.co_forumid = forum.f_forumid
JOIN post
  ON post.m_messageid = containerof_post.co_messageid
JOIN comment_replyof_post
  ON comment_replyof_post.crp_message2id = post.m_messageid
JOIN comment
  ON comment.m_messageid = comment_replyof_post.crp_message1id
JOIN comment_tag
  ON comment_tag.mt_messageid = comment.m_messageid
JOIN tag
  ON tag.t_tagid = comment_tag.mt_tagid
JOIN tag_hastype
  ON tag_hastype.tht_tagid = tag.t_tagid
JOIN tagclass
  ON tagclass.tc_tagclassid = tag_hastype.tht_tagclassid;
