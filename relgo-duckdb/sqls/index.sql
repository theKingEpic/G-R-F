CALL relgo_create_graph_index('CREATE SELF RAI knows_r ON knows (FROM k_person1id REFERENCES person.p_personid, TO k_person2id REFERENCES person.p_personid);');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI per_place ON person_islocatedin (FROM pi_personid REFERENCES person.p_personid, TO pi_placeid REFERENCES place.pl_placeid);');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI comm_per ON comment_hascreator (FROM chc_messageid REFERENCES comment.m_messageid, TO chc_personid REFERENCES person.p_personid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI comm_loc ON comment_islocatedin (FROM ci_messageid REFERENCES comment.m_messageid, TO ci_placeid REFERENCES place.pl_placeid)');

CALL relgo_create_graph_index('CREATE SELF RAI comm_comm ON comment_replyof_comment (FROM crc_message1id REFERENCES comment.m_messageid, TO crc_message2id REFERENCES comment.m_messageid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI comm_post ON comment_replyof_post (FROM crp_message1id REFERENCES comment.m_messageid, TO crp_message2id REFERENCES post.m_messageid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI contain ON containerof_post (FROM co_forumid REFERENCES forum.f_forumid, TO co_messageid REFERENCES post.m_messageid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI mod ON has_moderator (FROM hmod_forumid REFERENCES forum.f_forumid, TO hmod_personid REFERENCES person.p_personid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI post_per ON post_hascreator (FROM phc_messageid REFERENCES post.m_messageid, TO phc_personid REFERENCES person.p_personid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI post_loc ON post_islocatedin (FROM pi_messageid REFERENCES post.m_messageid, TO pi_placeid REFERENCES place.pl_placeid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI org_loc ON org_islocatedin (FROM oi_organisationid REFERENCES organisation.o_organisationid, TO oi_placeid REFERENCES place.pl_placeid)');

CALL relgo_create_graph_index('CREATE SELF RAI ipo ON ispartof (FROM ipo_place1id REFERENCES place.pl_placeid, TO ipo_place2id REFERENCES place.pl_placeid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI tag_type ON tag_hastype (FROM tht_tagid REFERENCES tag.t_tagid, TO tht_tagclassid REFERENCES tagclass.tc_tagclassid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI subclass ON issubclassof (FROM isc_tagclass1id REFERENCES tagclass.tc_tagclassid, TO isc_tagclass2id REFERENCES tagclass.tc_tagclassid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI tag_post ON post_tag (FROM mt_messageid REFERENCES post.m_messageid, TO mt_tagid REFERENCES tag.t_tagid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI likes_comment_r ON likes_comment (FROM l_personid REFERENCES person.p_personid, TO l_messageid REFERENCES comment.m_messageid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI likes_post_r ON likes_post (FROM l_personid REFERENCES person.p_personid, TO l_messageid REFERENCES post.m_messageid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI cmg_per ON person_company (FROM pc_personid REFERENCES person.p_personid, TO pc_organisationid REFERENCES organisation.o_organisationid);');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI per_frm ON forum_person (FROM fp_personid REFERENCES person.p_personid, TO fp_forumid REFERENCES forum.f_forumid)');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI comm_tag ON comment_tag (FROM mt_messageid REFERENCES comment.m_messageid, TO mt_tagid REFERENCES tag.t_tagid);');

CALL relgo_create_graph_index('CREATE UNDIRECTED RAI per_tag ON person_tag (FROM pt_personid REFERENCES person.p_personid, TO pt_tagid REFERENCES tag.t_tagid);');


CALL relgo_create_graph_index('CREATE UNDIRECTED RAI comm_message ON comment_replyof_message (FROM crm_message1id REFERENCES comment.m_messageid, TO crm_message2id REFERENCES message.m_messageid)');


CALL relgo_create_graph_index('CREATE UNDIRECTED RAI message_per ON message_hascreator (FROM mhc_messageid REFERENCES message.m_messageid, TO mhc_personid REFERENCES person.p_personid)');


CALL relgo_create_graph_index('CREATE UNDIRECTED RAI mess_tag ON message_tag (FROM mt_messageid REFERENCES message.m_messageid, TO mt_tagid REFERENCES tag.t_tagid);');


CALL relgo_create_graph_index('CREATE UNDIRECTED RAI message_loc ON message_islocatedin (FROM mi_messageid REFERENCES message.m_messageid, TO mi_placeid REFERENCES place.pl_placeid)');


CALL relgo_create_graph_index('CREATE UNDIRECTED RAI likes_message_r ON likes_message (FROM l_personid REFERENCES person.p_personid, TO l_messageid REFERENCES message.m_messageid)');