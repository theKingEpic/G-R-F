#include "../../third_party/relgo/create_index/graph_index_builder.hpp"
#include "../../third_party/relgo/interface/op_transform.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>

/**
This file contains an example on how a query tree can be programmatically
constructed. This is essentially hand-rolling the binding+planning phase for one
specific query.

Note that this API is currently very unstable, and is subject to change at any
moment. In general, this API should not be used currently outside of internal
use cases.
**/

using namespace duckdb;
using namespace std;

const char *LDBC_INDEX[] = {
    "create index person_id on person(id);",
    "create index person_firstname on person(p_firstname);",
    "create index place_name on place(pl_name);",
    "create index comment_id on comment(id);"
    "create index tagclass_tcname on tagclass(tc_name);",
    "create index tag_name on tag(t_name);",
    "create index forum_id on forum(f_forumid)"};

// u1 - e1 - u2 - e2 - u3 - e3 - u4 - e4 - u5

class TreeNode {
public:
  TreeNode(int s, int e) : start(s), end(e) {
    lnode = NULL;
    rnode = NULL;
  }
  int start, end;
  TreeNode *lnode;
  TreeNode *rnode;

  ~TreeNode() {
    if (lnode) {
      delete lnode;
      lnode = NULL;
    }
    if (rnode) {
      delete rnode;
      rnode = NULL;
    }
  }
};

/* mode val: 0 - calcite, 1 - glogue*/
void build(TreeNode *node, std::vector<TreeNode *> &subtree, int mode) {
  if (node->start > node->end)
    return;
  else if (node->start == node->end) {
    subtree.push_back(node);
    return;
  } else if (node->start == node->end - 1) {
    TreeNode *tn1 = new TreeNode(node->start, node->start);
    TreeNode *tn2 = new TreeNode(node->end, node->end);

    TreeNode *new_node = new TreeNode(node->start, node->end);
    new_node->lnode = tn1;
    new_node->rnode = tn2;

    TreeNode *new_node2 = new TreeNode(node->start, node->end);
    new_node->lnode = tn2;
    new_node->rnode = tn1;

    subtree.push_back(new_node);
    subtree.push_back(new_node2);
    return;
  }

  int end_point = node->end;
  if (mode == 0) {
    end_point -= 1;
  }

  for (int cut_point = node->start; cut_point <= end_point; ++cut_point) {
    std::vector<TreeNode *> sub1, sub2;
    if (cut_point == node->start) {
      TreeNode *tn1 = new TreeNode(node->start, node->start);
      TreeNode *tn2 = new TreeNode(node->start + 1, node->end);
      build(tn1, sub1, mode);
      build(tn2, sub2, mode);
    } else if (cut_point == node->end) {
      TreeNode *tn1 = new TreeNode(node->end, node->end);
      TreeNode *tn2 = new TreeNode(node->start, node->end - 1);
      build(tn1, sub1, mode);
      build(tn2, sub2, mode);
    } else {
      if (mode == 0) {
        TreeNode *tn1 = new TreeNode(node->start, cut_point);
        TreeNode *tn2 = new TreeNode(cut_point + 1, node->end);
        build(tn1, sub1, mode);
        build(tn2, sub2, mode);
      } else if (mode == 1) {
        TreeNode *tn1 = new TreeNode(node->start, cut_point);
        TreeNode *tn2 = new TreeNode(cut_point, node->end);
        build(tn1, sub1, mode);
        build(tn2, sub2, mode);
      } else {
        std::cout << "Undefined Mode" << std::endl;
      }
    }

    for (size_t i = 0; i < sub1.size(); ++i) {
      for (size_t j = 0; j < sub2.size(); ++j) {
        TreeNode *new_node = new TreeNode(node->start, node->end);
        new_node->lnode = sub1[i];
        new_node->rnode = sub2[j];

        TreeNode *new_node2 = new TreeNode(node->start, node->end);
        new_node->lnode = sub2[j];
        new_node->rnode = sub1[i];

        subtree.push_back(new_node);
        subtree.push_back(new_node2);
      }
    }
  }
}

long long count(int start, int end, int mode,
                std::vector<long long> &recorder) {
  if (start > end)
    return 0;
  else if (start == end) {
    return 1;
  } else if (start == end - 1) {
    return 2;
  }

  if (recorder[end - start] != -1)
    return recorder[end - start];

  int end_point = end;
  if (mode == 0) {
    end_point -= 1;
  }

  long long sum = 0;
  for (int cut_point = start; cut_point <= end_point; ++cut_point) {
    long long left = 0;
    long long right = 0;
    if (cut_point == start) {
      left = count(start, start, mode, recorder);
      right = count(start + 1, end, mode, recorder);
    } else if (cut_point == end) {
      left = count(end, end, mode, recorder);
      right = count(start, end - 1, mode, recorder);
    } else {
      if (mode == 0) {
        left = count(start, cut_point, mode, recorder);
        right = count(cut_point + 1, end, mode, recorder);
      } else if (mode == 1) {
        left = count(start, cut_point, mode, recorder);
        right = count(cut_point, end, mode, recorder);
      } else {
        std::cout << "Undefined Mode" << std::endl;
      }
    }

    sum += left * right * 2;
  }

  recorder[end - start] = sum;
  return sum;
}

void run_count() {
  int length = 10;
  int mode = 0;
  std::vector<long long> recorder_calcite(1001, -1);

  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      start_calcite = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());

  long long result_calcite = count(0, length * 2, mode, recorder_calcite);

  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      end_calcite = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());
  std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(
                   end_calcite - start_calcite)
                   .count()
            << " ms " << std::endl;
  std::cout << result_calcite << std::endl;

  recorder_calcite.clear();

  std::vector<long long> recorder_glogue(1001, -1);
  mode = 1;
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      start_glogue = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());

  long long result_glogue = count(0, length, mode, recorder_glogue);

  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      end_glogue = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());
  std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(
                   end_glogue - start_glogue)
                   .count()
            << " ms " << std::endl;
  std::cout << result_glogue << std::endl;
}

void run_compare() {
  int length = 10;
  int mode = 0;
  TreeNode *root_calcite = new TreeNode(0, length * 2);
  std::vector<TreeNode *> subtree_calcite;

  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      start_calcite = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());

  build(root_calcite, subtree_calcite, mode);

  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      end_calcite = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());
  std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(
                   end_calcite - start_calcite)
                   .count()
            << " ms " << std::endl;
  std::cout << subtree_calcite.size() << std::endl;

  mode = 1;
  TreeNode *root_glogue = new TreeNode(0, length);
  std::vector<TreeNode *> subtree_glogue;

  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      start_glogue = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());

  build(root_glogue, subtree_glogue, mode);

  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      end_glogue = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());
  std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(
                   end_glogue - start_glogue)
                   .count()
            << " ms " << std::endl;
  std::cout << subtree_glogue.size() << std::endl;
}

void replace_all(std::string &str, const std::string &from,
                 const std::string &to) {
  if (from.empty())
    return;
  int start_pos = 0;
  while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length(); // In case 'to' contains 'from', like replacing
                              // 'x' with 'yx'
  }
}

void getStringListFromFile(string filename, int index, int count,
                           std::vector<string> &slist) {
  std::fstream infile(filename, std::ios::in);
  if (!infile)
    std::cout << filename << " not found" << std::endl;

  string schema, data;
  std::getline(infile, schema);
  std::cout << schema << std::endl;
  char delimiter = '|';

  unordered_set<string> record;

  string result = "";
  while (std::getline(infile, data)) {
    int pos = 0;
    int last = 0;
    int indexer = 0;

    while ((pos = data.find(delimiter, last)) != std::string::npos) {
      if (index == indexer) {
        string token = data.substr(last, pos - last);
        if (record.find(token) == record.end()) {
          slist.push_back(token);
          record.insert(token);
        }
        break;
      }
      indexer += 1;
      last = pos + 1;
    }
    if (index == indexer) {
      string token = data.substr(last, pos - last);
      if (record.find(token) == record.end()) {
        slist.push_back(token);
        record.insert(token);
      }
    }

    if (slist.size() >= count)
      break;
  }

  infile.close();
}

void extractInfo(string &inputstr, std::vector<bool> &filter, char delimiter,
                 string &result) {
  int pos = 0;
  int last = 0;
  int indexer = 0;
  std::string token;

  while ((pos = inputstr.find(delimiter, last)) != std::string::npos) {
    if (filter[indexer]) {
      token = inputstr.substr(last, pos - last);
      result += "\'" + token + "\'" + ',';
    }
    indexer += 1;
    last = pos + 1;
  }
  if (filter[indexer]) {
    result += "\'" + inputstr.substr(last) + "\'";
  }

  int lastindex = result.size() - 1;
  if (result[lastindex] == ',') {
    result = result.substr(0, lastindex);
  }
}

void extractInfoFile(Connection &con, string filename, string tablename,
                     std::vector<bool> &filter) {
  std::fstream infile(filename, std::ios::in);
  string schema, data;
  std::getline(infile, schema);

  string result = "";
  while (std::getline(infile, data)) {
    result.clear();
    extractInfo(data, filter, '|', result);
    con.Query("INSERT INTO " + tablename + " VALUES (" + result + ")");
    data.clear();
  }

  infile.close();
}

void CreateGraphFromSQL(Connection &con, string schema_path = "",
                        string load_path = "", string constraint_path = "") {
  std::ifstream schema_file(schema_path,
                            std::ios::in); //"../../../../schema.sql"
  if (schema_file) {
    std::stringstream buffer_schema;
    buffer_schema << schema_file.rdbuf();
    string schema_sql(buffer_schema.str());
    replace_all(schema_sql, "\n", " ");
    auto ps = con.Query(schema_sql);
    // std::cout << schema_sql << std::endl;
    ps->Print();
  }
  schema_file.close();
  std::cout << "finish load schema" << std::endl;

  std::ifstream load_file(load_path, std::ios::in);
  if (load_file) {
    std::stringstream buffer_load;
    buffer_load << load_file.rdbuf();
    string load_sql(buffer_load.str());
    replace_all(load_sql, "\n", "");
    auto ps = con.Query(load_sql);
    // std::cout << load_sql << std::endl;
    ps->Print();
  }
  load_file.close();
  std::cout << "finish load file" << std::endl;
  // auto result_thread = con.Query("select count(*) from knows;");
  // result_thread->Print();

  std::ifstream constraint_file(constraint_path, std::ios::in);
  if (constraint_file) {
    std::stringstream buffer_constraint;
    buffer_constraint << constraint_file.rdbuf();
    string constraint_sql(buffer_constraint.str());
    // replace_all(constraint_sql, "\n", "");
    // con.Query(constraint_sql);
  }
  constraint_file.close();
}

void CreateGraphFromFile(Connection &con) {
  std::vector<string> table_names{"Person",    "Forum",     "Post",
                                  "Knows",     "HasMember", "ContainerOf",
                                  "HasCreator"};
  for (int i = 0; i < table_names.size(); ++i) {
    con.Query("DROP TABLE " + table_names[i]);
  }

  con.Query("CREATE TABLE Person(id STRING)");
  con.Query("CREATE TABLE Forum(id STRING, title STRING)");
  con.Query("CREATE TABLE Post(id STRING)");
  con.Query("CREATE TABLE Knows(id1 STRING, id2 STRING)");
  con.Query("CREATE TABLE HasMember(forumId STRING, personId STRING)");
  con.Query("CREATE TABLE ContainerOf(forumId STRING, postId STRING)");
  con.Query("CREATE TABLE HasCreator(postId STRING, personId STRING)");

  // string prepath = "/Users/louyk/Desktop/dbs/duckdb/resource/sample/";
  string prepath = "../../../../dataset/ldbc/sf1/";

  std::vector<bool> filter_person{true,  false, false, false, false,
                                  false, false, false, false, false};
  extractInfoFile(con, prepath + "person_0_0.csv", "Person", filter_person);

  std::vector<bool> filter_forum{true, true, false};
  extractInfoFile(con, prepath + "forum_0_0.csv", "Forum", filter_forum);

  std::vector<bool> filter_post{true,  false, false, false,
                                false, false, false, false};
  extractInfoFile(con, prepath + "post_0_0.csv", "Post", filter_post);

  std::vector<bool> filter_knows{true, true, false};
  extractInfoFile(con, prepath + "person_knows_person_0_0.csv", "Knows",
                  filter_knows);

  std::vector<bool> filter_hasmember{true, true, false};
  extractInfoFile(con, prepath + "forum_hasMember_person_0_0.csv", "HasMember",
                  filter_hasmember);

  std::vector<bool> filter_containerof{true, true};
  extractInfoFile(con, prepath + "forum_containerOf_post_0_0.csv",
                  "ContainerOf", filter_containerof);

  std::vector<bool> filter_hascreator{true, true};
  extractInfoFile(con, prepath + "post_hasCreator_person_0_0.csv", "HasCreator",
                  filter_hascreator);
}

void CreateGraph(Connection &con) {
  std::vector<string> table_names{"Person",    "Forum",     "Post",
                                  "Knows",     "HasMember", "ContainerOf",
                                  "HasCreator"};
  for (size_t i = 0; i < table_names.size(); ++i) {
    con.Query("DROP TABLE " + table_names[i]);
  }

  con.Query("CREATE TABLE Person(id STRING)");
  con.Query("CREATE TABLE Forum(id STRING, title STRING)");
  con.Query("CREATE TABLE Post(id STRING)");
  con.Query("CREATE TABLE Knows(id1 STRING, id2 STRING)");
  con.Query("CREATE TABLE HasMember(forumId STRING, personId STRING)");
  con.Query("CREATE TABLE ContainerOf(forumId STRING, postId STRING)");
  con.Query("CREATE TABLE HasCreator(postId STRING, personId STRING)");

  int S_NUM = 3;
  for (int i = 0; i < S_NUM; ++i) {
    con.Query("INSERT INTO Person VALUES (\'" + to_string(i) + "\')");
    con.Query("INSERT INTO Forum VALUES (\'" + to_string(i) + "\',\'" +
              to_string(i) + "\')");
    con.Query("INSERT INTO Post VALUES (\'" + to_string(i) + "\')");
    con.Query("INSERT INTO HasMember VALUES (\'" + to_string(i) + "\',\'" +
              to_string(i) + "\')");
    con.Query("INSERT INTO ContainerOf VALUES (\'" + to_string(i) + "\',\'" +
              to_string(i) + "\')");
    con.Query("INSERT INTO HasCreator VALUES (\'" + to_string(i) + "\',\'" +
              to_string(i) + "\')");
  }

  for (int i = 0; i < S_NUM; ++i) {
    for (int j = 0; j < S_NUM; ++j) {
      if (i == j)
        continue;
      con.Query("INSERT INTO Knows VALUES (\'" + to_string(i) + "\',\'" +
                to_string(j) + "\')");
    }
  }
}

const char *LDBC_SIMPLIFIED_MERGE_RAIS[] = {
    "CREATE SELF RAI knows_r ON knows (FROM k_person1id REFERENCES "
    "person.p_personid, TO k_person2id REFERENCES person.p_personid);",
    "CREATE UNDIRECTED RAI per_place ON person_islocatedin (FROM pi_personid "
    "REFERENCES person.p_personid, TO pi_placeid REFERENCES "
    "place.pl_placeid);",
    "CREATE UNDIRECTED RAI comm_per ON comment_hascreator (FROM chc_messageid "
    "REFERENCES comment.m_messageid, TO chc_personid REFERENCES "
    "person.p_personid)",
    "CREATE UNDIRECTED RAI comm_loc ON comment_islocatedin (FROM ci_messageid "
    "REFERENCES comment.m_messageid, TO ci_placeid REFERENCES "
    "place.pl_placeid)",
    "CREATE SELF RAI comm_comm ON comment_replyof_comment (FROM "
    "crc_message1id "
    "REFERENCES comment.m_messageid, TO crc_message2id REFERENCES "
    "comment.m_messageid)",
    "CREATE UNDIRECTED RAI comm_post ON comment_replyof_post (FROM "
    "crp_message1id "
    "REFERENCES comment.m_messageid, TO crp_message2id REFERENCES "
    "post.m_messageid)",
    "CREATE UNDIRECTED RAI contain ON containerof_post (FROM "
    "co_forumid "
    "REFERENCES forum.f_forumid, TO co_messageid REFERENCES "
    "post.m_messageid)",
    "CREATE UNDIRECTED RAI mod ON has_moderator (FROM "
    "hmod_forumid "
    "REFERENCES forum.f_forumid, TO hmod_personid REFERENCES "
    "person.p_personid)",
    "CREATE UNDIRECTED RAI post_per ON post_hascreator (FROM phc_messageid "
    "REFERENCES post.m_messageid, TO phc_personid REFERENCES "
    "person.p_personid)",
    "CREATE UNDIRECTED RAI post_loc ON post_islocatedin (FROM pi_messageid "
    "REFERENCES post.m_messageid, TO pi_placeid REFERENCES "
    "place.pl_placeid)",
    "CREATE UNDIRECTED RAI org_loc ON org_islocatedin (FROM oi_organisationid "
    "REFERENCES organisation.o_organisationid, TO oi_placeid REFERENCES "
    "place.pl_placeid)",
    "CREATE UNDIRECTED RAI ipo ON ispartof (FROM ipo_place1id "
    "REFERENCES place.pl_placeid, TO ipo_place2id REFERENCES "
    "place.pl_placeid)",
    "CREATE UNDIRECTED RAI tag_type ON tag_hastype (FROM tht_tagid "
    "REFERENCES tag.t_tagid, TO tht_tagclassid REFERENCES "
    "tagclass.tc_tagclassid)",
    "CREATE UNDIRECTED RAI subclass ON issubclassof (FROM isc_tagclass1id "
    "REFERENCES tagclass.tc_tagclassid, TO isc_tagclass2id REFERENCES "
    "tagclass.tc_tagclassid)",
    "CREATE UNDIRECTED RAI tag_post ON post_tag (FROM mt_messageid REFERENCES "
    "post.m_messageid, TO mt_tagid REFERENCES tag.t_tagid)",
    "CREATE UNDIRECTED RAI likes_comment_r ON likes_comment (FROM l_personid "
    "REFERENCES person.p_personid, TO l_messageid REFERENCES "
    "comment.m_messageid)",
    "CREATE UNDIRECTED RAI cmg_per ON person_company (FROM pc_personid "
    "REFERENCES person.p_personid, TO pc_organisationid REFERENCES "
    "organisation.o_organisationid);",
    "CREATE UNDIRECTED RAI per_frm ON forum_person (FROM fp_personid "
    "REFERENCES person.p_personid, TO fp_forumid REFERENCES "
    "forum.f_forumid)"};

const char *LDBC_SIMPLIFIED_RAIS[] = {
    "CREATE SELF RAI knows_r ON knows (FROM k_person1id REFERENCES "
    "person.p_personid, TO k_person2id REFERENCES person.p_personid);",
    "CREATE UNDIRECTED RAI per_frm ON forum_person (FROM fp_personid "
    "REFERENCES person.p_personid, TO fp_forumid REFERENCES "
    "forum.f_forumid);"};
// "CREATE PKFK RAI orgIsLocatedIn ON organisation (FROM o_placeid
// REFERENCES " "place.pl_placeid, TO o_placeid REFERENCES
// place.pl_placeid);", "CREATE PKFK RAI commentHasCreator ON comment (FROM
// m_creatorid REFERENCES " "person.p_personid, TO m_creatorid REFERENCES
// person.p_personid);", "CREATE PKFK RAI postHasCreator ON post (FROM
// m_creatorid REFERENCES " "person.p_personid, TO m_creatorid REFERENCES
// person.p_personid);", "CREATE PKFK RAI commentIsLocatedIn ON comment
// (FROM m_locationid " "REFERENCES place.pl_placeid, TO m_locationid
// REFERENCES " "place.pl_placeid);", "CREATE PKFK RAI postIsLocatedIn ON
// post (FROM m_locationid REFERENCES " "place.pl_placeid, TO m_locationid
// REFERENCES place.pl_placeid);", "CREATE PKFK RAI replyOfComment ON
// comment (FROM m_replyof_comment " "REFERENCES comment.m_messageid, TO
// m_replyof_comment REFERENCES " "comment.m_messageid);", "CREATE PKFK RAI
// replyOfPost ON comment (FROM m_replyof_post REFERENCES "
// "post.m_messageid, TO m_replyof_post REFERENCES post.m_messageid);",
// "CREATE PKFK RAI personIsLocatedIn ON person (FROM p_placeid REFERENCES
// " "place.pl_placeid, TO p_placeid REFERENCES place.pl_placeid);",
// "CREATE PKFK RAI postIsPublishedIn ON post (FROM m_ps_forumid REFERENCES
// " "forum.f_forumid, TO m_ps_forumid REFERENCES forum.f_forumid);",
// "CREATE PKFK RAI forumHasModerator ON forum (FROM f_moderatorid
// REFERENCES " "person.p_personid, TO f_moderatorid REFERENCES
// person.p_personid);", "CREATE PKFK RAI tagType ON tag (FROM t_tagclassid
// REFERENCES " "tagclass.tc_tagclassid, TO t_tagclassid REFERENCES "
// "tagclass.tc_tagclassid);",
// "CREATE PKFK RAI tagClassSub ON tagclass (FROM tc_subclassoftagclassid "
// "REFERENCES tagclass.tc_tagclassid, TO tc_subclassoftagclassid
// REFERENCES " "tagclass.tc_tagclassid);", "CREATE UNDIRECTED RAI per_frm
// ON forum_person (FROM fp_personid " "REFERENCES person.p_personid, TO
// fp_forumid REFERENCES forum.f_forumid);", "CREATE UNDIRECTED RAI tag_frm
// ON forum_tag (FROM ft_forumid REFERENCES " "forum.f_forumid, TO ft_tagid
// REFERENCES tag.t_tagid);", "CREATE UNDIRECTED RAI tag_per ON person_tag
// (FROM pt_personid REFERENCES " "person.p_personid, TO pt_tagid
// REFERENCES tag.t_tagid);", "CREATE SELF RAI knows_r ON knows (FROM
// k_person1id REFERENCES " "person.p_personid, TO k_person2id REFERENCES
// person.p_personid);", "CREATE UNDIRECTED RAI likes_comment_r ON
// likes_comment (FROM l_personid " "REFERENCES person.p_personid, TO
// l_messageid REFERENCES " "comment.m_messageid);", "CREATE UNDIRECTED RAI
// likes_post_r ON likes_post (FROM l_personid " "REFERENCES
// person.p_personid, TO l_messageid REFERENCES " "post.m_messageid);",
// "CREATE UNDIRECTED RAI tag_comment ON comment_tag (FROM mt_messageid "
// "REFERENCES comment.m_messageid, TO mt_tagid REFERENCES tag.t_tagid);",
// "CREATE UNDIRECTED RAI tag_post ON post_tag (FROM mt_messageid
// REFERENCES " "post.m_messageid, TO mt_tagid REFERENCES tag.t_tagid);",
// "CREATE UNDIRECTED RAI cmg_per ON person_company (FROM pc_personid "
// "REFERENCES person.p_personid, TO pc_organisationid REFERENCES "
// "organisation.o_organisationid);",
// "CREATE UNDIRECTED RAI unive_per ON person_university (FROM "
// "pu_organisationid REFERENCES organisation.o_organisationid, TO "
// "pu_personid REFERENCES person.p_personid);"};

const char *IMDB_RAIS[] = {
    "CREATE UNDIRECTED RAI it_midx_t ON movie_info_idx (FROM info_type_id "
    "REFERENCES info_type.id, TO movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI cn_mc_t ON movie_companies (FROM company_id "
    "REFERENCES company_name.id, TO movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI ct_mc_t ON movie_companies (FROM "
    "company_type_id "
    "REFERENCES company_type.id, TO movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI k_mk_t ON movie_keyword (FROM keyword_id "
    "REFERENCES "
    "keyword.id, TO movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI it_mi_t ON movie_info (FROM info_type_id "
    "REFERENCES "
    "info_type.id, TO movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI lt_ml_t ON movie_link (FROM link_type_id "
    "REFERENCES "
    "link_type.id, TO movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI lt_ml_t_link ON movie_link (FROM link_type_id "
    "REFERENCES link_type.id, TO linked_movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI it_pi_n ON person_info (FROM info_type_id "
    "REFERENCES info_type.id, TO person_id REFERENCES name.id);",
    "CREATE UNDIRECTED RAI t_ci_n ON cast_info (FROM movie_id REFERENCES "
    "title.id, TO person_id REFERENCES name.id);",
    "CREATE UNDIRECTED RAI rt_ci_n ON cast_info (FROM role_id REFERENCES "
    "role_type.id, TO person_id REFERENCES name.id);",
    "CREATE UNDIRECTED RAI rt_ci_t ON cast_info (FROM role_id REFERENCES "
    "role_type.id, TO movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI cct_cc_t_subject ON complete_cast (FROM "
    "subject_id "
    "REFERENCES comp_cast_type.id, TO movie_id REFERENCES title.id);",
    "CREATE UNDIRECTED RAI cct_cc_t_status ON complete_cast (FROM "
    "status_id "
    "REFERENCES comp_cast_type.id, TO movie_id REFERENCES title.id);",
    "CREATE PKFK RAI n_an ON aka_name (FROM person_id REFERENCES name.id, "
    "TO "
    "person_id REFERENCES name.id);",
    "CREATE PKFK RAI kt_t ON title (FROM kind_id REFERENCES kind_type.id, "
    "TO "
    "kind_id REFERENCES kind_type.id);",
    "CREATE PKFK RAI at_t ON aka_title (FROM movie_id REFERENCES "
    "title.id, TO "
    "movie_id REFERENCES title.id);",
    "CREATE PKFK RAI chn_ci ON cast_info (FROM person_role_id REFERENCES "
    "char_name.id, TO person_role_id REFERENCES char_name.id);"};

void buildIndex(Connection &con) {
  for (auto &rai_stmt : LDBC_SIMPLIFIED_RAIS) {
    auto result = con.Query(rai_stmt);
    std::cout << rai_stmt << std::endl;
  }
}

void buildMergeIndex(Connection &con) {
  con.context->SetOptimizeMode(OptimizeMode::OPTIMIZE_WITH_GOPT);
  for (auto &rai_stmt : LDBC_SIMPLIFIED_MERGE_RAIS) {
    std::string input_stmt = "#" + std::string(rai_stmt);
    auto result = con.Query(input_stmt);
  }
  con.context->SetOptimizeMode(OptimizeMode::ORIGINAL);
}

void buildFullIndex(Connection &con) {
  for (auto &rai_stmt : LDBC_SIMPLIFIED_RAIS) {
    auto result = con.Query(rai_stmt);
  }
}

void buildImdbIndex(Connection &con) {
  for (auto &rai_stmt : IMDB_RAIS) {
    std::cout << rai_stmt << std::endl;
    auto result = con.Query(rai_stmt);
  }
}

void create_db_conn(DuckDB &db, Connection &con) {

  buildMergeIndex(con);
  // buildIndex(con);
  // buildImdbIndex(con);
}

void generate_queries(string query_path, string para_path,
                      std::vector<string> &generated_queries) {
  std::ifstream para_file(para_path, std::ios::in);

  string schema, type, data;
  std::getline(para_file, schema);
  std::cout << schema << std::endl;
  std::getline(para_file, type);
  char delimiter = '|';
  std::vector<string> slots;
  std::vector<string> data_types;

  schema += delimiter;
  string cur = "";
  for (int i = 0; i < schema.size(); ++i) {
    if (schema[i] == delimiter) {
      cur = "$" + cur;
      slots.push_back(cur);
      cur.clear();
    } else {
      cur += schema[i];
    }
  }

  type += delimiter;
  cur.clear();
  for (int i = 0; i < type.size(); ++i) {
    if (type[i] == delimiter) {
      data_types.push_back(cur);
      cur.clear();
    } else {
      cur += type[i];
    }
  }

  std::ifstream query_file(query_path, std::ios::in);
  std::stringstream buffer;
  buffer << query_file.rdbuf();
  string query_template(buffer.str());
  replace_all(query_template, "\n", " ");

  while (std::getline(para_file, data)) {
    int pos = 0;
    int last = 0;
    int indexer = 0;
    data += "|";
    std::vector<string> tokenizers;

    string query_template_tmp(query_template);
    while ((pos = data.find(delimiter, last)) != std::string::npos) {
      string token = data.substr(last, pos - last);
      tokenizers.push_back(token);
      if (data_types[indexer] == "VARCHAR")
        token = "\'" + token + "\'";
      if (data_types[indexer] == "INTEGER")
        token = token + "L";
      if (slots[indexer] == "$durationDays") {
        long long dur = atoll(token.c_str()) * 86400000;
        long long end_time = atoll(tokenizers[indexer - 1].c_str()) + dur;
        token = to_string(end_time);
      }
      replace_all(query_template_tmp, slots[indexer], token);
      indexer += 1;
      last = pos + 1;
    }

    generated_queries.push_back(query_template_tmp);
  }

  if (generated_queries.empty()) {
    generated_queries.push_back(query_template);
  }
}

void generate_imdb_queries(string query_path,
                           std::vector<string> &generated_queries) {
  std::ifstream query_file(query_path, std::ios::in);
  std::stringstream buffer;
  buffer << query_file.rdbuf();
  string query_template(buffer.str());
  replace_all(query_template, "\n", " ");

  generated_queries.push_back(query_template);
}

int main(int argc, char **args) {
  int count_num = 50;
  int mode = 2;              // atoi(args[1]);
  int start_query_index = 1; // atoi(args[2]);
  int end_query_index = 2;   // atoi(args[3]);
  // string dataset(args[4]);

  // string suffix(args[5]);
  // vector<string> constantval_list;
  // getStringListFromFile("../../../../dataset/ldbc/sf1/person_0_0.csv", 0,
  // count_num, constantval_list); constantval_list.push_back("4398046511870");

  // string query_index = "2";
  // vector<string> generated_queries;
  // string query_path =
  // "../../../../dataset/ldbc-merge/query/queries/interactive-complex-" +
  // query_index + ".sql"; string para_path =
  // "../../../../dataset/ldbc-merge/query/paras/generated_version/sf01/interactive_"
  // + query_index + "_param.txt"; generate_queries(query_path, para_path,
  // generated_queries);

  // std::cout << "Generate Queries Over" << std::endl;

  string schema_path =
      "../../../third_party/relgo/resource/schema_ldbc_merge.sql";
  string load_path =
      "../../../third_party/relgo/resource/load_ldbc01_merge.sql";
  string constraint_path = "";

  // string schema_path = "../../../resource/schema_imdb.sql";
  // string load_path = "../../../resource/load_imdb.sql";

  // string schema_path = "../../../../dataset/ldbc-merge/schema.sql";
  // string load_path = "../../../../dataset/ldbc-merge/load.sql";

  DuckDB db(nullptr);
  Connection con(db);

  con.context->transaction.BeginTransaction();
  con.context->transaction.SetAutoCommit(false);
  std::cout << "SET THREADS TO 1" << std::endl;
  auto result_thread = con.Query("SET threads TO 1;");
  result_thread->Print();
  con.context->transaction.Commit();

  con.DisableProfiling();
  con.context->transaction.BeginTransaction();
  con.context->transaction.SetAutoCommit(false);
  CreateGraphFromSQL(con, schema_path, load_path);
  con.context->transaction.Commit();

  // auto resu = con.Query(
  //     "SELECT constraint_type, constraint_text FROM duckdb_constraints();");
  // resu->Print();

  // return 0;

  con.context->transaction.BeginTransaction();
  con.context->transaction.SetAutoCommit(false);
  create_db_conn(db, con);
  con.context->transaction.Commit();

  std::cout << "FINISH BUILDING INDEX" << std::endl;

  duckdb::unique_ptr<OpTransform> op = make_uniq<OpTransform>(con);
  op->init();
  con.context->SetOpTransform(move(op));

  string suffix_str = "";
  // if (suffix != "-1")
  //    suffix_str += "_" + suffix;

  /*std::fstream outfile("output.txt", std::ios::out);
  for (int i = 0; i < generated_queries.size(); ++i)
      outfile << generated_queries[i] << std::endl << std::endl;
  outfile.close();*/
  // for (int query_index = start_query_index; query_index < end_query_index;
  //      ++query_index) {
  while (true) {
    // if (query_index == 10)
    //    continue;
    int query_index = start_query_index;
    std::string query_no = "";
    std::cout << "set query: ";
    std::cin >> query_no;

    if (query_no == "break")
      break;

    query_index = 1;
    string query_index_str = to_string(query_index);
    std::vector<string> generated_queries;
    // string query_path =
    //     "/home/graphscope/gopt/relgo-artifact2/relgo/outer_resource/"
    //     "resource/queries/ldbc_merge/interactive-complex-" +
    //     query_no + ".sql";
    string query_path = "../../../third_party/relgo/resource/queries/sqlpgq/"
                        "ldbc/interactive-complex-" +
                        query_no + ".sql";
    string para_path =
        "../../../third_party/relgo/resource/paras/ldbc01/interactive_" +
        query_no + "_param.txt";
    // string query_path =
    // "../../../../dataset/ldbc-merge/query/queries/interactive-complex-" +
    // query_index_str + ".sql"; string para_path =
    // "../../../../dataset/ldbc-merge/query/paras/generated_version/" + dataset
    // + "/interactive_" + query_index_str + "_param.txt";
    // string query_path =
    //     "../../../resource/imdb_query/" + query_index_str + "a.sql";
    generate_queries(query_path, para_path, generated_queries);
    // generate_imdb_queries(query_path, generated_queries);
    // std::cout << generated_queries[0] << std::endl;

    /*std::fstream outfile("output.txt", std::ios::out);
    for (int i = 0; i < generated_queries.size(); ++i)
        outfile << generated_queries[i] << std::endl << std::endl;
    outfile.close();
*/

    long long time_cost = 0;
    long long result_num = 0;
    generated_queries.push_back(generated_queries[0]);

    for (int i = 0; i < generated_queries.size(); ++i) {
      // con.context->transaction.SetAutoCommit(false);
      // con.context->transaction.BeginTransaction();
      duckdb::unique_ptr<std::vector<string>> parameters =
          duckdb::make_uniq<std::vector<string>>();

      std::cout << "the query is " << generated_queries[i] << std::endl;
      parameters->push_back("933");
      parameters->push_back("Karl");
      parameters->push_back("1311326793031");
      parameters->push_back("Niger");
      parameters->push_back("Niger");

      //   con.context->SetPbParameters(mode, "job1a", move(parameters));
      // con.context->SetPbParameters(mode, "job17a", move(parameters));
      con.context->SetOptimizeMode(duckdb::OptimizeMode::OPTIMIZE_WITH_GOPT);
      // con.context->SetPbParameters(mode, "1-1", move(parameters));
      // con.context->SetPbParameters(mode, "../../../../output/" + dataset +
      // suffix_str + "/graindb/query" + query_index_str + "." + to_string(i));
      /*auto r1 = con.Query("select string_agg(o2.o_name || '|' ||
      pu_classyear::text || '|' || p2.pl_name, ';')\n" "     from
      person_university, organisation o2, place p2\n" "    where pu_personid =
      6597069767674 and pu_organisationid = o2.o_organisationid and o2.o_placeid
      =p2.pl_placeid\n" "    group by pu_personid"); r1->Print(); break;*/

      // auto result0 = con.Query("select * from knows;");
      // result0->Print();

      // auto result = con.Query(generated_queries[i]);
      // std::cout << "======================== start query
      // ====================" << std::endl;
      std::chrono::time_point<std::chrono::system_clock,
                              std::chrono::milliseconds>
          start = std::chrono::time_point_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now());

      // std::cout << generated_queries[i] << std::endl;
      // auto result = con.Query("select 1,'1','1','1',1,1,1,'1',1 from name");
      auto result = con.Query(generated_queries[i]);
      // auto result = con.Query("select p_personid from person where p_personid
      // < 100;");
      // auto result =
      //     con.Query("SELECT p1.p_personid FROM GRAPH_TABLE (graph MATCH "
      //               "(p1:person)-[:knows]-(p3:person)-[:"
      //               "forum_person]->(:forum) WHERE "
      //               "p3.p_personid = 933L COLUMNS "
      //               "(p1.p_personid as p1_id)) g;");
      // auto result = con.Query(
      //     "SELECT p1.p_personid AS p_personid FROM "
      //     "person p1, knows k1,  person p3, forum_person "
      //     "fp, forum f WHERE p1.p_personid = k1.k_person1id AND "
      //     "k1.k_person2id = p3.p_personid  "
      //     "AND fp.fp_personid = "
      //     "p3.p_personid AND fp.fp_forumid = f.f_forumid AND p3.p_personid =
      //     " "933;");
      // auto result = con.Query(get_test_query(0));
      std::chrono::time_point<std::chrono::system_clock,
                              std::chrono::milliseconds>
          end = std::chrono::time_point_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now());
      long long time_cost_this_time =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
              .count();
      std::cout << endl
                << time_cost_this_time << " ms " << result->RowCount()
                << std::endl;
      // result->Print();
      // con.QueryPb(generated_queries[i]);
      /*con.QueryPb("SELECT f.title FROM "
                  "Knows k1, Person p2, HasMember hm, Forum f, ContainerOf cof,
         Post po, HasCreator hc " "WHERE p2.id = k1.id2 AND p2.id = hm.personId
         AND f.id = hm.forumId AND f.id = cof.forumId AND " "po.id = cof.postId
         AND po.id = hc.postId AND p2.id = hc.personId AND k1.id1 = \'"
                  + constantval_list[i] + "\'");
      */
      // con.context->transaction.Commit();
      /*fstream outfile("output2.txt", std::ios::out);
      outfile << result->ToString() << std::endl;
      outfile.close();*/
      result->Print();
      if (i != 0) {
        time_cost += time_cost_this_time;
        result_num += result->RowCount();

        std::cout << result->RowCount() << std::endl;
      }
    }

    std::cout << "TIME COST: "
              << time_cost / ((double)generated_queries.size() - 1)
              << std::endl;
    std::cout << "RESULT NUM: " << result_num << std::endl;

    // int64_t total_execution_time = 0;
    // int start_tc = 4;
    // for (size_t tc = start_tc; tc < con.context->execution_query.size();
    // ++tc) { 	std::cout << con.context->execution_query[tc] << std::endl;
    // }

    // for (size_t tc = start_tc; tc < con.context->total_execution_time.size();
    // ++tc) { 	std::cout << con.context->total_execution_time[tc] << " ";
    // 	total_execution_time += con.context->total_execution_time[tc];
    // }
    // std::cout << "AVERAGE EXECUTION TIME COST: "
    //           << total_execution_time /
    //           ((double)con.context->total_execution_time.size() - start_tc)
    //           << std::endl;
  }

  return 0;
}