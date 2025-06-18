#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../operators/physical_join.hpp"
#include "../proto_generated/physical.pb.h"
#include "../utils/types.hpp"

namespace relgo {

#define DEFAULT_TABLE_ALIAS 10000

class JoinInfo {
public:
  JoinInfo() {};
  JoinInfo(std::string _table_id, std::string _column_name, int _column_index)
      : table_id(_table_id), column_name(_column_name),
        column_index(_column_index) {}
  std::string table_id;
  std::string column_name;
  int column_index;
};

class JoinConditionPair {
public:
  JoinConditionPair()
      : data_type(PhysicalType::UNKNOWN), use_graph_index(false),
        join_direction(JoinDirection::UNKNOWN) {}
  std::unordered_map<std::string, JoinInfo> join_info;
  PhysicalType data_type;
  bool use_graph_index;
  JoinDirection join_direction;
  JoinIndexType join_index_type;
};

class GraphIndexMap {
public:
  GraphIndexMap() {}

  std::string createKey(const std::string &a, const std::string &b,
                        std::string bridge = "");
  //! two differnent tables can only be joined in one way
  std::shared_ptr<JoinConditionPair> getJoinPair(std::string &table_id1,
                                                 std::string &table_id2,
                                                 std::string join_bridge = "");
  bool hasJoinPair(std::string &table_id1, std::string &table_id2,
                   std::string join_bridge = "");
  void addJoinPair(std::string source_table_id, std::string source_column_name,
                   int source_column_index, std::string target_table_id,
                   std::string target_column_name, int target_column_index,
                   PhysicalType data_type, bool use_graph_index = false,
                   JoinDirection join_direction = JoinDirection::UNKNOWN,
                   JoinIndexType join_index_type = JoinIndexType::HASH_JOIN,
                   std::string join_bridge = "");

  std::unordered_map<std::string, std::shared_ptr<JoinConditionPair>>
      join_pairs;
};

class IndexColumn {
public:
  IndexColumn(std::string _column_name, int _column_index,
              PhysicalType _column_type)
      : column_name(_column_name), column_index(_column_index),
        column_type(_column_type) {}

  std::string column_name;
  int column_index;
  PhysicalType column_type;
};

class AliasInfo {
public:
  AliasInfo() {}
  AliasInfo(int _current_alias, std::vector<int> &_related_alias)
      : current_alias(_current_alias), related_alias(_related_alias) {}
  int current_alias;
  std::vector<int> related_alias;
};

class IndexManager {
public:
  IndexManager() : default_table_alias(DEFAULT_TABLE_ALIAS) {}
  IndexManager(std::string yaml_path)
      : default_table_alias(DEFAULT_TABLE_ALIAS) {}
  void initFromPb(const physical::PhysicalPlan &op);
  void analyzPhysicalOpr(const physical::PhysicalPlan &plan, int &op_idx);
  void addUsedIndexColumn(JoinConditionPair &pair, std::string table_id1,
                          int &alias1, std::string table_id2, int &alias2);

  std::unique_ptr<AliasInfo> getAlias(physical::PhysicalOpr_Operator &op);
  std::unique_ptr<AliasInfo> getAlias(const physical::Scan &op);
  std::unique_ptr<AliasInfo> getAlias(const physical::GetV &op);
  std::unique_ptr<AliasInfo> getAlias(const physical::EdgeExpand &op);
  std::unique_ptr<AliasInfo> getAlias(const physical::Intersect &op);
  std::unique_ptr<AliasInfo> getAlias();

  void getNecessaryProps(const algebra::QueryParams &param, int table_alias);
  void getNecessaryProps(const common::Expression &predicate);
  void getNecessaryProps(const physical::Scan &op, int table_alias);
  void getNecessaryProps(const physical::Project &op);
  void getNecessaryProps(const physical::GroupBy &op);
  void getNecessaryProps(const algebra::Select &op);

  GraphIndexMap graph_index_map;
  std::unordered_map<std::string,
                     std::unordered_map<int, std::vector<IndexColumn>>>
      used_index_columns;
  std::unordered_map<int, std::string> table_alias2id;
  std::unordered_map<int, std::unordered_set<std::string>> necessary_props;
  std::unordered_set<int> skippable_table;
  int default_table_alias;
  int last_table_alias;
};

} // namespace relgo

#endif