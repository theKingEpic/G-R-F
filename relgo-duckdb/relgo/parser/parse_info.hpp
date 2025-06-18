#ifndef PARSE_INFO_H
#define PARSE_INFO_H

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "../graph/graph_schema.hpp"
#include "../index/graph_index_manager.hpp"

namespace relgo {

class ParseConfig {
public:
  ParseConfig(bool _check_scan_right = true)
      : check_scan_right(_check_scan_right) {}

public:
  bool check_scan_right;
};

class ParseInfo {
public:
  ParseInfo() {}
  ParseInfo(GraphSchema &_gs, IndexManager &_index_manager)
      : gs(_gs), index_manager(_index_manager), parse_info_index(0),
        inner_index_prefix("PI_"), current_card(0) {
    index_manager.default_table_alias = DEFAULT_TABLE_ALIAS;
    index_manager.last_table_alias = -1;
  }

  ParseInfo(ParseInfo &other)
      : current_table_schema(other.current_table_schema),
        current_table_id(other.current_table_id), gs(other.gs),
        index_manager(other.index_manager),
        parse_info_index(other.parse_info_index),
        inner_index_prefix(other.inner_index_prefix), config(other.config) {}

  SchemaElement current_table_schema;
  GraphSchema gs;
  IndexManager index_manager;
  std::string current_table_id;
  idx_t current_card;

  int parse_info_index;
  std::string inner_index_prefix;
  std::unordered_map<std::string, SchemaElement> checkpoints;

  ParseConfig config;

  void setCurrentTableSchema(std::string table_id, int table_alias);
  void join(ParseInfo &other);
  void join_front(ParseInfo &other);
  void project(std::vector<int> &cols);
  void project(std::vector<std::string> &col_names);
  void project(std::vector<std::string> &col_names,
               std::vector<int> &col_alias);
  void checkpoint();
  bool isInternalTable(std::string &table_id);
  void appendIndex(int alias);
  void setCard(idx_t _card);
  idx_t getCard();

  std::string toString();
};

} // namespace relgo

#endif