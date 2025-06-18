#ifndef PHYSICAL_SCAN_H
#define PHYSICAL_SCAN_H

#include "condition_tree.hpp"
#include "physical_operator.hpp"

#include <sstream>
#include <string>

namespace relgo {

enum class ScanOpt : uint8_t { VERTEX = 0, EDGE = 1, TABLE = 2, UNKNOWN = 3 };

class PhysicalScan : public PhysicalOperator {
public:
  PhysicalScan()
      : PhysicalOperator(PhysicalOperatorType::PHYSICAL_SCAN_TYPE),
        table_id("undefined"), table_alias(-1), table_name(""),
        is_all_columns(true), filter_conditions(nullptr),
        use_graph_index(false) {}
  PhysicalScan(std::string tid, std::string &tname, std::vector<int> &colids,
               std::vector<std::string> &colnames)
      : PhysicalOperator(PhysicalOperatorType::PHYSICAL_SCAN_TYPE),
        table_id(tid), table_name(tname), column_ids(colids),
        column_names(colnames), filter_conditions(nullptr),
        use_graph_index(false) {}
  ~PhysicalScan() {}

  std::string toString();

  void set_type(ScanOpt opt) { this->table_type = opt; }
  void set_id(std::string tid) { this->table_id = tid; }
  void set_alias(int talias) { this->table_alias = talias; }
  void set_table_name(std::string &name) { this->table_name = name; }
  void set_column_ids(std::vector<int> &colids) { this->column_ids = colids; }
  void set_column_names(std::vector<std::string> &colnames) {
    this->column_names = colnames;
  }
  void append_column(int col, std::string &colname, PhysicalType type) {
    this->column_ids.push_back(col);
    this->column_names.push_back(colname);
    this->column_types.push_back(type);
  }

public:
  ScanOpt table_type;
  std::string table_id;
  int table_alias;
  std::string table_name;
  std::vector<int> column_ids;
  std::vector<std::string> column_names;
  std::vector<PhysicalType> column_types;
  bool is_all_columns;
  std::shared_ptr<ConditionNode> filter_conditions;
  bool use_graph_index;
};

} // namespace relgo

#endif