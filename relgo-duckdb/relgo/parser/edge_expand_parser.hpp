#ifndef EDGE_EXPAND_PARSER_H
#define EDGE_EXPAND_PARSER_H

#include "../operators/physical_adj_join.hpp"
#include "../operators/physical_hash_join.hpp"
#include "../operators/physical_join.hpp"
#include "../operators/physical_neighbor_join.hpp"
#include "../operators/physical_scan.hpp"
#include "parse_info.hpp"

namespace relgo {

class EdgeExpandParser {
public:
  EdgeExpandParser() {}

  static std::unique_ptr<PhysicalJoin> parseEdgeExpand(
      const physical::EdgeExpand &edge, const physical::PhysicalOpr &op,
      std::unique_ptr<PhysicalOperator> right_op, ParseInfo &parse_info);
  static std::unique_ptr<PhysicalJoin> parseEdgeExpand(
      const physical::EdgeExpand &edge, const physical::PhysicalOpr &op,
      const physical::GetV &next_op, std::unique_ptr<PhysicalOperator> right_op,
      ParseInfo &parse_info);
  static std::unique_ptr<PhysicalJoin> fillInJoinCondition(
      std::unique_ptr<PhysicalJoin> &join_op, std::string start_table_id,
      std::string start_table_name, int start_table_alias,
      std::unique_ptr<PhysicalScan> &scan_op, ParseInfo &parse_info,
      ParseInfo &other, std::string join_bridge = "");
  static std::unique_ptr<PhysicalJoin> fillInJoinConditionRev(
      std::unique_ptr<PhysicalJoin> &join_op, std::string start_table_id,
      std::string start_table_name, int start_table_alias,
      std::unique_ptr<PhysicalScan> &scan_op, ParseInfo &parse_info,
      ParseInfo &other, std::string join_bridge = "");
  static std::unique_ptr<PhysicalJoin>
  fillJoin(std::unique_ptr<PhysicalJoin> &join_op, std::string start_table_id,
           std::string start_table_name, int start_table_alias,
           std::unique_ptr<PhysicalScan> &scan_op, ParseInfo &parse_info,
           ParseInfo &other, std::unique_ptr<PhysicalOperator> right,
           bool scan_right = false, std::string join_bridge = "");
  static bool isScanRight(const physical::EdgeExpand &edge,
                          std::string &next_table_id, ParseInfo &parse_info);
};

} // namespace relgo

#endif