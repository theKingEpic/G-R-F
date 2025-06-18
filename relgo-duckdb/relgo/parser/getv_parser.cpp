#include "getv_parser.hpp"

#include "algebra_parser.hpp"
#include "common_parser.hpp"
#include "edge_expand_parser.hpp"
#include "scan_parser.hpp"

namespace relgo {

std::unique_ptr<PhysicalJoin>
GetVParser::parseGetV(const physical::GetV &vertex,
                      std::unique_ptr<PhysicalOperator> right_op,
                      ParseInfo &parse_info) {
  std::unique_ptr<PhysicalScan> scan_op =
      std::unique_ptr<PhysicalScan>(new PhysicalScan());
  std::unique_ptr<PhysicalJoin> join_op = nullptr;

  std::unique_ptr<AliasInfo> alias_info =
      parse_info.index_manager.getAlias(vertex);
  int start_table_alias = alias_info->related_alias[0];
  int vertex_alias = alias_info->current_alias;
  scan_op->set_alias(vertex_alias);

  std::string start_table_id =
      parse_info.index_manager.table_alias2id[start_table_alias];
  std::string start_table_name = parse_info.gs.table_id2name[start_table_id];

  ParseInfo other(parse_info.gs, parse_info.index_manager);

  scan_op = move(ScanParser::resolveScanParams(vertex.params(), move(scan_op),
                                               vertex_alias, other));

  std::shared_ptr<JoinConditionPair> join_condition_pair =
      other.index_manager.graph_index_map.getJoinPair(scan_op->table_id,
                                                      start_table_id);
  if (join_condition_pair->use_graph_index) {
    join_op = std::unique_ptr<PhysicalAdjJoin>(new PhysicalAdjJoin());
    join_op->join_direction = join_condition_pair->join_direction;
  } else {
    join_op = std::unique_ptr<PhysicalHashJoin>(new PhysicalHashJoin());
  }

  bool scan_right = isScanRight(vertex, scan_op->table_id, parse_info);
  join_op = EdgeExpandParser::fillJoin(
      join_op, start_table_id, start_table_name, start_table_alias, scan_op,
      parse_info, other, std::move(right_op), scan_right);

  return move(join_op);
}

bool GetVParser::isScanRight(const physical::GetV &vertex,
                             std::string &next_table_id,
                             ParseInfo &parse_info) {
  if (!parse_info.config.check_scan_right)
    return false;

  idx_t next_card = parse_info.gs.table_row_count[next_table_id];
  if (next_card < parse_info.getCard())
    return true;

  return false;
}

} // namespace relgo