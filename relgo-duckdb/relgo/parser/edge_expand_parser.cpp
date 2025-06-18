#include "edge_expand_parser.hpp"

#include "algebra_parser.hpp"
#include "common_parser.hpp"
#include "scan_parser.hpp"

namespace relgo {

std::unique_ptr<PhysicalJoin> EdgeExpandParser::parseEdgeExpand(
    const physical::EdgeExpand &edge, const physical::PhysicalOpr &op,
    std::unique_ptr<PhysicalOperator> right_op, ParseInfo &parse_info) {
  physical::EdgeExpand_ExpandOpt expand_opt = edge.expand_opt();
  physical::EdgeExpand_Direction expand_direction = edge.direction();

  bool has_predicate = false;
  bool is_optional = edge.is_optional();
  if (edge.params().has_predicate())
    has_predicate = true;

  std::unique_ptr<AliasInfo> alias_info =
      parse_info.index_manager.getAlias(edge);
  int start_table_alias = alias_info->related_alias[0];

  std::string start_table_id =
      parse_info.index_manager.table_alias2id[start_table_alias];
  std::string start_table_name = parse_info.gs.table_id2name[start_table_id];

  std::unique_ptr<PhysicalScan> scan_op = nullptr;
  std::unique_ptr<PhysicalJoin> join_op = nullptr;
  ParseInfo other(parse_info.gs, parse_info.index_manager);

  std::string source_table_id = std::to_string(op.meta_data(0)
                                                   .type()
                                                   .graph_type()
                                                   .graph_data_type(0)
                                                   .label()
                                                   .src_label()
                                                   .value());
  std::string target_table_id = std::to_string(op.meta_data(0)
                                                   .type()
                                                   .graph_type()
                                                   .graph_data_type(0)
                                                   .label()
                                                   .dst_label()
                                                   .value());

  if (expand_opt == physical::EdgeExpand_ExpandOpt_EDGE) {
    int edge_alias = alias_info->current_alias;

    scan_op = std::unique_ptr<PhysicalScan>(new PhysicalScan());
    scan_op->set_alias(edge_alias);

    scan_op = move(ScanParser::resolveScanParams(edge.params(), move(scan_op),
                                                 edge_alias, other));

    std::shared_ptr<JoinConditionPair> join_pair =
        parse_info.index_manager.graph_index_map.getJoinPair(scan_op->table_id,
                                                             start_table_id);
    if (join_pair) {
      if (join_pair->use_graph_index && !is_optional)
        join_op = std::unique_ptr<PhysicalAdjJoin>(new PhysicalAdjJoin());
      else {
        if (is_optional)
          join_op = std::unique_ptr<PhysicalHashJoin>(
              new PhysicalHashJoin(HashJoinType::LEFT_JOIN));
        else
          join_op = std::unique_ptr<PhysicalHashJoin>(
              new PhysicalHashJoin(HashJoinType::INNER_JOIN));
      }
    } else {
      std::cerr << "cannot join " << scan_op->table_id << " and "
                << start_table_id << std::endl;
    }

    bool scan_right = isScanRight(edge, scan_op->table_id, parse_info);
    join_op =
        fillJoin(join_op, start_table_id, start_table_name, start_table_alias,
                 scan_op, parse_info, other, std::move(right_op), scan_right);

  } else {
    std::unique_ptr<AliasInfo> target_alias_info =
        parse_info.index_manager.getAlias();
    int edge_alias = target_alias_info->current_alias;
    std::string edge_table_id =
        parse_info.index_manager.table_alias2id[edge_alias];
    std::string edge_table_name = parse_info.gs.table_id2name[edge_table_id];
    int dst_vertex_table_alias = alias_info->current_alias;

    scan_op = std::unique_ptr<PhysicalScan>(new PhysicalScan());
    scan_op->set_alias(dst_vertex_table_alias);

    if (expand_direction ==
        physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
      scan_op = move(ScanParser::initializeScanByDefault(
          move(scan_op), target_table_id,
          parse_info.gs.table_id2name[target_table_id], dst_vertex_table_alias,
          other));
    } else {
      scan_op = move(ScanParser::initializeScanByDefault(
          move(scan_op), source_table_id,
          parse_info.gs.table_id2name[source_table_id], dst_vertex_table_alias,
          other));
    }

    if (!has_predicate && expand_opt == physical::EdgeExpand_ExpandOpt_VERTEX) {
      if (!parse_info.index_manager.graph_index_map.hasJoinPair(
              scan_op->table_id, start_table_id, edge_table_id) ||
          is_optional) {
        has_predicate = true;
      } else {
        join_op =
            std::unique_ptr<PhysicalNeighborJoin>(new PhysicalNeighborJoin());

        bool scan_right = isScanRight(edge, scan_op->table_id, parse_info);
        join_op = fillJoin(join_op, start_table_id, start_table_name,
                           start_table_alias, scan_op, parse_info, other,
                           std::move(right_op), scan_right, edge_table_id);

        if (expand_direction ==
            physical::EdgeExpand_Direction::EdgeExpand_Direction_IN) {
          join_op->join_direction = JoinDirection::SOURCE_EDGE;
        } else {
          join_op->join_direction = JoinDirection::TARGET_EDGE;
        }

        if (scan_right) {
          if (join_op->join_direction == JoinDirection::SOURCE_EDGE) {
            join_op->join_direction = JoinDirection::TARGET_EDGE;
          } else {
            join_op->join_direction = JoinDirection::SOURCE_EDGE;
          }
        }

        join_op->edge_table_name = edge_table_name;
      }
    }

    if (has_predicate && expand_opt == physical::EdgeExpand_ExpandOpt_VERTEX) {
      std::unique_ptr<PhysicalScan> inner_scan_op =
          std::unique_ptr<PhysicalScan>(new PhysicalScan());
      std::unique_ptr<PhysicalJoin> inner_join_op = nullptr;
      ParseInfo inner_other(parse_info.gs, parse_info.index_manager);

      inner_scan_op->set_alias(edge_alias);
      inner_scan_op = move(ScanParser::resolveScanParams(
          edge.params(), move(inner_scan_op), edge_alias, inner_other));

      std::shared_ptr<JoinConditionPair> inner_join_pair =
          parse_info.index_manager.graph_index_map.getJoinPair(
              inner_scan_op->table_id, start_table_id);
      if (inner_join_pair) {
        if (inner_join_pair->use_graph_index && !is_optional)
          inner_join_op =
              std::unique_ptr<PhysicalAdjJoin>(new PhysicalAdjJoin());
        else {
          if (is_optional)
            inner_join_op = std::unique_ptr<PhysicalHashJoin>(
                new PhysicalHashJoin(HashJoinType::LEFT_JOIN));
          else
            inner_join_op = std::unique_ptr<PhysicalHashJoin>(
                new PhysicalHashJoin(HashJoinType::INNER_JOIN));
        }
      } else {
        std::cerr << "cannot join " << inner_scan_op->table_id << " and "
                  << start_table_id << std::endl;
      }

      bool inner_scan_right =
          isScanRight(edge, inner_scan_op->table_id, parse_info);
      inner_join_op =
          fillJoin(inner_join_op, start_table_id, start_table_name,
                   start_table_alias, inner_scan_op, parse_info, inner_other,
                   std::move(right_op), inner_scan_right);

      std::shared_ptr<JoinConditionPair> join_pair =
          parse_info.index_manager.graph_index_map.getJoinPair(
              scan_op->table_id, edge_table_id);
      if (join_pair) {
        if (join_pair->use_graph_index && !is_optional)
          join_op = std::unique_ptr<PhysicalAdjJoin>(new PhysicalAdjJoin());
        else {
          if (is_optional)
            join_op = std::unique_ptr<PhysicalHashJoin>(
                new PhysicalHashJoin(HashJoinType::LEFT_JOIN));
          else
            join_op = std::unique_ptr<PhysicalHashJoin>(
                new PhysicalHashJoin(HashJoinType::INNER_JOIN));
        }
      } else {
        std::cerr << "cannot join " << scan_op->table_id << " and "
                  << edge_table_id << std::endl;
      }

      bool scan_right = isScanRight(edge, scan_op->table_id, parse_info);
      join_op =
          fillJoin(join_op, edge_table_id, edge_table_name, edge_alias, scan_op,
                   parse_info, other, std::move(inner_join_op), scan_right);
    }
  }

  return move(join_op);
}

std::unique_ptr<PhysicalJoin> EdgeExpandParser::parseEdgeExpand(
    const physical::EdgeExpand &edge, const physical::PhysicalOpr &op,
    const physical::GetV &next_op, std::unique_ptr<PhysicalOperator> right_op,
    ParseInfo &parse_info) {
  physical::EdgeExpand_ExpandOpt expand_opt = edge.expand_opt();
  physical::EdgeExpand_Direction expand_direction = edge.direction();

  bool has_predicate = false;
  bool is_optional = edge.is_optional();
  if (edge.params().has_predicate())
    has_predicate = true;

  std::unique_ptr<AliasInfo> alias_info =
      parse_info.index_manager.getAlias(edge);
  int start_table_alias = alias_info->related_alias[0];

  std::string start_table_id =
      parse_info.index_manager.table_alias2id[start_table_alias];
  std::string start_table_name = parse_info.gs.table_id2name[start_table_id];

  std::unique_ptr<PhysicalScan> scan_op = nullptr;
  std::unique_ptr<PhysicalJoin> join_op = nullptr;
  ParseInfo other(parse_info.gs, parse_info.index_manager);

  std::unique_ptr<AliasInfo> target_alias_info =
      parse_info.index_manager.getAlias();
  int edge_alias = target_alias_info->current_alias;
  std::string edge_table_id =
      parse_info.index_manager.table_alias2id[edge_alias];
  std::string edge_table_name = parse_info.gs.table_id2name[edge_table_id];

  std::unique_ptr<AliasInfo> dst_alias_info =
      parse_info.index_manager.getAlias(next_op);
  int dst_vertex_table_alias = dst_alias_info->current_alias;

  scan_op = std::unique_ptr<PhysicalScan>(new PhysicalScan());
  scan_op->set_alias(dst_vertex_table_alias);

  scan_op = move(ScanParser::resolveScanParams(next_op.params(), move(scan_op),
                                               dst_vertex_table_alias, other));

  if (!has_predicate) {
    if (!parse_info.index_manager.graph_index_map.hasJoinPair(
            scan_op->table_id, start_table_id, edge_table_id) ||
        is_optional) {
      has_predicate = true;
    } else {
      join_op =
          std::unique_ptr<PhysicalNeighborJoin>(new PhysicalNeighborJoin());
      join_op = fillInJoinCondition(join_op, start_table_id, start_table_name,
                                    start_table_alias, scan_op, parse_info,
                                    other, edge_table_id);
      if (expand_direction ==
          physical::EdgeExpand_Direction::EdgeExpand_Direction_IN) {
        join_op->join_direction = JoinDirection::SOURCE_EDGE;
      } else {
        join_op->join_direction = JoinDirection::TARGET_EDGE;
      }

      join_op->edge_table_name = edge_table_name;

      join_op->right = move(right_op);
      join_op->left = move(scan_op);
      return move(join_op);
    }
  }

  std::unique_ptr<PhysicalScan> inner_scan_op =
      std::unique_ptr<PhysicalScan>(new PhysicalScan());
  std::unique_ptr<PhysicalJoin> inner_join_op = nullptr;
  ParseInfo inner_other(parse_info.gs, parse_info.index_manager);

  inner_scan_op->set_alias(edge_alias);
  inner_scan_op = move(ScanParser::resolveScanParams(
      edge.params(), move(inner_scan_op), edge_alias, inner_other));

  std::shared_ptr<JoinConditionPair> inner_join_pair =
      parse_info.index_manager.graph_index_map.getJoinPair(
          inner_scan_op->table_id, start_table_id);
  if (inner_join_pair) {
    if (inner_join_pair->use_graph_index && !is_optional)
      inner_join_op = std::unique_ptr<PhysicalAdjJoin>(new PhysicalAdjJoin());
    else {
      if (is_optional)
        inner_join_op = std::unique_ptr<PhysicalHashJoin>(
            new PhysicalHashJoin(HashJoinType::LEFT_JOIN));
      else
        inner_join_op = std::unique_ptr<PhysicalHashJoin>(
            new PhysicalHashJoin(HashJoinType::INNER_JOIN));
    }
  } else {
    std::cerr << "cannot join " << inner_scan_op->table_id << " and "
              << start_table_id << std::endl;
  }

  bool inner_scan_right =
      isScanRight(edge, inner_scan_op->table_id, parse_info);
  inner_join_op = fillJoin(inner_join_op, start_table_id, start_table_name,
                           start_table_alias, inner_scan_op, parse_info,
                           inner_other, std::move(right_op), inner_scan_right);

  std::shared_ptr<JoinConditionPair> join_pair =
      parse_info.index_manager.graph_index_map.getJoinPair(scan_op->table_id,
                                                           edge_table_id);
  if (join_pair) {
    if (join_pair->use_graph_index && !is_optional)
      join_op = std::unique_ptr<PhysicalAdjJoin>(new PhysicalAdjJoin());
    else {
      if (is_optional)
        join_op = std::unique_ptr<PhysicalHashJoin>(
            new PhysicalHashJoin(HashJoinType::LEFT_JOIN));
      else
        join_op = std::unique_ptr<PhysicalHashJoin>(
            new PhysicalHashJoin(HashJoinType::INNER_JOIN));
    }
  } else {
    std::cerr << "cannot join " << scan_op->table_id << " and " << edge_table_id
              << std::endl;
  }

  bool scan_right = isScanRight(edge, scan_op->table_id, parse_info);
  join_op =
      fillJoin(join_op, edge_table_id, edge_table_name, edge_alias, scan_op,
               parse_info, other, std::move(inner_join_op), scan_right);

  return move(join_op);
}

std::unique_ptr<PhysicalJoin> EdgeExpandParser::fillInJoinCondition(
    std::unique_ptr<PhysicalJoin> &join_op, std::string start_table_id,
    std::string start_table_name, int start_table_alias,
    std::unique_ptr<PhysicalScan> &scan_op, ParseInfo &parse_info,
    ParseInfo &other, std::string join_bridge) {
  join_op->conditions = std::vector<std::unique_ptr<ConditionNode>>();
  std::unique_ptr<ConditionOperatorNode> connection_condition =
      std::unique_ptr<ConditionOperatorNode>(
          new ConditionOperatorNode(TripletCompType::EQ));
  std::shared_ptr<JoinConditionPair> join_condition_pair =
      other.index_manager.graph_index_map.getJoinPair(
          scan_op->table_id, start_table_id, join_bridge);

  if (!join_condition_pair)
    return nullptr;

  auto &join_info = join_condition_pair->join_info;
  std::string left_field_name = join_info[scan_op->table_id].column_name;
  int left_index =
      other.current_table_schema.getPropertyColIndex(left_field_name);
  std::string right_field_name = join_info[start_table_id].column_name;
  int right_index = parse_info.current_table_schema.getPropertyColIndex(
      right_field_name, start_table_id, start_table_alias);
  PhysicalType cmp_data_type = join_condition_pair->data_type;

  std::string left_table_oid =
      parse_info.gs.id_manager.getid(scan_op->table_id);
  std::unique_ptr<ConditionConstNode> left_condition =
      std::unique_ptr<ConditionConstNode>(new ConditionConstNode(
          left_field_name, cmp_data_type, left_index, left_table_oid,
          scan_op->table_name, scan_op->table_alias));
  std::string right_table_oid = parse_info.gs.id_manager.getid(start_table_id);
  std::unique_ptr<ConditionConstNode> right_condition =
      std::unique_ptr<ConditionConstNode>(new ConditionConstNode(
          right_field_name, cmp_data_type, right_index, right_table_oid,
          start_table_name, start_table_alias));
  connection_condition->addOperand(move(left_condition));
  connection_condition->addOperand(move(right_condition));
  join_op->conditions.push_back(move(connection_condition));
  join_op->use_graph_index = join_condition_pair->use_graph_index;

  if (join_op->use_graph_index) {
    join_op->join_direction = join_condition_pair->join_direction;
  }

  parse_info.join_front(other);

  for (int i = 0; i < parse_info.current_table_schema.properties.size(); ++i) {
    join_op->output_info.push_back(
        parse_info.current_table_schema.properties[i]);
  }

  return move(join_op);
}

std::unique_ptr<PhysicalJoin> EdgeExpandParser::fillInJoinConditionRev(
    std::unique_ptr<PhysicalJoin> &join_op, std::string start_table_id,
    std::string start_table_name, int start_table_alias,
    std::unique_ptr<PhysicalScan> &scan_op, ParseInfo &parse_info,
    ParseInfo &other, std::string join_bridge) {
  join_op->conditions = std::vector<std::unique_ptr<ConditionNode>>();
  std::unique_ptr<ConditionOperatorNode> connection_condition =
      std::unique_ptr<ConditionOperatorNode>(
          new ConditionOperatorNode(TripletCompType::EQ));
  std::shared_ptr<JoinConditionPair> join_condition_pair =
      other.index_manager.graph_index_map.getJoinPair(
          scan_op->table_id, start_table_id, join_bridge);

  if (!join_condition_pair)
    return nullptr;

  auto &join_info = join_condition_pair->join_info;
  std::string left_field_name = join_info[scan_op->table_id].column_name;
  int left_index =
      other.current_table_schema.getPropertyColIndex(left_field_name);
  std::string right_field_name = join_info[start_table_id].column_name;
  int right_index = parse_info.current_table_schema.getPropertyColIndex(
      right_field_name, start_table_id, start_table_alias);
  PhysicalType cmp_data_type = join_condition_pair->data_type;

  std::string left_table_oid =
      parse_info.gs.id_manager.getid(scan_op->table_id);
  std::unique_ptr<ConditionConstNode> left_condition =
      std::unique_ptr<ConditionConstNode>(new ConditionConstNode(
          left_field_name, cmp_data_type, left_index, left_table_oid,
          scan_op->table_name, scan_op->table_alias));
  std::string right_table_oid = parse_info.gs.id_manager.getid(start_table_id);
  std::unique_ptr<ConditionConstNode> right_condition =
      std::unique_ptr<ConditionConstNode>(new ConditionConstNode(
          right_field_name, cmp_data_type, right_index, right_table_oid,
          start_table_name, start_table_alias));

  connection_condition->addOperand(move(right_condition));
  connection_condition->addOperand(move(left_condition));

  join_op->conditions.push_back(move(connection_condition));
  join_op->use_graph_index = join_condition_pair->use_graph_index;

  if (join_op->use_graph_index) {
    join_op->join_direction =
        reverseJoinDirection(join_condition_pair->join_direction);
  }

  parse_info.join(other);

  for (int i = 0; i < parse_info.current_table_schema.properties.size(); ++i) {
    join_op->output_info.push_back(
        parse_info.current_table_schema.properties[i]);
  }

  return move(join_op);
}

std::unique_ptr<PhysicalJoin> EdgeExpandParser::fillJoin(
    std::unique_ptr<PhysicalJoin> &join_op, std::string start_table_id,
    std::string start_table_name, int start_table_alias,
    std::unique_ptr<PhysicalScan> &scan_op, ParseInfo &parse_info,
    ParseInfo &other, std::unique_ptr<PhysicalOperator> right, bool scan_right,
    std::string join_bridge) {
  if (!scan_right) {
    join_op = fillInJoinCondition(join_op, start_table_id, start_table_name,
                                  start_table_alias, scan_op, parse_info, other,
                                  join_bridge);
    join_op->left = move(scan_op);
    join_op->right = move(right);

    return move(join_op);
  } else {
    join_op = fillInJoinConditionRev(join_op, start_table_id, start_table_name,
                                     start_table_alias, scan_op, parse_info,
                                     other, join_bridge);
    join_op->left = move(right);
    join_op->right = move(scan_op);
    return move(join_op);
  }
}

bool EdgeExpandParser::isScanRight(const physical::EdgeExpand &edge,
                                   std::string &next_table_id,
                                   ParseInfo &parse_info) {
  bool is_optional = edge.is_optional();
  if (is_optional)
    return true;

  if (!parse_info.config.check_scan_right)
    return false;

  idx_t next_card = parse_info.gs.table_row_count[next_table_id];
  std::cout << "check is scan right: " << next_card << " "
            << parse_info.getCard() << std::endl;
  if (next_card < parse_info.getCard())
    return true;

  return false;
}

} // namespace relgo