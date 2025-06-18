#include "intersect_parser.hpp"

#include "core_parser.hpp"
#include "edge_expand_parser.hpp"

namespace relgo {

bool IntersectParser::opHasNullChild(std::unique_ptr<PhysicalOperator> &op) {
  if (op->op_type == PhysicalOperatorType::PHYSICAL_NEIGHBOR_JOIN_TYPE) {
    PhysicalNeighborJoin *neighbor_join =
        dynamic_cast<PhysicalNeighborJoin *>(op.get());
    return neighbor_join->right == nullptr;
  } else if (op->op_type == PhysicalOperatorType::PHYSICAL_ADJ_JOIN_TYPE) {
    PhysicalAdjJoin *adj_join = dynamic_cast<PhysicalAdjJoin *>(op.get());
    return adj_join->right == nullptr;
  } else if (op->op_type == PhysicalOperatorType::PHYSICAL_HASH_JOIN_TYPE) {
    PhysicalHashJoin *hash_join = dynamic_cast<PhysicalHashJoin *>(op.get());
    return hash_join->right == nullptr;
  } else
    return false;
}

std::unique_ptr<PhysicalOperator> IntersectParser::traverseSubOperators(
    std::unique_ptr<PhysicalOperator> &sub_op,
    std::unique_ptr<PhysicalOperator> &last_op, idx_t &end_alias, int depth,
    int op_index, ParseInfo &parse_info) {
  if (depth == 0 && end_alias == -1) {
    PhysicalJoin *join_op = dynamic_cast<PhysicalJoin *>(sub_op.get());
    PhysicalScan *scan_op = dynamic_cast<PhysicalScan *>(join_op->left.get());
    end_alias = scan_op->table_alias;
  }

  bool has_null_child = opHasNullChild(sub_op);
  ParseInfo backup_info(parse_info);
  if (!has_null_child) {
    PhysicalJoin *join_op = dynamic_cast<PhysicalJoin *>(sub_op.get());
    last_op = traverseSubOperators(join_op->right, last_op, end_alias,
                                   depth + 1, op_index, parse_info);
  }

  if (has_null_child) {
    PhysicalJoin *join_op = dynamic_cast<PhysicalJoin *>(sub_op.get());
    join_op->right = move(last_op);
  }

  if (sub_op->op_type == PhysicalOperatorType::PHYSICAL_NEIGHBOR_JOIN_TYPE) {
    PhysicalJoin *join_op = dynamic_cast<PhysicalJoin *>(sub_op.get());
    PhysicalScan *scan_op = dynamic_cast<PhysicalScan *>(join_op->left.get());
    std::vector<Property> properties;
    std::unordered_map<std::string, std::string> info;

    for (size_t i = 0; i < scan_op->column_ids.size(); ++i) {
      properties.push_back(Property(i, scan_op->column_names[i],
                                    scan_op->column_types[i], scan_op->table_id,
                                    scan_op->table_alias));
    }

    SchemaElement schema_element =
        SchemaElement(scan_op->table_name, scan_op->table_id, properties, info,
                      SchemaElementType::SCHEMA_VERTEX_ELEMENT);

    parse_info.current_table_schema.merge_front(schema_element);
    join_op->output_info.clear();
    for (int i = 0; i < parse_info.current_table_schema.properties.size();
         ++i) {
      join_op->output_info.push_back(
          parse_info.current_table_schema.properties[i]);
    }
  } else if (sub_op->op_type == PhysicalOperatorType::PHYSICAL_ADJ_JOIN_TYPE ||
             sub_op->op_type == PhysicalOperatorType::PHYSICAL_HASH_JOIN_TYPE) {
    PhysicalJoin *join = dynamic_cast<PhysicalJoin *>(sub_op.get());
    for (size_t i = 0; i < join->conditions.size(); ++i) {
      int condition_part = has_null_child ? 1 : 0;
      ConditionOperatorNode *op_cond =
          dynamic_cast<ConditionOperatorNode *>(join->conditions[i].get());
      ConditionConstNode *const_cond = dynamic_cast<ConditionConstNode *>(
          op_cond->operands[condition_part].get());
      const_cond->field_index =
          backup_info.current_table_schema.getPropertyColIndex(
              const_cond->field,
              backup_info.gs.id_manager.mapid(const_cond->table_id),
              const_cond->table_alias);
      if (!has_null_child) {
        std::reverse(op_cond->operands.begin(), op_cond->operands.end());
      }
    }

    if (!has_null_child) {
      PhysicalJoin *last_op_ptr = dynamic_cast<PhysicalJoin *>(last_op.get());
      for (size_t i = 0; i < join->conditions.size(); ++i) {
        last_op_ptr->conditions.push_back(move(join->conditions[i]));
      }
    } else {
      PhysicalJoin *join_op = dynamic_cast<PhysicalJoin *>(sub_op.get());
      PhysicalScan *scan_op = dynamic_cast<PhysicalScan *>(join_op->left.get());
      std::vector<Property> properties;
      std::unordered_map<std::string, std::string> info;

      for (size_t i = 0; i < scan_op->column_ids.size(); ++i) {
        properties.push_back(Property(i, scan_op->column_names[i],
                                      scan_op->column_types[i],
                                      scan_op->table_id, scan_op->table_alias));
      }

      SchemaElement schema_element =
          SchemaElement(scan_op->table_name, scan_op->table_id, properties,
                        info, SchemaElementType::SCHEMA_VERTEX_ELEMENT);

      parse_info.current_table_schema.merge_front(schema_element);
      join->output_info.clear();
      for (int i = 0; i < parse_info.current_table_schema.properties.size();
           ++i) {
        join->output_info.push_back(
            parse_info.current_table_schema.properties[i]);
      }
    }

    if (has_null_child && last_op) {
      join->right = move(last_op);
    }

    if (!has_null_child) {
      return move(last_op);
    } else
      return move(sub_op);
  }

  return move(sub_op);
}

std::unique_ptr<PhysicalOperator> IntersectParser::unpackIntersect(
    std::unique_ptr<PhysicalExtendIntersectJoin> &op,
    std::unique_ptr<PhysicalOperator> right_op, ParseInfo &parse_info) {
  std::unique_ptr<PhysicalJoin> result_op;
  std::unique_ptr<PhysicalOperator> last_op = move(right_op);

  idx_t end_alias = -1;
  for (size_t subplan_idx = 0; subplan_idx < op->sub_operators.size();
       ++subplan_idx) {
    std::unique_ptr<PhysicalOperator> sub_op =
        move(op->sub_operators[subplan_idx]);
    if (sub_op == nullptr)
      continue;

    last_op = traverseSubOperators(sub_op, last_op, end_alias, 0, subplan_idx,
                                   parse_info);
  }

  return move(last_op);
}

std::unique_ptr<PhysicalOperator>
IntersectParser::parseIntersect(const physical::Intersect &intersect,
                                std::unique_ptr<PhysicalOperator> right_op,
                                ParseInfo &parse_info) {
  std::unique_ptr<PhysicalScan> scan_op = nullptr;
  std::unique_ptr<PhysicalExtendIntersectJoin> join_op =
      std::unique_ptr<PhysicalExtendIntersectJoin>(
          new PhysicalExtendIntersectJoin());

  ParseInfo parse_backup(parse_info);
  parse_info.config.check_scan_right = false;

  std::vector<std::unique_ptr<PhysicalOperator>> subplan_ops;
  for (size_t subplan_idx = 0; subplan_idx < intersect.sub_plans_size();
       ++subplan_idx) {
    ParseInfo other(parse_info);
    std::unique_ptr<PhysicalOperator> subop =
        CoreParser::parse(intersect.sub_plans(subplan_idx), other);
    parse_info.index_manager.default_table_alias =
        other.index_manager.default_table_alias;
    subplan_ops.push_back(move(subop));

    if (subplan_idx == intersect.sub_plans_size() - 1) {
      parse_info = other;
    }
  }

  bool all_neighbor_joins = true;
  for (size_t subplan_idx = 0; subplan_idx < subplan_ops.size();
       ++subplan_idx) {
    if (subplan_ops[subplan_idx]->op_type !=
        PhysicalOperatorType::PHYSICAL_NEIGHBOR_JOIN_TYPE) {
      all_neighbor_joins = false;
    }
  }

  join_op->sub_operators = move(subplan_ops);

  if (all_neighbor_joins) {
    for (size_t subplan_idx = 0; subplan_idx < join_op->sub_operators.size();
         ++subplan_idx) {
      PhysicalNeighborJoin *neighbor_join =
          dynamic_cast<PhysicalNeighborJoin *>(
              join_op->sub_operators[subplan_idx].get());
      for (size_t condition_idx = 0;
           condition_idx < neighbor_join->conditions.size(); ++condition_idx) {
        join_op->merge_conditions.push_back(
            move(neighbor_join->conditions[condition_idx]));
      }

      if (subplan_idx == 0) {
        join_op->left = move(neighbor_join->left);
      }
    }

    for (int i = 0; i < parse_info.current_table_schema.properties.size();
         ++i) {
      join_op->output_info.push_back(
          parse_info.current_table_schema.properties[i]);
    }

    join_op->right = move(right_op);

    std::unique_ptr<AliasInfo> alias_info =
        parse_info.index_manager.getAlias(intersect);

    parse_info.config.check_scan_right = parse_backup.config.check_scan_right;
    return join_op;
  } else {
    std::unique_ptr<PhysicalOperator> serial_join_op =
        unpackIntersect(join_op, move(right_op), parse_backup);
    parse_info = parse_backup;
    return serial_join_op;
  }
}

} // namespace relgo