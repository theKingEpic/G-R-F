#include "graph_index_manager.hpp"

#include "../operators/physical_join.hpp"
#include "../operators/physical_scan.hpp"
#include "../parser/aggregate_parser.hpp"
#include "../parser/algebra_parser.hpp"
#include "../parser/common_parser.hpp"
#include "../parser/expr_parser.hpp"
#include "../parser/scan_parser.hpp"

#include <yaml-cpp/yaml.h>

namespace relgo {

std::string GraphIndexMap::createKey(const std::string &a, const std::string &b,
                                     std::string bridge) {
  std::string key1 = std::to_string(a.length()) + ":" + a;
  std::string key2 = std::to_string(b.length()) + ":" + b;

  if (bridge == "") {
    return key1 + "|" + key2;
  } else {
    return key1 + "|" + bridge + "|" + key2;
  }
}

bool GraphIndexMap::hasJoinPair(std::string &to_table_id,
                                std::string &from_table_id,
                                std::string join_bridge) {
  std::string key = createKey(to_table_id, from_table_id, join_bridge);
  return join_pairs.find(key) != join_pairs.end();
}

std::shared_ptr<JoinConditionPair>
GraphIndexMap::getJoinPair(std::string &to_table_id, std::string &from_table_id,
                           std::string join_bridge) {
  std::string key = createKey(to_table_id, from_table_id, join_bridge);
  if (join_pairs.find(key) != join_pairs.end()) {
    return join_pairs[key];
  } else {
    std::cout << "Table " << to_table_id << " and " << from_table_id
              << " cannot be joined." << std::endl;
    return nullptr;
  }
}

void GraphIndexMap::addJoinPair(
    std::string source_table_id, std::string source_column_name,
    int source_column_index, std::string target_table_id,
    std::string target_column_name, int target_column_index,
    PhysicalType data_type, bool use_graph_index, JoinDirection join_direction,
    JoinIndexType join_index_type, std::string join_bridge) {
  std::shared_ptr<JoinConditionPair> join_condition =
      std::make_shared<JoinConditionPair>();

  join_condition->join_info[source_table_id] =
      JoinInfo(source_table_id, source_column_name, source_column_index);
  join_condition->join_info[target_table_id] =
      JoinInfo(target_table_id, target_column_name, target_column_index);

  join_condition->data_type = data_type;
  if (use_graph_index)
    join_condition->use_graph_index = true;
  join_condition->join_direction = join_direction;
  join_condition->join_index_type = join_index_type;

  this->join_pairs[createKey(target_table_id, source_table_id, join_bridge)] =
      join_condition;
}

void IndexManager::addUsedIndexColumn(JoinConditionPair &pair,
                                      std::string table_id1, int &alias1,
                                      std::string table_id2, int &alias2) {
  if (used_index_columns.find(table_id1) == used_index_columns.end()) {
    used_index_columns[table_id1] =
        std::unordered_map<int, std::vector<IndexColumn>>();
  }
  if (used_index_columns[table_id1].find(alias1) ==
      used_index_columns[table_id1].end()) {
    used_index_columns[table_id1][alias1] = std::vector<IndexColumn>();
  }
  used_index_columns[table_id1][alias1].push_back(
      IndexColumn(pair.join_info[table_id1].column_name,
                  pair.join_info[table_id1].column_index, pair.data_type));

  if (used_index_columns.find(table_id2) == used_index_columns.end()) {
    used_index_columns[table_id2] =
        std::unordered_map<int, std::vector<IndexColumn>>();
  }
  if (used_index_columns[table_id2].find(alias2) ==
      used_index_columns[table_id2].end()) {
    used_index_columns[table_id2][alias2] = std::vector<IndexColumn>();
  }

  used_index_columns[table_id2][alias2].push_back(
      IndexColumn(pair.join_info[table_id2].column_name,
                  pair.join_info[table_id2].column_index, pair.data_type));
}

std::unique_ptr<AliasInfo>
IndexManager::getAlias(physical::PhysicalOpr_Operator &oper) {
  std::vector<int> related;
  if (oper.has_scan()) {
    return move(getAlias(oper.scan()));
  } else if (oper.has_vertex()) {
    return move(getAlias(oper.vertex()));
  } else if (oper.has_edge()) {
    return move(getAlias(oper.edge()));
  }

  return std::unique_ptr<AliasInfo>(new AliasInfo());
}

std::unique_ptr<AliasInfo>
IndexManager::getAlias(const physical::Scan &scan_instance) {
  std::vector<int> related;
  if (scan_instance.has_alias()) {
    int table_alias = scan_instance.alias().value();
    last_table_alias = table_alias;
    return std::unique_ptr<AliasInfo>(new AliasInfo(table_alias, related));
  } else {
    int table_alias = default_table_alias++;
    last_table_alias = table_alias;
    return std::unique_ptr<AliasInfo>(new AliasInfo(table_alias, related));
  }
}

std::unique_ptr<AliasInfo>
IndexManager::getAlias(const physical::GetV &get_v_instance) {
  std::vector<int> related;
  int edge_table_alias = last_table_alias;
  if (get_v_instance.has_tag()) {
    edge_table_alias = get_v_instance.tag().value();
  };

  int vertex_table_alias;
  if (get_v_instance.has_alias()) {
    vertex_table_alias = get_v_instance.alias().value();
  } else {
    vertex_table_alias = default_table_alias++;
  }
  last_table_alias = vertex_table_alias;

  related.push_back(edge_table_alias);
  return std::unique_ptr<AliasInfo>(new AliasInfo(vertex_table_alias, related));
}

std::unique_ptr<AliasInfo>
IndexManager::getAlias(const physical::EdgeExpand &get_e_instance) {
  std::vector<int> related;
  int vertex_table_alias = last_table_alias;
  if (get_e_instance.has_v_tag()) {
    vertex_table_alias = get_e_instance.v_tag().value();
  }

  int edge_table_alias;
  if (get_e_instance.has_alias()) {
    edge_table_alias = get_e_instance.alias().value();
  } else {
    edge_table_alias = default_table_alias++;
  }
  last_table_alias = edge_table_alias;

  related.push_back(vertex_table_alias);
  return std::unique_ptr<AliasInfo>(new AliasInfo(edge_table_alias, related));
}

std::unique_ptr<AliasInfo>
IndexManager::getAlias(const physical::Intersect &intersect_instance) {
  std::vector<int> related;
  int end_vertex_alias;
  if (intersect_instance.key()) {
    end_vertex_alias = intersect_instance.key();
  } else {
    end_vertex_alias = default_table_alias++;
  }

  last_table_alias = end_vertex_alias;
  return std::unique_ptr<AliasInfo>(new AliasInfo(end_vertex_alias, related));
}

std::unique_ptr<AliasInfo> IndexManager::getAlias() {
  std::vector<int> related;
  int new_table_alias = default_table_alias++;
  last_table_alias = new_table_alias;
  return std::unique_ptr<AliasInfo>(new AliasInfo(new_table_alias, related));
}

void IndexManager::getNecessaryProps(const algebra::QueryParams &params,
                                     int table_alias) {
  ParseInfo parse_info;
  if (params.has_predicate()) {
    for (int i = 0; i < params.predicate().operators_size(); ++i) {
      auto &item = params.predicate().operators(i);
      if (item.item_case() == common::ExprOpr::kVar) {
        std::string var_name =
            ExprParser::parseProperty(item.var().property(), parse_info);
        necessary_props[table_alias].insert(var_name);
      }
    }
  }
}

void IndexManager::getNecessaryProps(const common::Expression &predicate) {
  ParseInfo parse_info;
  for (int i = 0; i < predicate.operators_size(); ++i) {
    auto &item = predicate.operators(i);
    if (item.item_case() == common::ExprOpr::kVar) {
      std::string var_name =
          ExprParser::parseProperty(item.var().property(), parse_info);

      int table_alias = item.var().tag().id();
      necessary_props[table_alias].insert(var_name);
    }
  }
}

void IndexManager::getNecessaryProps(const physical::Scan &scan_instance,
                                     int table_alias) {
  ParseInfo parse_info;
  if (scan_instance.has_params()) {
    auto &params = scan_instance.params();
    getNecessaryProps(params, table_alias);
  }

  if (scan_instance.has_idx_predicate()) {
    auto &idx_pred = scan_instance.idx_predicate();
    for (size_t i = 0; i < idx_pred.or_predicates_size(); ++i) {
      auto and_pred = idx_pred.or_predicates(i);

      for (size_t j = 0; j < and_pred.predicates_size(); ++j) {
        auto triplet = and_pred.predicates(j);
        const common::Property &key = triplet.key();

        std::string prop = ExprParser::parseProperty(key, parse_info);
        necessary_props[table_alias].insert(prop);
      }
    }
  }
}

void IndexManager::getNecessaryProps(
    const physical::Project &project_instance) {
  ParseInfo parse_info;
  for (int i = 0; i < project_instance.mappings_size(); ++i) {
    const physical::Project_ExprAlias mapping = project_instance.mappings(i);

    for (int i = 0; i < mapping.expr().operators_size(); ++i) {
      auto &item = mapping.expr().operators(i);
      if (item.item_case() == common::ExprOpr::kVar) {
        int table_alias = -1;
        auto &var = item.var();
        if (var.has_tag()) {
          table_alias =
              CommonParser::parseNameOrId(var.tag()).GetValueUnsafe<int32_t>();
        }

        std::string var_name =
            ExprParser::parseProperty(var.property(), parse_info);
        necessary_props[table_alias].insert(var_name);
      }
    }
  }
}

void IndexManager::getNecessaryProps(
    const physical::GroupBy &aggregate_instance) {
  ParseInfo parse_info;
  for (size_t i = 0; i < aggregate_instance.functions_size(); ++i) {
    const physical::GroupBy_AggFunc function = aggregate_instance.functions(i);
    const physical::GroupBy_AggFunc_Aggregate type = function.aggregate();
    AggregateType aggregate_type =
        AggregateParser::convertPhysicalAggTypeToAggType(type);

    bool finish_this_func = false;
    for (size_t var_idx = 0; var_idx < function.vars_size(); ++var_idx) {
      std::vector<std::unique_ptr<VariableExprOpr>> var_opr_list =
          ExprParser::parseVariables(function.vars(var_idx), parse_info);
      for (auto &var_opr : var_opr_list) {
        necessary_props[var_opr->table_alias].insert(var_opr->variable_name);

        if (aggregate_type == AggregateType::COUNT_TYPE &&
            !function.vars(var_idx).has_property()) {
          finish_this_func = true;
          break;
        }
      }

      if (finish_this_func)
        break;
    }
  }
}

void IndexManager::getNecessaryProps(const algebra::Select &select_instance) {
  ParseInfo parse_info;
  getNecessaryProps(select_instance.predicate());
}

void IndexManager::analyzPhysicalOpr(const physical::PhysicalPlan &plan,
                                     int &op_idx) {
  physical::PhysicalOpr op = plan.plan(op_idx);
  physical::PhysicalOpr_Operator oper = op.opr();
  if (oper.has_project()) {
    physical::Project project_instance = oper.project();
    getNecessaryProps(project_instance);
  } else if (oper.has_group_by()) {
    physical::GroupBy aggregate_instance = oper.group_by();
    getNecessaryProps(aggregate_instance);
  } else if (oper.has_select()) {
    algebra::Select select_instance = oper.select();
    getNecessaryProps(select_instance);
  } else if (oper.has_scan()) {
    physical::Scan scan_instance = oper.scan();
    std::string table_id =
        CommonParser::parseNameOrId(scan_instance.params().tables(0))
            .ToString();
    std::unique_ptr<AliasInfo> alias_info = getAlias(scan_instance);
    table_alias2id[alias_info->current_alias] = table_id;
    getNecessaryProps(scan_instance, alias_info->current_alias);
  } else if (oper.has_join()) {
    physical::Join join_instance = oper.join();

    std::string left_table_id =
        CommonParser::parseNameOrId(join_instance.left_keys(0).tag())
            .ToString();
    int left_table_alias = DEFAULT_TABLE_ALIAS;
    std::string right_table_id =
        CommonParser::parseNameOrId(join_instance.right_keys(0).tag())
            .ToString();
    int right_table_alias = DEFAULT_TABLE_ALIAS;
    std::shared_ptr<JoinConditionPair> join_condition_pair =
        graph_index_map.getJoinPair(left_table_id, right_table_id);
    addUsedIndexColumn(*(join_condition_pair.get()), left_table_id,
                       left_table_alias, right_table_id, right_table_alias);
  } else if (oper.has_vertex()) {
    physical::GetV get_v_instance = oper.vertex();
    std::unique_ptr<AliasInfo> alias_info = getAlias(get_v_instance);
    int edge_table_alias = alias_info->related_alias[0];
    std::string edge_table_id = table_alias2id[edge_table_alias];

    algebra::QueryParams para = get_v_instance.params();
    int vertex_table_alias = alias_info->current_alias;

    std::string vertex_table_id = "";
    if (para.tables_size() > 0) {
      vertex_table_id = CommonParser::parseNameOrId(para.tables(0)).ToString();
    }
    table_alias2id[vertex_table_alias] = vertex_table_id;
    if (get_v_instance.has_params()) {
      getNecessaryProps(get_v_instance.params(), vertex_table_alias);
    }

    std::shared_ptr<JoinConditionPair> join_condition_pair =
        graph_index_map.getJoinPair(vertex_table_id, edge_table_id);
    addUsedIndexColumn(*(join_condition_pair.get()), vertex_table_id,
                       vertex_table_alias, edge_table_id, edge_table_alias);
  } else if (oper.has_edge()) {
    physical::EdgeExpand get_e_instance = oper.edge();
    physical::EdgeExpand_ExpandOpt expand_opt = get_e_instance.expand_opt();
    physical::EdgeExpand_Direction expand_direction =
        get_e_instance.direction();
    std::unique_ptr<AliasInfo> alias_info = getAlias(get_e_instance);
    int vertex_table_alias = alias_info->related_alias[0];
    std::string vertex_table_id = table_alias2id[vertex_table_alias];

    algebra::QueryParams para = get_e_instance.params();

    bool has_predicate = false;
    bool is_optional = get_e_instance.is_optional();
    if (get_e_instance.params().has_predicate())
      has_predicate = true;

    std::string edge_table_id = "";
    if (para.tables_size() > 0) {
      edge_table_id = CommonParser::parseNameOrId(para.tables(0)).ToString();
    }

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
      int edge_table_alias = alias_info->current_alias;
      table_alias2id[edge_table_alias] = edge_table_id;
      if (get_e_instance.has_params()) {
        getNecessaryProps(get_e_instance.params(), edge_table_alias);
      }

      std::shared_ptr<JoinConditionPair> join_condition_pair =
          graph_index_map.getJoinPair(edge_table_id, vertex_table_id);
      addUsedIndexColumn(*(join_condition_pair.get()), edge_table_id,
                         edge_table_alias, vertex_table_id, vertex_table_alias);
    } else {
      if (expand_opt == physical::EdgeExpand_ExpandOpt_VERTEX &&
          op_idx != plan.plan_size() - 1 &&
          plan.plan(op_idx + 1).opr().has_vertex()) {
        op_idx++;
        physical::GetV next_op = plan.plan(op_idx).opr().vertex();
        std::unique_ptr<AliasInfo> target_alias_info = getAlias();
        int edge_table_alias = target_alias_info->current_alias;
        table_alias2id[edge_table_alias] = edge_table_id;
        if (get_e_instance.has_params()) {
          getNecessaryProps(get_e_instance.params(), edge_table_alias);
        }

        std::unique_ptr<AliasInfo> dst_alias_info = getAlias(next_op);
        int dst_vertex_table_alias = dst_alias_info->current_alias;

        std::string dst_table_id =
            CommonParser::parseNameOrId(next_op.params().tables(0)).ToString();

        table_alias2id[dst_vertex_table_alias] = dst_table_id;
        if (next_op.has_params()) {
          getNecessaryProps(next_op.params(), dst_vertex_table_alias);
        }

        if (!has_predicate && !is_optional) {
          std::shared_ptr<JoinConditionPair> join_condition_pair =
              graph_index_map.getJoinPair(dst_table_id, vertex_table_id,
                                          edge_table_id);

          if (join_condition_pair) {
            addUsedIndexColumn(*(join_condition_pair.get()), dst_table_id,
                               dst_vertex_table_alias, vertex_table_id,
                               vertex_table_alias);
          } else {
            std::shared_ptr<JoinConditionPair> join_condition_pair =
                graph_index_map.getJoinPair(edge_table_id, vertex_table_id);
            addUsedIndexColumn(*(join_condition_pair.get()), edge_table_id,
                               edge_table_alias, vertex_table_id,
                               vertex_table_alias);
            std::shared_ptr<JoinConditionPair> next_join_condition_pair =
                graph_index_map.getJoinPair(dst_table_id, edge_table_id);
            addUsedIndexColumn(*(join_condition_pair.get()), dst_table_id,
                               dst_vertex_table_alias, edge_table_id,
                               edge_table_alias);
          }
        } else {
          std::shared_ptr<JoinConditionPair> join_condition_pair =
              graph_index_map.getJoinPair(edge_table_id, vertex_table_id);
          addUsedIndexColumn(*(join_condition_pair.get()), edge_table_id,
                             edge_table_alias, vertex_table_id,
                             vertex_table_alias);
          std::shared_ptr<JoinConditionPair> next_join_condition_pair =
              graph_index_map.getJoinPair(dst_table_id, edge_table_id);
          addUsedIndexColumn(*(join_condition_pair.get()), dst_table_id,
                             dst_vertex_table_alias, edge_table_id,
                             edge_table_alias);
        }
      } else {
        std::unique_ptr<AliasInfo> target_alias_info = getAlias();
        int edge_table_alias = target_alias_info->current_alias;
        table_alias2id[edge_table_alias] = edge_table_id;
        if (get_e_instance.has_params()) {
          getNecessaryProps(get_e_instance.params(), edge_table_alias);
        }

        int dst_vertex_table_alias = alias_info->current_alias;

        if (!has_predicate &&
            expand_opt == physical::EdgeExpand_ExpandOpt_VERTEX) {
          if (expand_direction ==
              physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
            table_alias2id[dst_vertex_table_alias] = target_table_id;
            std::shared_ptr<JoinConditionPair> join_condition_pair =
                graph_index_map.getJoinPair(target_table_id, vertex_table_id,
                                            edge_table_id);

            if (join_condition_pair && !is_optional) {
              addUsedIndexColumn(*(join_condition_pair.get()), target_table_id,
                                 dst_vertex_table_alias, vertex_table_id,
                                 vertex_table_alias);
            } else {
              has_predicate = true;
            }
          } else {
            table_alias2id[dst_vertex_table_alias] = source_table_id;
            std::shared_ptr<JoinConditionPair> join_condition_pair =
                graph_index_map.getJoinPair(source_table_id, vertex_table_id,
                                            edge_table_id);

            if (join_condition_pair && !is_optional) {
              addUsedIndexColumn(*(join_condition_pair.get()), source_table_id,
                                 dst_vertex_table_alias, vertex_table_id,
                                 vertex_table_alias);
            } else {
              has_predicate = true;
            }
          }
        }

        if (has_predicate &&
            expand_opt == physical::EdgeExpand_ExpandOpt_VERTEX) {

          std::shared_ptr<JoinConditionPair> join_condition_pair =
              graph_index_map.getJoinPair(edge_table_id, vertex_table_id);
          addUsedIndexColumn(*(join_condition_pair.get()), edge_table_id,
                             edge_table_alias, vertex_table_id,
                             vertex_table_alias);

          if (expand_direction ==
              physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
            table_alias2id[dst_vertex_table_alias] = target_table_id;
            std::shared_ptr<JoinConditionPair> join_condition_pair =
                graph_index_map.getJoinPair(target_table_id, edge_table_id);
            addUsedIndexColumn(*(join_condition_pair.get()), target_table_id,
                               dst_vertex_table_alias, edge_table_id,
                               edge_table_alias);
          } else {
            table_alias2id[dst_vertex_table_alias] = source_table_id;
            std::shared_ptr<JoinConditionPair> join_condition_pair =
                graph_index_map.getJoinPair(source_table_id, edge_table_id);
            addUsedIndexColumn(*(join_condition_pair.get()), source_table_id,
                               dst_vertex_table_alias, edge_table_id,
                               edge_table_alias);
          }
        }
      }
    }
  } else if (oper.has_intersect()) {
    const physical::Intersect intersect_instance = oper.intersect();

    for (size_t subplan_idx = 0;
         subplan_idx < intersect_instance.sub_plans_size(); ++subplan_idx) {
      initFromPb(intersect_instance.sub_plans(subplan_idx));
    }

    std::unique_ptr<AliasInfo> alias_info = getAlias(intersect_instance);
  }
}

void IndexManager::initFromPb(const physical::PhysicalPlan &op) {
  for (int i = 0; i < op.plan_size(); ++i) {
    analyzPhysicalOpr(op, i);
  }

  // for (auto &pair : necessary_props) {
  //   std::cout << pair.first << ": ";
  //   for (auto &var : pair.second) {
  //     std::cout << var << " ";
  //   }
  //   std::cout << std::endl;
  // }
}

} // namespace relgo