#pragma once

#include "../graph_index/physical_graph_index.hpp"
#include "../graph_operators/includes/indexed_join_condition.hpp"
#include "../graph_operators/includes/indexed_table_scan.hpp"
#include "../graph_operators/includes/logical_indexed_get.hpp"
#include "../graph_operators/includes/physical_adj_join.hpp"
#include "../graph_operators/includes/physical_extend_intersect.hpp"
#include "../graph_operators/includes/physical_indexed_table_scan.hpp"
#include "../graph_operators/includes/physical_neighbor_join.hpp"
#include "../relgo_statement/relgo_client_context.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "plan_ir.hpp"

#include <algorithm>
#include <iostream>
#include <regex>
#include <string>

namespace duckdb {

class OpTransform : public relgo::PlanIR<unique_ptr<duckdb::PhysicalOperator>,
                                         LogicalType, ExpressionType, Value> {
public:
  OpTransform(shared_ptr<RelGoClientContext> _context) : context(_context) {
    table_types["person"] = "vertex";
    table_types["knows"] = "edge";
  }
  ~OpTransform() {}

  // Connection &connection;
  shared_ptr<RelGoClientContext> context;
  std::unordered_map<std::string, std::string> table_types;

  void loadGraphSchema() {
    auto all_tables = context->QueryConnection(
        "SELECT table_name, table_oid, estimated_size FROM duckdb_tables;");
    auto all_tables_chunk = all_tables->Fetch();

    for (size_t i = 0; i < all_tables_chunk->size(); ++i) {
      std::string table_name = all_tables_chunk->GetValue(0, i).ToString();
      std::string table_oid = all_tables_chunk->GetValue(1, i).ToString();
      int table_row_count =
          all_tables_chunk->GetValue(2, i).GetValueUnsafe<int64_t>();
      graph_schema.addTable(table_name, table_oid, table_row_count);
    }

    for (const auto &table_name : graph_schema.table_names) {
      std::string table_id = graph_schema.table_name2id[table_name];

      auto table_columns =
          context->QueryConnection("DESCRIBE " + table_name + ";")->Fetch();
      std::vector<relgo::Property> properties;

      for (size_t j = 0; j < table_columns->size(); ++j) {
        int property_id = j;
        std::string property_name = table_columns->GetValue(0, j).ToString();
        std::string property_type = table_columns->GetValue(1, j).ToString();
        relgo::PhysicalType property_physical_type = convertTypeRev(
            LogicalType(TransformStringToLogicalTypeId(property_type)));

        properties.emplace_back(relgo::Property(
            property_id, property_name, property_physical_type, table_id));
      }

      std::string table_type = table_types[table_name];
      if (table_type == "vertex") {
        graph_schema.table_properties[table_id] =
            std::make_shared<relgo::SchemaVertex>(table_name, table_id,
                                                  properties);
      } else {
        std::string source_id = "";
        std::string target_id = "";
        std::string source_table = "";
        std::string target_table = "";

        graph_schema.table_properties[table_id] =
            std::make_shared<relgo::SchemaEdge>(
                table_name, table_id, properties, source_id, target_id,
                source_table, target_table);
      }
    }
  }

  void loadForeignKeys() {
    auto all_constraints = context->QueryConnection(
        "SELECT table_name, constraint_type, constraint_text "
        "FROM duckdb_constraints();");
    // all_constraints->Print();
    auto all_constraints_chunk = all_constraints->Fetch();

    for (size_t i = 0; i < all_constraints_chunk->size(); ++i) {
      std::string constraint_type =
          all_constraints_chunk->GetValue(1, i).ToString();

      if (constraint_type != "FOREIGN KEY")
        continue;

      std::string edge_table_name =
          all_constraints_chunk->GetValue(0, i).ToString();
      std::string constraint_text =
          all_constraints_chunk->GetValue(2, i).ToString();

      std::regex pattern(
          R"(FOREIGN\s+KEY\s*\(\s*(.*?)\s*\)\s*REFERENCES\s+(\S+)\s*\(\s*(.*?)\s*\))",
          std::regex::icase);

      std::string edge_column_name = "";
      std::string vertex_table_name = "";
      std::string vertex_column_name = "";
      std::smatch matches;
      if (std::regex_search(constraint_text, matches, pattern) &&
          matches.size() == 4) {
        edge_column_name = trim(matches[1].str());
        vertex_table_name = trim(matches[2].str());
        vertex_column_name = trim(matches[3].str());
      } else {
        std::cerr << "Invalid FOREIGN KEY format" << std::endl;
      }

      std::string edge_table_id = graph_schema.table_name2id[edge_table_name];
      std::string vertex_table_id =
          graph_schema.table_name2id[vertex_table_name];
      int edge_column_index =
          graph_schema.table_properties[edge_table_id]->getPropertyColIndex(
              edge_column_name);
      int vertex_column_index =
          graph_schema.table_properties[vertex_table_id]->getPropertyColIndex(
              vertex_column_name);
      relgo::PhysicalType data_type =
          graph_schema.table_properties[edge_table_id]
              ->properties[edge_column_index]
              .type;
      graph_index_manager.graph_index_map.addJoinPair(
          edge_table_id, edge_column_name, edge_column_index, vertex_table_id,
          vertex_column_name, vertex_column_index, data_type, false);
      graph_index_manager.graph_index_map.addJoinPair(
          vertex_table_id, vertex_column_name, vertex_column_index,
          edge_table_id, edge_column_name, edge_column_index, data_type, false);

      // graph_schema.table_element_type[edge_table_id] = "edge";
      // graph_schema.table_element_type[vertex_table_id] = "vertex";
      // graph_schema.edge_adjacency[edge_table_id] =
      //     std::make_pair(vertex_table_name, vertex_table_name);
    }
  }

  void loadGraphIndex() {
    context->transaction.BeginTransaction();
    context->transaction.SetAutoCommit(false);

    for (const auto &table_name : graph_schema.table_names) {
      auto table_or_view =
          Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                            table_name, OnEntryNotFound::RETURN_NULL);
      auto &table_instance = table_or_view->Cast<TableCatalogEntry>();
      auto &table_indexes = table_instance.GetStorage().info->indexes.Indexes();

      for (size_t j = 0; j < table_indexes.size(); ++j) {
        auto &rai = table_indexes[j];
        if (rai->type != duckdb::IndexType::EXTENSION)
          continue;

        auto table_rai_index = dynamic_cast<PhysicalGraphIndex *>(rai.get());
        auto &table_rai = table_rai_index->graph_index;
        std::string edge_table_name = table_rai->table_name;
        std::string edge_table_id = graph_schema.table_name2id[edge_table_name];

        std::string index_type = "undirected";
        if (table_rai->referenced_tables[1] == edge_table_name) {
          index_type = "pkfk";
        } else if (table_rai->referenced_tables[0] ==
                   table_rai->referenced_tables[1]) {
          index_type = "self";
        }

        for (size_t dir = 0; dir < 2; ++dir) {
          int edge_column_index_with_index =
              index_type != "pkfk"
                  ? relgo::COLUMN_IDENTIFIER_GRAPH_INDEX_ID + 2 * j + (1 - dir)
                  : relgo::COLUMN_IDENTIFIER_GRAPH_INDEX_ID + 2 * j;
          std::string edge_column_name =
              graph_schema.table_properties[edge_table_id]
                  ->properties[table_rai->column_ids[dir]]
                  .name +
              "_rowid";

          idx_t vertex_column_index = COLUMN_IDENTIFIER_ROW_ID;
          std::string vertex_table_name = table_rai->referenced_tables[dir];
          std::string vertex_table_id =
              graph_schema.table_name2id[vertex_table_name];
          std::string vertex_column_name = vertex_table_name + "_rowid";

          if (index_type == "pkfk" && vertex_table_name == edge_table_name)
            continue;

          if (index_type != "pkfk") {
            graph_schema.table_element_type[edge_table_id] = "edge";
            graph_schema.table_element_type[vertex_table_id] = "vertex";
            graph_schema.edge_adjacency[edge_table_id] =
                std::make_pair(table_rai->referenced_tables[0],
                               table_rai->referenced_tables[1]);
          }

          if (dir == 0) {
            graph_index_manager.graph_index_map.addJoinPair(
                vertex_table_id, vertex_column_name, vertex_column_index,
                edge_table_id, edge_column_name, edge_column_index_with_index,
                relgo::PhysicalType::INT64, true,
                relgo::JoinDirection::EDGE_SOURCE,
                relgo::JoinIndexType::ADJ_JOIN);

            if (index_type != "self" && index_type != "pkfk")
              graph_index_manager.graph_index_map.addJoinPair(
                  edge_table_id, edge_column_name, edge_column_index_with_index,
                  vertex_table_id, vertex_column_name, vertex_column_index,
                  relgo::PhysicalType::INT64, true,
                  relgo::JoinDirection::SOURCE_EDGE,
                  relgo::JoinIndexType::ADJ_JOIN);
          } else {
            if (index_type != "self" && index_type != "pkfk")
              graph_index_manager.graph_index_map.addJoinPair(
                  vertex_table_id, vertex_column_name, vertex_column_index,
                  edge_table_id, edge_column_name, edge_column_index_with_index,
                  relgo::PhysicalType::INT64, true,
                  relgo::JoinDirection::EDGE_TARGET,
                  relgo::JoinIndexType::ADJ_JOIN);

            graph_index_manager.graph_index_map.addJoinPair(
                edge_table_id, edge_column_name, edge_column_index_with_index,
                vertex_table_id, vertex_column_name, vertex_column_index,
                relgo::PhysicalType::INT64, true,
                relgo::JoinDirection::TARGET_EDGE,
                relgo::JoinIndexType::ADJ_JOIN);
          }
        }

        // index for merge sip join
        if (index_type == "undirected" || index_type == "self") {
          idx_t source_vertex_column_index = COLUMN_IDENTIFIER_ROW_ID;
          std::string source_vertex_table_name =
              table_rai->referenced_tables[0];
          std::string source_vertex_table_id =
              graph_schema.table_name2id[source_vertex_table_name];
          std::string source_vertex_column_name =
              source_vertex_table_name + "_rowid";

          idx_t target_vertex_column_index = COLUMN_IDENTIFIER_ROW_ID;
          std::string target_vertex_table_name =
              table_rai->referenced_tables[1];
          std::string target_vertex_table_id =
              graph_schema.table_name2id[target_vertex_table_name];
          std::string target_vertex_column_name =
              target_vertex_table_name + "_rowid";

          if (source_vertex_table_id != target_vertex_table_id) {
            graph_index_manager.graph_index_map.addJoinPair(
                source_vertex_table_id, source_vertex_column_name,
                source_vertex_column_index, target_vertex_table_id,
                target_vertex_column_name, target_vertex_column_index,
                relgo::PhysicalType::INT64, true,
                relgo::JoinDirection::TARGET_EDGE,
                relgo::JoinIndexType::NEIGHBOR_JOIN, edge_table_id);

            if (index_type != "self")
              graph_index_manager.graph_index_map.addJoinPair(
                  target_vertex_table_id, target_vertex_column_name,
                  target_vertex_column_index, source_vertex_table_id,
                  source_vertex_column_name, source_vertex_column_index,
                  relgo::PhysicalType::INT64, true,
                  relgo::JoinDirection::SOURCE_EDGE,
                  relgo::JoinIndexType::NEIGHBOR_JOIN, edge_table_id);
          } else {
            graph_index_manager.graph_index_map.addJoinPair(
                source_vertex_table_id, source_vertex_column_name,
                source_vertex_column_index, target_vertex_table_id,
                target_vertex_column_name, target_vertex_column_index,
                relgo::PhysicalType::INT64, true, relgo::JoinDirection::UNKNOWN,
                relgo::JoinIndexType::NEIGHBOR_JOIN, edge_table_id);
          }
        }
      }
    }

    context->transaction.Commit();
  }

  relgo::PhysicalType convertTypeRev(LogicalType type) {
    switch (type.id()) {
    case LogicalTypeId::UTINYINT:
      return relgo::PhysicalType::UINT8;
    case LogicalTypeId::USMALLINT:
      return relgo::PhysicalType::UINT16;
    case LogicalTypeId::UINTEGER:
      return relgo::PhysicalType::UINT32;
    case LogicalTypeId::UBIGINT:
      return relgo::PhysicalType::UINT64;
    case LogicalTypeId::TINYINT:
      return relgo::PhysicalType::INT8;
    case LogicalTypeId::SMALLINT:
      return relgo::PhysicalType::INT16;
    case LogicalTypeId::INTEGER:
      return relgo::PhysicalType::INT32;
    case LogicalTypeId::BIGINT:
      return relgo::PhysicalType::INT64;
    case LogicalTypeId::FLOAT:
      return relgo::PhysicalType::FLOAT;
    case LogicalTypeId::DOUBLE:
      return relgo::PhysicalType::DOUBLE;
    case LogicalTypeId::VARCHAR:
      return relgo::PhysicalType::VARCHAR;
    case LogicalTypeId::BOOLEAN:
      return relgo::PhysicalType::BOOL;
    case LogicalTypeId::INVALID:
      return relgo::PhysicalType::INVALID;
    default:
      std::cerr << "Unsupported LogicalType: " << std::endl;
      return relgo::PhysicalType::INVALID;
    }
  }

  LogicalType convertType(relgo::PhysicalType type) {
    if (type == relgo::PhysicalType::UINT8)
      return LogicalType::UTINYINT;
    else if (type == relgo::PhysicalType::UINT16)
      return LogicalType::USMALLINT;
    else if (type == relgo::PhysicalType::UINT32)
      return LogicalType::UINTEGER;
    else if (type == relgo::PhysicalType::UINT64)
      return LogicalType::UBIGINT;
    else if (type == relgo::PhysicalType::INT8)
      return LogicalType::TINYINT;
    else if (type == relgo::PhysicalType::INT16)
      return LogicalType::SMALLINT;
    else if (type == relgo::PhysicalType::INT32)
      return LogicalType::INTEGER;
    else if (type == relgo::PhysicalType::INT64)
      return LogicalType::BIGINT;
    else if (type == relgo::PhysicalType::FLOAT)
      return LogicalType::FLOAT;
    else if (type == relgo::PhysicalType::DOUBLE)
      return LogicalType::DOUBLE;
    else if (type == relgo::PhysicalType::VARCHAR)
      return LogicalType::VARCHAR;
    else if (type == relgo::PhysicalType::BOOL)
      return LogicalType::BOOLEAN;
    else if (type == relgo::PhysicalType::INVALID)
      return LogicalType::INVALID;

    std::cout << "This type is not processed " << std::endl;
    return LogicalType::UNKNOWN;
  }

  ExpressionType convertExprType(relgo::TripletCompType type) {
    if (type == relgo::TripletCompType::EQ)
      return ExpressionType::COMPARE_EQUAL;
    else if (type == relgo::TripletCompType::NE)
      return ExpressionType::COMPARE_NOTEQUAL;
    else if (type == relgo::TripletCompType::GT)
      return ExpressionType::COMPARE_GREATERTHAN;
    else if (type == relgo::TripletCompType::GE)
      return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
    else if (type == relgo::TripletCompType::LT)
      return ExpressionType::COMPARE_LESSTHAN;
    else if (type == relgo::TripletCompType::LE)
      return ExpressionType::COMPARE_LESSTHANOREQUALTO;
    else if (type == relgo::TripletCompType::ISNULL)
      return ExpressionType::OPERATOR_IS_NULL;
    else
      std::cout << "TripletCompType " << relgo::fromTripletCompTypeToStr(type)
                << " is not support yet" << std::endl;

    return ExpressionType::INVALID;
  }

  Value convertValue(relgo::Value val) {
    if (val.type() == relgo::PhysicalType::UINT8) {
      return Value::UTINYINT(val.GetValueUnsafe<u_int8_t>());
    } else if (val.type() == relgo::PhysicalType::UINT16) {
      return Value::USMALLINT(val.GetValueUnsafe<u_int16_t>());
    } else if (val.type() == relgo::PhysicalType::UINT32) {
      return Value::UINTEGER(val.GetValueUnsafe<u_int32_t>());
    } else if (val.type() == relgo::PhysicalType::UINT64) {
      return Value::UBIGINT(val.GetValueUnsafe<u_int64_t>());
    } else if (val.type() == relgo::PhysicalType::INT8) {
      return Value::TINYINT(val.GetValueUnsafe<int8_t>());
    } else if (val.type() == relgo::PhysicalType::INT16) {
      return Value::SMALLINT(val.GetValueUnsafe<int16_t>());
    } else if (val.type() == relgo::PhysicalType::INT32) {
      return Value::INTEGER(val.GetValueUnsafe<int32_t>());
    } else if (val.type() == relgo::PhysicalType::INT64) {
      return Value::BIGINT(val.GetValueUnsafe<int64_t>());
    } else if (val.type() == relgo::PhysicalType::VARCHAR) {
      return Value(val.ToString());
    }

    std::cout << "value type not supported yet" << std::endl;
    return Value(-1);
  }

  duckdb::JoinType convertJoinType(relgo::HashJoinType type) {
    switch (type) {
    case relgo::HashJoinType::LEFT_JOIN:
      return JoinType::LEFT;
    case relgo::HashJoinType::RIGHT_JOIN:
      return JoinType::RIGHT;
    case relgo::HashJoinType::INNER_JOIN:
      return JoinType::INNER;
    default:
      return JoinType::INNER;
    }
  }

  unique_ptr<duckdb::PhysicalOperator>
  convert(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type == relgo::PhysicalOperatorType::PHYSICAL_SCAN_TYPE) {
      return convertScan(move(op));
    } else if (op->op_type ==
               relgo::PhysicalOperatorType::PHYSICAL_PROJECT_TYPE) {
      return convertProject(move(op));
    } else if (op->op_type ==
               relgo::PhysicalOperatorType::PHYSICAL_AGGREGATE_TYPE) {
      return convertAggregate(move(op));
    } else if (op->op_type ==
               relgo::PhysicalOperatorType::PHYSICAL_HASH_JOIN_TYPE) {
      return convertHashJoin(move(op));
    } else if (op->op_type ==
               relgo::PhysicalOperatorType::PHYSICAL_ADJ_JOIN_TYPE) {
      return convertAdjJoin(move(op));
    } else if (op->op_type ==
               relgo::PhysicalOperatorType::PHYSICAL_NEIGHBOR_JOIN_TYPE) {
      return convertNeighborJoin(move(op));
    } else if (op->op_type == relgo::PhysicalOperatorType::
                                  PHYSICAL_EXTEND_INTERSECT_JOIN_TYPE) {
      return convertExtendIntersesctJoin(move(op));
    } else if (op->op_type ==
               relgo::PhysicalOperatorType::PHYSICAL_SELECT_TYPE) {
      return convertSelect(move(op));
    } else {
      std::cerr << "Unsolved physical operator for convertion" << std::endl;
    }
  }

  //========================== SCAN Convertor ===========================
public:
  unique_ptr<LogicalIndexedGet>
  getGetOperator(ClientContext &context, TableCatalogEntry &table,
                 string &alias, idx_t table_index,
                 vector<LogicalType> &table_types) {
    auto &catalog = Catalog::GetSystemCatalog(context);
    DuckTableEntry &duck_table = dynamic_cast<DuckTableEntry &>(table);
    unique_ptr<FunctionData> bind_data =
        make_uniq<TableScanBindData>(duck_table);
    auto indexed_scan_function = IndexedTableScanFunction::GetFunction();
    vector<string> table_names;
    vector<TableColumnType> table_categories;
    vector<string> column_name_alias;

    vector<LogicalType> return_types;
    vector<string> return_names;
    for (auto &col : table.GetColumns().Logical()) {
      table_types.push_back(col.Type());
      table_names.push_back(col.Name());
      return_types.push_back(col.Type());
      return_names.push_back(col.Name());
    }
    table_names =
        BindContext::AliasColumnNames(alias, table_names, column_name_alias);

    auto logical_get = make_uniq<LogicalIndexedGet>(
        table_index, indexed_scan_function, std::move(bind_data),
        std::move(return_types), std::move(return_names));

    // context->AddBaseTable(table_index, alias, table_names, table_types,
    // logical_get->column_ids,
    //                          logical_get->GetTable().get());

    return logical_get;
  }

  unique_ptr<TableFilter>
  parse_filter(std::shared_ptr<relgo::ConditionNode> current_node,
               std::string &col_name) {
    if (!current_node)
      return nullptr;
    if (current_node->node_type == relgo::ConditionNodeType::OPERATOR_NODE) {
      relgo::ConditionOperatorNode *cur_node =
          dynamic_cast<relgo::ConditionOperatorNode *>(current_node.get());
      if (cur_node->opType == relgo::TripletCompType::AND) {
        unique_ptr<ConjunctionAndFilter> and_filter =
            duckdb::make_uniq<ConjunctionAndFilter>();
        auto first_child = parse_filter(cur_node->operands[0], col_name);
        auto second_child = parse_filter(cur_node->operands[1], col_name);
        if (first_child && second_child) {
          and_filter->child_filters.push_back(move(first_child));
          and_filter->child_filters.push_back(move(second_child));
          return move(and_filter);
        } else if (first_child && !second_child) {
          return first_child;
        } else if (!first_child && second_child) {
          return second_child;
        } else
          return nullptr;
      } else if (cur_node->opType == relgo::TripletCompType::OR) {
        unique_ptr<ConjunctionOrFilter> or_filter =
            duckdb::make_uniq<ConjunctionOrFilter>();
        auto first_child = parse_filter(cur_node->operands[0], col_name);
        auto second_child = parse_filter(cur_node->operands[1], col_name);
        if (first_child && second_child) {
          or_filter->child_filters.push_back(move(first_child));
          or_filter->child_filters.push_back(move(second_child));
          return move(or_filter);
        } else if (first_child && !second_child) {
          return first_child;
        } else if (!first_child && second_child) {
          return second_child;
        } else
          return nullptr;
      } else if (cur_node->opType == relgo::TripletCompType::EQ ||
                 cur_node->opType == relgo::TripletCompType::NE ||
                 cur_node->opType == relgo::TripletCompType::LT ||
                 cur_node->opType == relgo::TripletCompType::LE ||
                 cur_node->opType == relgo::TripletCompType::GT ||
                 cur_node->opType == relgo::TripletCompType::GE) {
        std::shared_ptr<relgo::ConditionNode> left_node = cur_node->operands[0];
        std::shared_ptr<relgo::ConditionNode> right_node =
            cur_node->operands[1];
        if (left_node->node_type ==
                relgo::ConditionNodeType::CONST_VALUE_NODE &&
            right_node->node_type ==
                relgo::ConditionNodeType::CONST_VARIABLE_NODE) {
          relgo::ConditionConstNode *var_node =
              dynamic_cast<relgo::ConditionConstNode *>(right_node.get());
          if (var_node->field == col_name) {
            relgo::ConditionConstNode *val_node =
                dynamic_cast<relgo::ConditionConstNode *>(left_node.get());
            unique_ptr<ConstantFilter> constant_filter =
                duckdb::make_uniq<ConstantFilter>(
                    convertExprType(cur_node->opType),
                    convertValue(val_node->value));
            return move(constant_filter);
          } else
            return nullptr;
        } else if (left_node->node_type ==
                       relgo::ConditionNodeType::CONST_VARIABLE_NODE &&
                   right_node->node_type ==
                       relgo::ConditionNodeType::CONST_VALUE_NODE) {
          relgo::ConditionConstNode *var_node =
              dynamic_cast<relgo::ConditionConstNode *>(left_node.get());
          if (var_node->field == col_name) {
            relgo::ConditionConstNode *val_node =
                dynamic_cast<relgo::ConditionConstNode *>(right_node.get());
            unique_ptr<ConstantFilter> constant_filter =
                duckdb::make_uniq<ConstantFilter>(
                    convertExprType(cur_node->opType),
                    convertValue(val_node->value));
            return move(constant_filter);
          } else
            return nullptr;
        } else
          return nullptr;
      }
    } else {
      return nullptr;
    }
  }

  unique_ptr<duckdb::Expression>
  parse_filter(std::shared_ptr<relgo::ConditionNode> current_node) {
    if (!current_node)
      return nullptr;
    if (current_node->node_type == relgo::ConditionNodeType::OPERATOR_NODE) {
      relgo::ConditionOperatorNode *cur_node =
          dynamic_cast<relgo::ConditionOperatorNode *>(current_node.get());
      if (cur_node->opType == relgo::TripletCompType::AND) {
        auto and_conjunction = make_uniq<BoundConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND);
        auto first_child = parse_filter(cur_node->operands[0]);
        auto second_child = parse_filter(cur_node->operands[1]);

        and_conjunction->children.push_back(move(first_child));
        and_conjunction->children.push_back(move(second_child));

        return move(and_conjunction);
      } else if (cur_node->opType == relgo::TripletCompType::OR) {
        auto or_conjunction = make_uniq<BoundConjunctionExpression>(
            ExpressionType::CONJUNCTION_OR);
        auto first_child = parse_filter(cur_node->operands[0]);
        auto second_child = parse_filter(cur_node->operands[1]);

        or_conjunction->children.push_back(move(first_child));
        or_conjunction->children.push_back(move(second_child));

        return move(or_conjunction);
      } else if (cur_node->opType == relgo::TripletCompType::EQ ||
                 cur_node->opType == relgo::TripletCompType::NE ||
                 cur_node->opType == relgo::TripletCompType::LT ||
                 cur_node->opType == relgo::TripletCompType::LE ||
                 cur_node->opType == relgo::TripletCompType::GT ||
                 cur_node->opType == relgo::TripletCompType::GE) {
        std::shared_ptr<relgo::ConditionNode> left_node = cur_node->operands[0];
        std::shared_ptr<relgo::ConditionNode> right_node =
            cur_node->operands[1];
        if (left_node->node_type ==
                relgo::ConditionNodeType::CONST_VALUE_NODE &&
            right_node->node_type ==
                relgo::ConditionNodeType::CONST_VARIABLE_NODE) {
          relgo::ConditionConstNode *val_node =
              dynamic_cast<relgo::ConditionConstNode *>(left_node.get());
          relgo::ConditionConstNode *var_node =
              dynamic_cast<relgo::ConditionConstNode *>(right_node.get());

          auto const_left =
              make_uniq<BoundConstantExpression>(convertValue(val_node->value));
          auto const_right = make_uniq<BoundReferenceExpression>(
              var_node->field, convertType(var_node->field_type),
              var_node->field_index);

          auto const_filter = duckdb::make_uniq<BoundComparisonExpression>(
              convertExprType(cur_node->opType), move(const_left),
              move(const_right));
          return move(const_filter);
        } else if (left_node->node_type ==
                       relgo::ConditionNodeType::CONST_VARIABLE_NODE &&
                   right_node->node_type ==
                       relgo::ConditionNodeType::CONST_VALUE_NODE) {
          relgo::ConditionConstNode *var_node =
              dynamic_cast<relgo::ConditionConstNode *>(left_node.get());
          relgo::ConditionConstNode *val_node =
              dynamic_cast<relgo::ConditionConstNode *>(right_node.get());

          auto const_left = make_uniq<BoundReferenceExpression>(
              var_node->field, convertType(var_node->field_type),
              var_node->field_index);
          auto const_right =
              make_uniq<BoundConstantExpression>(convertValue(val_node->value));

          auto const_filter = duckdb::make_uniq<BoundComparisonExpression>(
              convertExprType(cur_node->opType), move(const_left),
              move(const_right));
          return move(const_filter);
        } else if (left_node->node_type ==
                       relgo::ConditionNodeType::CONST_VARIABLE_NODE &&
                   right_node->node_type ==
                       relgo::ConditionNodeType::CONST_VARIABLE_NODE) {
          relgo::ConditionConstNode *var_node_left =
              dynamic_cast<relgo::ConditionConstNode *>(left_node.get());
          relgo::ConditionConstNode *var_node_right =
              dynamic_cast<relgo::ConditionConstNode *>(right_node.get());

          auto const_left = make_uniq<BoundReferenceExpression>(
              var_node_left->field, convertType(var_node_left->field_type),
              var_node_left->field_index);
          auto const_right = make_uniq<BoundReferenceExpression>(
              var_node_right->field, convertType(var_node_right->field_type),
              var_node_right->field_index);

          auto const_filter = duckdb::make_uniq<BoundComparisonExpression>(
              convertExprType(cur_node->opType), move(const_left),
              move(const_right));
          return move(const_filter);
        } else {
          relgo::ConditionConstNode *val_node_left =
              dynamic_cast<relgo::ConditionConstNode *>(left_node.get());
          relgo::ConditionConstNode *val_node_right =
              dynamic_cast<relgo::ConditionConstNode *>(right_node.get());

          auto const_left = make_uniq<BoundConstantExpression>(
              convertValue(val_node_left->value));
          auto const_right = make_uniq<BoundConstantExpression>(
              convertValue(val_node_right->value));

          auto const_filter = duckdb::make_uniq<BoundComparisonExpression>(
              convertExprType(cur_node->opType), move(const_left),
              move(const_right));
          return move(const_filter);
        }
      }
    } else {
      return nullptr;
    }
  }

  duckdb::unique_ptr<duckdb::PhysicalOperator>
  convertScan(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type != relgo::PhysicalOperatorType::PHYSICAL_SCAN_TYPE) {
      std::cout << "PhysicalTableScan is not matched with this operator"
                << std::endl;
      return nullptr;
    }

    relgo::PhysicalScan *scan_node =
        dynamic_cast<relgo::PhysicalScan *>(op.get());

    std::string table_name = scan_node->table_name;
    int table_index = scan_node->table_alias;

    auto table_or_view =
        Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                          table_name, OnEntryNotFound::RETURN_NULL);
    auto &table_instance = table_or_view->Cast<TableCatalogEntry>();

    vector<idx_t> column_ids;
    vector<LogicalType> get_column_types;

    for (size_t i = 0; i < scan_node->column_ids.size(); ++i) {
      column_ids.push_back(scan_node->column_ids[i]);
      get_column_types.push_back(convertType(scan_node->column_types[i]));
    }
    std::string alias_table =
        scan_node->table_name + std::to_string(scan_node->table_alias);
    vector<LogicalType> table_types;
    vector<unique_ptr<Expression>> filter_table;
    unique_ptr<LogicalIndexedGet> get_op_table = move(getGetOperator(
        *context, table_instance, alias_table, table_index, table_types));
    unique_ptr<TableFilterSet> table_filters = NULL;

    if (scan_node->filter_conditions) {
      table_filters = make_uniq<TableFilterSet>();
      std::shared_ptr<relgo::ConditionNode> filter_node =
          scan_node->filter_conditions;

      for (size_t i = 0; i < column_ids.size(); ++i) {
        std::string column_name = scan_node->column_names[i];
        unique_ptr<TableFilter> result = parse_filter(filter_node, column_name);
        if (result == nullptr)
          continue;

        table_filters->filters[i] = move(result);
      }
    }

    duckdb::unique_ptr<PhysicalIndexedTableScan> scan_op =
        make_uniq<PhysicalIndexedTableScan>(
            get_column_types, get_op_table->function, get_op_table->table_index,
            move(get_op_table->bind_data), table_types, column_ids,
            move(filter_table), vector<column_t>(), get_op_table->names,
            std::move(table_filters), get_op_table->estimated_cardinality,
            get_op_table->extra_info);

    return move(scan_op);
  }

  // ========================== Project Convertor ===========================
  duckdb::unique_ptr<duckdb::PhysicalOperator>
  convertProject(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type != relgo::PhysicalOperatorType::PHYSICAL_PROJECT_TYPE) {
      std::cout << "PhysicalProjection is not matched with this operator"
                << std::endl;
      return nullptr;
    }

    relgo::PhysicalProject *project_node =
        dynamic_cast<relgo::PhysicalProject *>(op.get());

    vector<LogicalType> result_types;
    vector<unique_ptr<Expression>> select_list;
    for (int i = 0; i < project_node->projection.size(); ++i) {
      std::shared_ptr<relgo::ConditionNode> project_info =
          project_node->projection[i];
      if (project_info->node_type ==
          relgo::ConditionNodeType::CONST_VARIABLE_NODE) {
        relgo::ConditionConstNode *project_item =
            dynamic_cast<relgo::ConditionConstNode *>(project_info.get());
        result_types.push_back(convertType(project_item->field_type));
        auto result_col = make_uniq<BoundReferenceExpression>(
            project_item->field, convertType(project_item->field_type),
            project_item->field_index);
        select_list.push_back(move(result_col));
      } else {
        std::cout << "Unsolved: project const value in PhysicalProject"
                  << std::endl;
      }
    }

    auto projection_op =
        make_uniq<PhysicalProjection>(result_types, move(select_list), 0);
    projection_op->children.push_back(move(convert(move(project_node->child))));

    return move(projection_op);
  }

  // ========================== Aggregate Convertor ===========================
  duckdb::unique_ptr<duckdb::PhysicalOperator>
  convertAggregate(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type != relgo::PhysicalOperatorType::PHYSICAL_AGGREGATE_TYPE) {
      std::cout << "PhysicalAggregate is not matched with this operator"
                << std::endl;
      return nullptr;
    }

    relgo::PhysicalAggregate *aggregate_node =
        dynamic_cast<relgo::PhysicalAggregate *>(op.get());
    vector<unique_ptr<Expression>> aggregates;
    vector<LogicalType> aggregate_types;

    for (size_t i = 0; i < aggregate_node->groupby.size(); ++i) {
      std::string agg_name =
          relgo::convertAggTypeToStr(aggregate_node->aggregate_type_list[i]);
      std::string agg_error = "";
      QueryErrorContext error_context;
      auto agg_func = Catalog::GetEntry(
          *context, CatalogType::SCALAR_FUNCTION_ENTRY, "", "", agg_name,
          OnEntryNotFound::RETURN_NULL, error_context);
      auto &agg_func_set = agg_func->Cast<AggregateFunctionCatalogEntry>();
      FunctionBinder function_binder(*context);

      vector<duckdb::LogicalType> types;
      LogicalType variable_type =
          convertType(aggregate_node->groupby[i]->variable_type);
      types.push_back(variable_type);
      idx_t best_function = function_binder.BindFunction(
          agg_name, agg_func_set.functions, types, agg_error);
      auto bound_function =
          agg_func_set.functions.GetFunctionByOffset(best_function);

      auto children = make_uniq<BoundReferenceExpression>(
          aggregate_node->groupby[i]->variable_name, variable_type, 0);
      vector<unique_ptr<Expression>> childrenlist;
      childrenlist.push_back(move(children));
      auto aggregate = function_binder.BindAggregateFunction(
          bound_function, std::move(childrenlist), nullptr,
          AggregateType::NON_DISTINCT);

      aggregates.push_back(move(aggregate));
      aggregate_types.push_back(variable_type);
    }

    auto aggregate_op = make_uniq<PhysicalUngroupedAggregate>(
        aggregate_types, move(aggregates), 0);
    aggregate_op->children.push_back(
        move(convert(move(aggregate_node->child))));

    return move(aggregate_op);
  }

  // ========================== Join Convertor ===========================
  duckdb::unique_ptr<duckdb::PhysicalOperator>
  convertHashJoin(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type != relgo::PhysicalOperatorType::PHYSICAL_HASH_JOIN_TYPE) {
      std::cout << "PhysicalJoin is not matched with this operator"
                << std::endl;
      return nullptr;
    }

    relgo::PhysicalHashJoin *join_node =
        dynamic_cast<relgo::PhysicalHashJoin *>(op.get());

    vector<JoinCondition> conditions;

    for (size_t i = 0; i < join_node->conditions.size(); ++i) {
      auto &join_node_item = join_node->conditions[i];
      relgo::ConditionOperatorNode *join_operate_node =
          dynamic_cast<relgo::ConditionOperatorNode *>(join_node_item.get());

      relgo::ConditionConstNode *join_left_node =
          dynamic_cast<relgo::ConditionConstNode *>(
              join_operate_node->operands[0].get());
      relgo::ConditionConstNode *join_right_node =
          dynamic_cast<relgo::ConditionConstNode *>(
              join_operate_node->operands[1].get());

      JoinCondition join_condition;
      join_condition.left = make_uniq<BoundReferenceExpression>(
          join_left_node->field, convertType(join_left_node->field_type),
          join_left_node->field_index);
      join_condition.right = make_uniq<BoundReferenceExpression>(
          join_right_node->field, convertType(join_right_node->field_type),
          join_right_node->field_index);
      join_condition.comparison = ExpressionType::COMPARE_EQUAL;

      conditions.push_back(move(join_condition));
    }

    JoinType join_type = convertJoinType(join_node->join_type);

    LogicalComparisonJoin join_op(join_type);
    vector<LogicalType> output_types;
    vector<idx_t> left_projection_map;
    vector<idx_t> right_projection_map;
    for (size_t i = 0; i < join_node->output_info.size(); ++i) {
      output_types.push_back(convertType(join_node->output_info[i].type));
      // right_projection_map.push_back(i);
    }

    join_op.types = output_types;
    vector<LogicalType> delim_types;
    PerfectHashJoinStats perfect_stat;
    duckdb::unique_ptr<PhysicalHashJoin> join_operation =
        make_uniq<PhysicalHashJoin>(
            join_op, move(convert(move(join_node->left))),
            move(convert(move(join_node->right))), move(conditions), join_type,
            left_projection_map, right_projection_map, delim_types, 0,
            perfect_stat);

    return move(join_operation);
  }

  duckdb::unique_ptr<duckdb::PhysicalOperator>
  convertAdjJoin(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type != relgo::PhysicalOperatorType::PHYSICAL_ADJ_JOIN_TYPE) {
      std::cout << "PhysicalJoin is not matched with this operator"
                << std::endl;
      return nullptr;
    }

    relgo::PhysicalAdjJoin *join_node =
        dynamic_cast<relgo::PhysicalAdjJoin *>(op.get());

    vector<IndexedJoinCondition> conditions;

    for (size_t i = 0; i < join_node->conditions.size(); ++i) {
      auto &join_node_item = join_node->conditions[i];
      relgo::ConditionOperatorNode *join_operate_node =
          dynamic_cast<relgo::ConditionOperatorNode *>(join_node_item.get());

      relgo::ConditionConstNode *join_left_node =
          dynamic_cast<relgo::ConditionConstNode *>(
              join_operate_node->operands[0].get());
      relgo::ConditionConstNode *join_right_node =
          dynamic_cast<relgo::ConditionConstNode *>(
              join_operate_node->operands[1].get());

      IndexedJoinCondition join_condition;
      join_condition.left = make_uniq<BoundReferenceExpression>(
          join_left_node->field, convertType(join_left_node->field_type),
          join_left_node->field_index);
      join_condition.right = make_uniq<BoundReferenceExpression>(
          join_right_node->field, convertType(join_right_node->field_type),
          join_right_node->field_index);
      join_condition.comparison = ExpressionType::COMPARE_EQUAL;

      if (i == 0) {
        auto graph_index_info =
            make_uniq<relgo::GraphIndexInfo<int64_t *, SelectionVector>>();

        if (join_node->join_direction == relgo::JoinDirection::EDGE_SOURCE) {
          graph_index_info->rai_type = relgo::GraphIndexType::EDGE_SOURCE;
          std::string edge_table_name = join_left_node->table_name;
          std::string vertex_table_name = join_right_node->table_name;
          int edge_table_index = join_left_node->table_alias;
          int vertex_table_index = join_right_node->table_alias;

          auto edge_table_or_view =
              Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                                edge_table_name, OnEntryNotFound::RETURN_NULL);
          auto &edge_table_instance =
              edge_table_or_view->Cast<TableCatalogEntry>();
          auto &table_indexes =
              edge_table_instance.GetStorage().info->indexes.Indexes();

          for (size_t j = 0; j < table_indexes.size(); ++j) {
            if (table_indexes[j]->type == duckdb::IndexType::EXTENSION) {
              auto edge_table_rai = dynamic_cast<duckdb::PhysicalGraphIndex *>(
                  table_indexes[j].get());
              if (edge_table_rai->graph_index->referenced_tables[0] ==
                  vertex_table_name) {
                graph_index_info->rai = edge_table_rai->graph_index.get();
                break;
              }
            }
          }

          graph_index_info->forward = true;
          graph_index_info->vertex_name = vertex_table_name;
          graph_index_info->vertex_id = vertex_table_index;
          graph_index_info->passing_tables[0] = edge_table_index;
          graph_index_info->left_cardinalities[0] =
              edge_table_instance.GetStorage().info->cardinality;
          graph_index_info->compact_list =
              &graph_index_info->rai->alist->compact_forward_list;

        } else if (join_node->join_direction ==
                   relgo::JoinDirection::EDGE_TARGET) {
          graph_index_info->rai_type = relgo::GraphIndexType::EDGE_TARGET;
          std::string edge_table_name = join_left_node->table_name;
          std::string vertex_table_name = join_right_node->table_name;
          int edge_table_index = join_left_node->table_alias;
          int vertex_table_index = join_right_node->table_alias;

          auto edge_table_or_view =
              Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                                edge_table_name, OnEntryNotFound::RETURN_NULL);
          auto &edge_table_instance =
              edge_table_or_view->Cast<TableCatalogEntry>();
          auto &table_indexes =
              edge_table_instance.GetStorage().info->indexes.Indexes();

          for (size_t j = 0; j < table_indexes.size(); ++j) {
            if (table_indexes[j]->type == duckdb::IndexType::EXTENSION) {
              auto edge_table_rai = dynamic_cast<duckdb::PhysicalGraphIndex *>(
                  table_indexes[j].get());
              if (edge_table_rai->graph_index->referenced_tables[1] ==
                  vertex_table_name) {
                graph_index_info->rai = edge_table_rai->graph_index.get();
                break;
              }
            }
          }

          graph_index_info->forward = false;
          graph_index_info->vertex_name = vertex_table_name;
          graph_index_info->vertex_id = vertex_table_index;
          graph_index_info->passing_tables[0] = edge_table_index;
          graph_index_info->left_cardinalities[0] =
              edge_table_instance.GetStorage().info->cardinality;
          graph_index_info->compact_list =
              &graph_index_info->rai->alist->compact_backward_list;
        } else if (join_node->join_direction ==
                   relgo::JoinDirection::SOURCE_EDGE) {
          graph_index_info->rai_type = relgo::GraphIndexType::SOURCE_EDGE;

          std::string edge_table_name = join_right_node->table_name;
          std::string vertex_table_name = join_left_node->table_name;
          int edge_table_index = join_right_node->table_alias;
          int vertex_table_index = join_left_node->table_alias;

          auto edge_table_or_view =
              Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                                edge_table_name, OnEntryNotFound::RETURN_NULL);
          auto &edge_table_instance =
              edge_table_or_view->Cast<TableCatalogEntry>();

          auto vertex_table_or_view = Catalog::GetEntry(
              *context, CatalogType::TABLE_ENTRY, "", "", vertex_table_name,
              OnEntryNotFound::RETURN_NULL);
          auto &vertex_table_instance =
              vertex_table_or_view->Cast<TableCatalogEntry>();
          auto &table_indexes =
              edge_table_instance.GetStorage().info->indexes.Indexes();

          for (size_t j = 0; j < table_indexes.size(); ++j) {
            if (table_indexes[j]->type == duckdb::IndexType::EXTENSION) {
              auto edge_table_rai = dynamic_cast<duckdb::PhysicalGraphIndex *>(
                  table_indexes[j].get());
              if (edge_table_rai->graph_index->referenced_tables[0] ==
                  vertex_table_name) {
                graph_index_info->rai = edge_table_rai->graph_index.get();
                break;
              }
            }
          }

          graph_index_info->forward = false;
          graph_index_info->vertex_name = vertex_table_name;
          graph_index_info->vertex_id = vertex_table_index;
          graph_index_info->passing_tables[0] = vertex_table_index;
          graph_index_info->left_cardinalities[0] =
              vertex_table_instance.GetStorage().info->cardinality;

        } else if (join_node->join_direction ==
                   relgo::JoinDirection::TARGET_EDGE) {
          graph_index_info->rai_type = relgo::GraphIndexType::TARGET_EDGE;

          std::string edge_table_name = join_right_node->table_name;
          std::string vertex_table_name = join_left_node->table_name;
          int edge_table_index = join_right_node->table_alias;
          int vertex_table_index = join_left_node->table_alias;

          auto edge_table_or_view =
              Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                                edge_table_name, OnEntryNotFound::RETURN_NULL);
          auto &edge_table_instance =
              edge_table_or_view->Cast<TableCatalogEntry>();

          auto vertex_table_or_view = Catalog::GetEntry(
              *context, CatalogType::TABLE_ENTRY, "", "", vertex_table_name,
              OnEntryNotFound::RETURN_NULL);
          auto &vertex_table_instance =
              vertex_table_or_view->Cast<TableCatalogEntry>();
          auto &table_indexes =
              edge_table_instance.GetStorage().info->indexes.Indexes();

          for (size_t j = 0; j < table_indexes.size(); ++j) {
            if (table_indexes[j]->type == duckdb::IndexType::EXTENSION) {
              auto edge_table_rai = dynamic_cast<duckdb::PhysicalGraphIndex *>(
                  table_indexes[j].get());
              if (edge_table_rai->graph_index->referenced_tables[1] ==
                  vertex_table_name) {
                graph_index_info->rai = edge_table_rai->graph_index.get();
                break;
              }
            }
          }

          graph_index_info->forward = true;
          graph_index_info->vertex_name = vertex_table_name;
          graph_index_info->vertex_id = vertex_table_index;
          graph_index_info->passing_tables[0] = vertex_table_index;
          graph_index_info->left_cardinalities[0] =
              vertex_table_instance.GetStorage().info->cardinality;
        }

        join_condition.rais.push_back(move(graph_index_info));
      }
      conditions.push_back(move(join_condition));
    }

    LogicalComparisonJoin join_op(JoinType::INNER);
    vector<LogicalType> output_types;
    vector<idx_t> left_projection_map;
    vector<idx_t> right_projection_map;
    for (int i = 0; i < join_node->output_info.size(); ++i) {
      output_types.push_back(convertType(join_node->output_info[i].type));
      // right_projection_map.push_back(i);
    }

    join_op.types = output_types;
    vector<LogicalType> delim_types;
    duckdb::unique_ptr<PhysicalAdjJoin> join_operation =
        make_uniq<PhysicalAdjJoin>(
            join_op, move(convert(move(join_node->left))),
            move(convert(move(join_node->right))), move(conditions),
            JoinType::INNER, left_projection_map, right_projection_map,
            delim_types, 0);

    return move(join_operation);
  }

  duckdb::unique_ptr<duckdb::PhysicalOperator>
  convertNeighborJoin(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type !=
        relgo::PhysicalOperatorType::PHYSICAL_NEIGHBOR_JOIN_TYPE) {
      std::cout << "PhysicalJoin is not matched with this operator"
                << std::endl;
      return nullptr;
    }

    relgo::PhysicalNeighborJoin *join_node =
        dynamic_cast<relgo::PhysicalNeighborJoin *>(op.get());

    vector<IndexedJoinCondition> conditions;

    for (size_t i = 0; i < join_node->conditions.size(); ++i) {
      auto &join_node_item = join_node->conditions[i];
      relgo::ConditionOperatorNode *join_operate_node =
          dynamic_cast<relgo::ConditionOperatorNode *>(join_node_item.get());

      relgo::ConditionConstNode *join_left_node =
          dynamic_cast<relgo::ConditionConstNode *>(
              join_operate_node->operands[0].get());
      relgo::ConditionConstNode *join_right_node =
          dynamic_cast<relgo::ConditionConstNode *>(
              join_operate_node->operands[1].get());

      IndexedJoinCondition join_condition;
      join_condition.left = make_uniq<BoundReferenceExpression>(
          join_left_node->field, convertType(join_left_node->field_type),
          join_left_node->field_index);
      join_condition.right = make_uniq<BoundReferenceExpression>(
          join_right_node->field, convertType(join_right_node->field_type),
          join_right_node->field_index);
      join_condition.comparison = ExpressionType::COMPARE_EQUAL;

      auto graph_index_info =
          make_uniq<relgo::GraphIndexInfo<int64_t *, SelectionVector>>();

      if (join_node->join_direction == relgo::JoinDirection::SOURCE_EDGE) {
        graph_index_info->rai_type = relgo::GraphIndexType::SOURCE_EDGE;

        std::string edge_table_name = join_node->edge_table_name;
        std::string vertex_table_name = join_left_node->table_name;
        int vertex_table_index = join_left_node->table_alias;

        auto edge_table_or_view =
            Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                              edge_table_name, OnEntryNotFound::RETURN_NULL);
        auto &edge_table_instance =
            edge_table_or_view->Cast<TableCatalogEntry>();

        auto vertex_table_or_view =
            Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                              vertex_table_name, OnEntryNotFound::RETURN_NULL);
        auto &vertex_table_instance =
            vertex_table_or_view->Cast<TableCatalogEntry>();
        auto &table_indexes =
            edge_table_instance.GetStorage().info->indexes.Indexes();

        for (size_t j = 0; j < table_indexes.size(); ++j) {
          if (table_indexes[j]->type == duckdb::IndexType::EXTENSION) {
            auto edge_table_rai = dynamic_cast<duckdb::PhysicalGraphIndex *>(
                table_indexes[j].get());
            if (edge_table_rai->graph_index->referenced_tables[0] ==
                vertex_table_name) {
              graph_index_info->rai = edge_table_rai->graph_index.get();
              break;
            }
          }
        }

        graph_index_info->forward = false;
        graph_index_info->vertex_name = vertex_table_name;
        graph_index_info->vertex_id = vertex_table_index;
        graph_index_info->passing_tables[0] = vertex_table_index;
        graph_index_info->left_cardinalities[0] =
            vertex_table_instance.GetStorage().info->cardinality;

      } else if (join_node->join_direction ==
                 relgo::JoinDirection::TARGET_EDGE) {
        graph_index_info->rai_type = relgo::GraphIndexType::TARGET_EDGE;

        std::string edge_table_name = join_node->edge_table_name;
        std::string vertex_table_name = join_left_node->table_name;
        int vertex_table_index = join_left_node->table_alias;

        auto edge_table_or_view =
            Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                              edge_table_name, OnEntryNotFound::RETURN_NULL);
        auto &edge_table_instance =
            edge_table_or_view->Cast<TableCatalogEntry>();

        auto vertex_table_or_view =
            Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                              vertex_table_name, OnEntryNotFound::RETURN_NULL);
        auto &vertex_table_instance =
            vertex_table_or_view->Cast<TableCatalogEntry>();
        auto &table_indexes =
            edge_table_instance.GetStorage().info->indexes.Indexes();

        for (size_t j = 0; j < table_indexes.size(); ++j) {
          if (table_indexes[j]->type == duckdb::IndexType::EXTENSION) {
            auto edge_table_rai = dynamic_cast<duckdb::PhysicalGraphIndex *>(
                table_indexes[j].get());
            if (edge_table_rai->graph_index->referenced_tables[1] ==
                vertex_table_name) {
              graph_index_info->rai = edge_table_rai->graph_index.get();
              break;
            }
          }
        }

        graph_index_info->forward = true;
        graph_index_info->vertex_name = vertex_table_name;
        graph_index_info->vertex_id = vertex_table_index;
        graph_index_info->passing_tables[0] = vertex_table_index;
        graph_index_info->left_cardinalities[0] =
            vertex_table_instance.GetStorage().info->cardinality;
      }

      join_condition.rais.push_back(move(graph_index_info));
      conditions.push_back(move(join_condition));
    }

    LogicalComparisonJoin join_op(JoinType::INNER);
    vector<LogicalType> output_types;
    vector<idx_t> left_projection_map;
    vector<idx_t> right_projection_map;
    for (int i = 0; i < join_node->output_info.size(); ++i) {
      output_types.push_back(convertType(join_node->output_info[i].type));
      // right_projection_map.push_back(i);
    }

    join_op.types = output_types;
    vector<LogicalType> delim_types;
    vector<idx_t> merge_project_map;
    duckdb::unique_ptr<PhysicalNeighborJoin> join_operation =
        make_uniq<PhysicalNeighborJoin>(
            join_op, move(convert(move(join_node->left))),
            move(convert(move(join_node->right))), move(conditions),
            JoinType::INNER, left_projection_map, right_projection_map,
            merge_project_map, delim_types, 0);

    return move(join_operation);
  }

  duckdb::unique_ptr<duckdb::PhysicalOperator>
  convertExtendIntersesctJoin(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type !=
        relgo::PhysicalOperatorType::PHYSICAL_EXTEND_INTERSECT_JOIN_TYPE) {
      std::cout
          << "PhysicalExtendIntersectJoin is not matched with this operator"
          << std::endl;
      return nullptr;
    }

    relgo::PhysicalExtendIntersectJoin *join_node =
        dynamic_cast<relgo::PhysicalExtendIntersectJoin *>(op.get());

    vector<IndexedJoinCondition> merge_conditions, conditions;

    for (size_t i = 0; i < join_node->merge_conditions.size(); ++i) {
      relgo::PhysicalNeighborJoin *neighbor_join =
          dynamic_cast<relgo::PhysicalNeighborJoin *>(
              join_node->sub_operators[i].get());

      auto &join_node_item = join_node->merge_conditions[i];
      relgo::ConditionOperatorNode *join_operate_node =
          dynamic_cast<relgo::ConditionOperatorNode *>(join_node_item.get());

      relgo::ConditionConstNode *join_left_node =
          dynamic_cast<relgo::ConditionConstNode *>(
              join_operate_node->operands[0].get());
      relgo::ConditionConstNode *join_right_node =
          dynamic_cast<relgo::ConditionConstNode *>(
              join_operate_node->operands[1].get());

      IndexedJoinCondition join_condition;
      join_condition.left = make_uniq<BoundReferenceExpression>(
          join_left_node->field, convertType(join_left_node->field_type),
          join_left_node->field_index);
      join_condition.right = make_uniq<BoundReferenceExpression>(
          join_right_node->field, convertType(join_right_node->field_type),
          join_right_node->field_index);
      join_condition.comparison = ExpressionType::COMPARE_EQUAL;

      auto graph_index_info =
          make_uniq<relgo::GraphIndexInfo<int64_t *, SelectionVector>>();

      if (neighbor_join->join_direction == relgo::JoinDirection::SOURCE_EDGE) {
        graph_index_info->rai_type = relgo::GraphIndexType::SOURCE_EDGE;

        std::string edge_table_name = neighbor_join->edge_table_name;
        std::string vertex_table_name = join_left_node->table_name;
        int vertex_table_index = join_left_node->table_alias;

        auto edge_table_or_view =
            Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                              edge_table_name, OnEntryNotFound::RETURN_NULL);
        auto &edge_table_instance =
            edge_table_or_view->Cast<TableCatalogEntry>();

        auto vertex_table_or_view =
            Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                              vertex_table_name, OnEntryNotFound::RETURN_NULL);
        auto &vertex_table_instance =
            vertex_table_or_view->Cast<TableCatalogEntry>();
        auto &table_indexes =
            edge_table_instance.GetStorage().info->indexes.Indexes();

        for (size_t j = 0; j < table_indexes.size(); ++j) {
          if (table_indexes[j]->type == duckdb::IndexType::EXTENSION) {
            auto edge_table_rai = dynamic_cast<duckdb::PhysicalGraphIndex *>(
                table_indexes[j].get());
            if (edge_table_rai->graph_index->referenced_tables[0] ==
                vertex_table_name) {
              graph_index_info->rai = edge_table_rai->graph_index.get();
              break;
            }
          }
        }

        graph_index_info->forward = false;
        graph_index_info->vertex_name = vertex_table_name;
        graph_index_info->vertex_id = vertex_table_index;
        graph_index_info->passing_tables[0] = vertex_table_index;
        graph_index_info->left_cardinalities[0] =
            vertex_table_instance.GetStorage().info->cardinality;

      } else if (neighbor_join->join_direction ==
                 relgo::JoinDirection::TARGET_EDGE) {
        graph_index_info->rai_type = relgo::GraphIndexType::TARGET_EDGE;

        std::string edge_table_name = neighbor_join->edge_table_name;
        std::string vertex_table_name = join_left_node->table_name;
        int vertex_table_index = join_left_node->table_alias;

        auto edge_table_or_view =
            Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                              edge_table_name, OnEntryNotFound::RETURN_NULL);
        auto &edge_table_instance =
            edge_table_or_view->Cast<TableCatalogEntry>();

        auto vertex_table_or_view =
            Catalog::GetEntry(*context, CatalogType::TABLE_ENTRY, "", "",
                              vertex_table_name, OnEntryNotFound::RETURN_NULL);
        auto &vertex_table_instance =
            vertex_table_or_view->Cast<TableCatalogEntry>();
        auto &table_indexes =
            edge_table_instance.GetStorage().info->indexes.Indexes();

        for (size_t j = 0; j < table_indexes.size(); ++j) {
          if (table_indexes[j]->type == duckdb::IndexType::EXTENSION) {
            auto edge_table_rai = dynamic_cast<duckdb::PhysicalGraphIndex *>(
                table_indexes[j].get());
            if (edge_table_rai->graph_index->referenced_tables[1] ==
                vertex_table_name) {
              graph_index_info->rai = edge_table_rai->graph_index.get();
              break;
            }
          }
        }

        graph_index_info->forward = true;
        graph_index_info->vertex_name = vertex_table_name;
        graph_index_info->vertex_id = vertex_table_index;
        graph_index_info->passing_tables[0] = vertex_table_index;
        graph_index_info->left_cardinalities[0] =
            vertex_table_instance.GetStorage().info->cardinality;
      }

      join_condition.rais.push_back(move(graph_index_info));
      merge_conditions.push_back(move(join_condition));
    }

    // TODO: conditions are not processed yet

    LogicalComparisonJoin join_op(JoinType::INNER);
    vector<LogicalType> output_types;
    vector<idx_t> left_projection_map;
    vector<idx_t> right_projection_map;
    for (int i = 0; i < join_node->output_info.size(); ++i) {
      output_types.push_back(convertType(join_node->output_info[i].type));
      // right_projection_map.push_back(i);
    }

    join_op.types = output_types;
    vector<LogicalType> delim_types;
    vector<idx_t> merge_project_map;
    duckdb::unique_ptr<PhysicalExtendIntersect> join_operation =
        make_uniq<PhysicalExtendIntersect>(
            join_op, move(convert(move(join_node->left))),
            move(convert(move(join_node->right))), move(merge_conditions),
            move(conditions), JoinType::INNER, left_projection_map,
            right_projection_map, merge_project_map, delim_types, 0);

    return move(join_operation);
  }

  // ========================== Select/Filter Convertor
  // ===========================
  duckdb::unique_ptr<duckdb::PhysicalOperator>
  convertSelect(std::unique_ptr<relgo::PhysicalOperator> op) {
    if (op->op_type != relgo::PhysicalOperatorType::PHYSICAL_SELECT_TYPE) {
      std::cout << "PhysicalSelect is not matched with this operator"
                << std::endl;
      return nullptr;
    }

    relgo::PhysicalSelect *select_node =
        dynamic_cast<relgo::PhysicalSelect *>(op.get());

    vector<LogicalType> filter_types;
    vector<unique_ptr<Expression>> select_list_filter;

    for (size_t i = 0; i < select_node->types.size(); ++i) {
      filter_types.push_back(convertType(select_node->types[i]));
    }

    std::shared_ptr<relgo::ConditionNode> filter_node =
        select_node->filter_conditions;

    select_list_filter.push_back(move(parse_filter(filter_node)));

    unique_ptr<PhysicalFilter> filter_op =
        make_uniq<PhysicalFilter>(filter_types, move(select_list_filter), 0);
    filter_op->children.push_back(move(convert(move(select_node->child))));

    return move(filter_op);
  }
};

} // namespace duckdb