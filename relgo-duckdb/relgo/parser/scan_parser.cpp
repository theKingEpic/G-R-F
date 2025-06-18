#include "scan_parser.hpp"

#include "algebra_parser.hpp"
#include "common_parser.hpp"

namespace relgo {

std::unique_ptr<PhysicalScan>
ScanParser::resolveScanParams(const algebra::QueryParams &params,
                              std::unique_ptr<PhysicalScan> scan_op,
                              int table_alias, ParseInfo &parse_info) {
  std::string table_id = "";
  std::string table_name = "";

  for (const auto &table : params.tables()) {
    Value val = CommonParser::parseNameOrId(table);

    if (val.type() == PhysicalType::VARCHAR) {
      // table name
      table_name = val.GetValueUnsafe<std::string>();
      table_id = parse_info.gs.table_name2id[table_name];

      scan_op->set_id(table_id);
    } else {
      table_id = val.ToString();
      scan_op->set_id(table_id);

      table_name = parse_info.gs.table_properties[table_id]->name;

      scan_op->set_table_name(table_name);
    }
  }

  parse_info.setCurrentTableSchema(table_id, table_alias);

  // std::unique_ptr<std::vector<Property>> columns =
  // AlgebraParser::parseColumns(params, parse_info);

  // if (params.is_all_columns()) {
  //   scan_op->is_all_columns = true;
  // } else {
  //   scan_op->is_all_columns = false;
  //   std::vector<std::string> reserved_columns;
  //   for (int i = 0; i < columns->size(); ++i) {
  //     auto &property = columns->at(i);
  //     reserved_columns.push_back(property.name);
  //   }

  //   if (!reserved_columns.empty())
  //     parse_info.project(reserved_columns);
  // }

  parse_info.appendIndex(table_alias);

  auto &properties = parse_info.current_table_schema.properties;
  for (int i = 0; i < properties.size(); ++i) {
    scan_op->append_column(properties[i].id, properties[i].name,
                           properties[i].type);
  }

  if (params.has_predicate()) {
    scan_op->filter_conditions =
        AlgebraParser::parsePredicate(params, parse_info);
  }

  return move(scan_op);
}

std::unique_ptr<PhysicalScan> ScanParser::initializeScanByDefault(
    std::unique_ptr<PhysicalScan> scan_op, std::string table_id,
    std::string table_name, int table_alias, ParseInfo &parse_info) {

  scan_op->set_id(table_id);
  scan_op->set_table_name(table_name);

  parse_info.setCurrentTableSchema(table_id, table_alias);

  parse_info.appendIndex(table_alias);

  auto &properties = parse_info.current_table_schema.properties;
  for (int i = 0; i < properties.size(); ++i) {
    scan_op->append_column(properties[i].id, properties[i].name,
                           properties[i].type);
  }

  return move(scan_op);
}

std::unique_ptr<PhysicalScan> ScanParser::parseScan(const physical::Scan &scan,
                                                    ParseInfo &parse_info) {
  std::unique_ptr<PhysicalScan> scan_op =
      std::unique_ptr<PhysicalScan>(new PhysicalScan());
  switch (scan.scan_opt()) {
  case physical::Scan::VERTEX:
    scan_op->set_type(ScanOpt::VERTEX);
    break;
  case physical::Scan::EDGE:
    scan_op->set_type(ScanOpt::EDGE);
    break;
  case physical::Scan::TABLE:
    scan_op->set_type(ScanOpt::TABLE);
    break;
  default:
    std::cerr << "Unknown ScanOpt" << std::endl;
  }

  std::unique_ptr<AliasInfo> alias_info =
      parse_info.index_manager.getAlias(scan);
  scan_op->set_alias(alias_info->current_alias);

  int scan_table_alias = scan_op->table_alias;
  scan_op = move(resolveScanParams(scan.params(), move(scan_op),
                                   scan_table_alias, parse_info));

  // 4. 解析IndexPredicate（可能不存在）
  if (scan.has_idx_predicate()) {
    std::shared_ptr<ConditionNode> pk_condition =
        AlgebraParser::parseIndexPredicate(scan.idx_predicate(), parse_info);
    if (!scan_op->filter_conditions) {
      scan_op->filter_conditions = pk_condition;
    } else {
      std::shared_ptr<ConditionOperatorNode> and_op =
          std::unique_ptr<ConditionOperatorNode>(
              new ConditionOperatorNode(TripletCompType::AND));
      and_op->addOperand(move(scan_op->filter_conditions));
      and_op->addOperand(pk_condition);
      scan_op->filter_conditions = and_op;
    }
  }

  return move(scan_op);
}

} // namespace relgo