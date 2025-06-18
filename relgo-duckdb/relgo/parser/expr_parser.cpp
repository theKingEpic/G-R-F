#include "expr_parser.hpp"

#include "basic_type_parser.hpp"
#include "common_parser.hpp"

namespace relgo {

std::unique_ptr<BraceExprOpr>
ExprParser::parseBrace(const common::ExprOpr_Brace &item,
                       ParseInfo &parse_info) {
  if (item == common::ExprOpr_Brace_LEFT_BRACE) {
    return std::unique_ptr<BraceExprOpr>(new BraceExprOpr(true));
  } else {
    return std::unique_ptr<BraceExprOpr>(new BraceExprOpr(false));
  }
}

std::unique_ptr<LogicalExprOpr>
ExprParser::parseLogical(const common::Logical &item, ParseInfo &parse_info) {
  std::string logical_name = Logical_Name(item);
  return std::unique_ptr<LogicalExprOpr>(
      new LogicalExprOpr(fromStrToTripletCompType(logical_name)));
}

std::unique_ptr<ConstExprOpr> ExprParser::parseConst(const common::Value &item,
                                                     ParseInfo &parse_info) {
  Value val = CommonParser::parseValue(item);
  return std::unique_ptr<ConstExprOpr>(new ConstExprOpr(val));
}

std::unique_ptr<VariableExprOpr>
ExprParser::parseVariable(const common::Variable &item, ParseInfo &parse_info) {
  int table_alias = -1;
  std::string table_id = "-1";
  std::string table_name = "";
  if (item.has_tag()) {
    table_alias =
        CommonParser::parseNameOrId(item.tag()).GetValueUnsafe<int32_t>();

    table_id = parse_info.index_manager.table_alias2id[table_alias];
    table_name = parse_info.gs.table_id2name[table_id];
  }

  std::string var_name = parseProperty(item.property(), parse_info);
  int var_index = parse_info.current_table_schema.getPropertyColIndex(
      var_name, "null", table_alias);

  PhysicalType var_type =
      BasicTypeParser::parseDataType(item.node_type().data_type(), parse_info)
          ->at(0)
          .data_type;

  return std::unique_ptr<VariableExprOpr>(new VariableExprOpr(
      table_id, table_name, var_name, var_type, var_index, table_alias));
}

std::vector<std::unique_ptr<VariableExprOpr>>
ExprParser::parseVariables(const common::Variable &item,
                           ParseInfo &parse_info) {
  std::vector<std::unique_ptr<VariableExprOpr>> var_expr_oprs;
  int table_alias = -1;
  std::string table_id = "-1";
  std::string table_name = "";
  if (item.has_tag()) {
    table_alias =
        CommonParser::parseNameOrId(item.tag()).GetValueUnsafe<int32_t>();

    table_id = parse_info.index_manager.table_alias2id[table_alias];
    table_name = parse_info.gs.table_id2name[table_id];
  }

  if (item.has_property()) {
    std::string var_name = parseProperty(item.property(), parse_info);
    int var_index = parse_info.current_table_schema.getPropertyColIndex(
        var_name, "null", table_alias);

    PhysicalType var_type =
        BasicTypeParser::parseDataType(item.node_type().data_type(), parse_info)
            ->at(0)
            .data_type;

    var_expr_oprs.push_back(
        std::unique_ptr<VariableExprOpr>(new VariableExprOpr(
            table_id, table_name, var_name, var_type, var_index, table_alias)));
  } else {
    auto prop_list = item.node_type().graph_type().graph_data_type(0);
    for (size_t prop_idx = 0; prop_idx < prop_list.props_size(); ++prop_idx) {
      std::string var_name = prop_list.props(prop_idx).prop_id().name();
      int var_index = parse_info.current_table_schema.getPropertyColIndex(
          var_name, "null", table_alias);

      PhysicalType var_type = BasicTypeParser::parseDataType(
                                  prop_list.props(prop_idx).type(), parse_info)
                                  ->at(0)
                                  .data_type;

      var_expr_oprs.push_back(std::unique_ptr<VariableExprOpr>(
          new VariableExprOpr(table_id, table_name, var_name, var_type,
                              var_index, table_alias)));
    }
  }

  return var_expr_oprs;
}

std::unique_ptr<VariableExprOpr>
ExprParser::parseDynamicParam(const common::DynamicParam &item,
                              ParseInfo &parse_info) {
  int table_alias = -1;
  std::string table_id = parse_info.current_table_id;
  std::string table_name = parse_info.current_table_schema.name;
  std::string var_name = item.name();
  int var_index = item.index();

  PhysicalType var_type =
      BasicTypeParser::parseDataType(item.data_type().data_type(), parse_info)
          ->at(0)
          .data_type;

  return std::unique_ptr<VariableExprOpr>(new VariableExprOpr(
      table_id, table_name, var_name, var_type, var_index, table_alias));
}

std::string ExprParser::parseProperty(const common::Property &item,
                                      ParseInfo &parse_info) {
  if (item.has_id()) {
    return "id";
  } else if (item.has_label()) {
    return "label";
  } else if (item.has_len()) {
    return "len";
  } else if (item.has_all()) {
    return "all";
  }

  return CommonParser::parseNameOrId(item.key()).ToString();
}

std::unique_ptr<ExprOpr> ExprParser::parseExprOpr(const common::ExprOpr &item,
                                                  ParseInfo &parse_info) {
  if (item.item_case() == common::ExprOpr::kConst) {
    std::unique_ptr<ConstExprOpr> result =
        parseConst(item.const_(), parse_info);
    result->expr_type =
        BasicTypeParser::parseDataType(item.node_type().data_type(), parse_info)
            ->at(0)
            .data_type;
    return result;
  } else if (item.item_case() == common::ExprOpr::kBrace) {
    std::unique_ptr<BraceExprOpr> result = parseBrace(item.brace(), parse_info);
    return result;
  } else if (item.item_case() == common::ExprOpr::kLogical) {
    std::unique_ptr<LogicalExprOpr> result =
        parseLogical(item.logical(), parse_info);
    result->expr_type =
        BasicTypeParser::parseDataType(item.node_type().data_type(), parse_info)
            ->at(0)
            .data_type;
    return result;
  } else if (item.item_case() == common::ExprOpr::kVar) {
    std::unique_ptr<VariableExprOpr> result =
        parseVariable(item.var(), parse_info);
    result->expr_type =
        BasicTypeParser::parseDataType(item.node_type().data_type(), parse_info)
            ->at(0)
            .data_type;
    return result;
  } else {
    std::cout << "Unknown expression type" << std::endl;
  }

  return std::unique_ptr<ExprOpr>(new ExprOpr());
}

} // namespace relgo