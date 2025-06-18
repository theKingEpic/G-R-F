#include "algebra_parser.hpp"

#include "common_parser.hpp"
#include "expr_parser.hpp"

namespace relgo {

std::shared_ptr<ConditionNode>
AlgebraParser::parseIndexPredicate(const algebra::IndexPredicate &idx_pred,
                                   ParseInfo &parse_info) {

  std::vector<std::shared_ptr<ConditionNode>> or_list;

  for (size_t i = 0; i < idx_pred.or_predicates_size(); ++i) {
    auto and_pred = idx_pred.or_predicates(i);
    std::vector<std::shared_ptr<ConditionNode>> and_list;

    for (size_t j = 0; j < and_pred.predicates_size(); ++j) {
      auto triplet = and_pred.predicates(j);
      bool const_type = true;
      const common::Property &key = triplet.key();
      std::string cmp = Logical_Name(triplet.cmp());

      std::shared_ptr<ConditionOperatorNode> cmp_op =
          std::unique_ptr<ConditionOperatorNode>(
              new ConditionOperatorNode(fromStrToTripletCompType(cmp)));

      std::string prop = ExprParser::parseProperty(key, parse_info);
      int id_index = 0;
      PhysicalType prop_type = PhysicalType::UNKNOWN;
      if (key.has_id() || key.has_label() || key.has_len() || key.has_all())
        id_index = -1;
      else {
        id_index = parse_info.current_table_schema.getPropertyColIndex(prop);
        prop_type = parse_info.current_table_schema.properties[id_index].type;
      }

      cmp_op->addOperand(std::make_shared<ConditionConstNode>(
          prop, prop_type, id_index, parse_info.current_table_id,
          parse_info.current_table_schema.name, -1));

      if (triplet.has_const_()) {
        std::unique_ptr<relgo::ConstExprOpr> const_val =
            ExprParser::parseConst(triplet.const_(), parse_info);

        cmp_op->addOperand(
            std::make_shared<ConditionConstNode>(const_val->value));
      } else {
        std::unique_ptr<VariableExprOpr> var =
            ExprParser::parseDynamicParam(triplet.param(), parse_info);
        cmp_op->addOperand(std::make_shared<ConditionConstNode>(
            var->variable_name, var->variable_type, var->variable_index,
            var->table_id, var->table_name, var->table_alias));
      }

      and_list.push_back(move(cmp_op));
    }

    if (and_list.size() == 1) {
      or_list.push_back(move(and_list[0]));
    } else {
      std::shared_ptr<ConditionOperatorNode> and_root =
          std::make_shared<ConditionOperatorNode>(TripletCompType::AND);
      std::shared_ptr<ConditionOperatorNode> and_current = and_root;

      for (size_t and_index = 0; and_index < and_list.size() - 1; ++and_index) {
        and_current->addOperand(move(and_list[and_index]));

        if (and_index != and_list.size() - 2) {
          std::shared_ptr<ConditionOperatorNode> and_new_root =
              std::make_shared<ConditionOperatorNode>(TripletCompType::AND);
          and_current->addOperand(and_new_root);
          and_current = and_new_root;
        } else {
          and_current->addOperand(move(and_list[and_index + 1]));
        }
      }

      or_list.push_back(move(and_root));
    }

    if (or_list.size() == 1) {
      return or_list[0];
    } else {
      std::shared_ptr<ConditionOperatorNode> or_root =
          std::make_shared<ConditionOperatorNode>(TripletCompType::OR);
      std::shared_ptr<ConditionOperatorNode> or_current = or_root;

      for (size_t or_index = 0; or_index < or_list.size() - 1; ++or_index) {
        or_current->addOperand(move(or_list[or_index]));

        if (or_index != or_list.size() - 2) {
          std::shared_ptr<ConditionOperatorNode> or_new_root =
              std::make_shared<ConditionOperatorNode>(TripletCompType::AND);
          or_current->addOperand(or_new_root);
          or_current = or_new_root;
        } else {
          or_current->addOperand(move(or_list[or_index + 1]));
        }
      }

      return or_root;
    }
  }

  return nullptr;
}

std::unique_ptr<std::vector<Property>>
AlgebraParser::parseColumns(const algebra::QueryParams &item,
                            ParseInfo &parse_info) {
  std::unique_ptr<std::vector<Property>> result =
      std::unique_ptr<std::vector<Property>>(new std::vector<Property>());

  for (const auto &col : item.columns()) {
    Value val = CommonParser::parseNameOrId(col);
    if (val.type() == PhysicalType::VARCHAR) {
      std::string column_name = val.ToString();

      int32_t column_id =
          parse_info.current_table_schema.getPropertyColIndex(column_name);
      PhysicalType column_type =
          parse_info.current_table_schema.properties[column_id].type;
      result->push_back(Property(column_id, column_name, column_type,
                                 parse_info.current_table_id));
    } else {
      int32_t column_id = val.GetValueUnsafe<int32_t>();
      std::string column_name =
          parse_info.current_table_schema.properties[column_id].name;
      PhysicalType column_type =
          parse_info.current_table_schema.properties[column_id].type;
      result->push_back(Property(column_id, column_name, column_type,
                                 parse_info.current_table_id));
    }
  }

  return move(result);
}

std::shared_ptr<ConditionNode>
AlgebraParser::parsePredicate(const algebra::QueryParams &item,
                              ParseInfo &parse_info) {
  if (!item.has_predicate()) {
    return nullptr;
  }

  return parsePredicate(item.predicate(), parse_info);
}

std::shared_ptr<ConditionNode>
AlgebraParser::parsePredicate(const common::Expression &predicate,
                              ParseInfo &parse_info) {
  std::vector<std::unique_ptr<ExprOpr>> opr_list;
  for (int i = 0; i < predicate.operators_size(); ++i) {
    std::unique_ptr<ExprOpr> opr =
        ExprParser::parseExprOpr(predicate.operators(i), parse_info);
    opr_list.push_back(move(opr));
  }

  std::shared_ptr<ConditionNode> result = parseExprOprList(opr_list);
  return result;
}

// 解析函数实现
std::shared_ptr<ConditionNode> AlgebraParser::parseExprOprList(
    const std::vector<std::unique_ptr<ExprOpr>> &tokens) {
  auto it = tokens.begin();
  return parseExprOprListExpression(it, tokens.end());
}

std::shared_ptr<ConditionNode> AlgebraParser::parseExprOprListExpression(
    std::vector<std::unique_ptr<ExprOpr>>::const_iterator &it,
    const std::vector<std::unique_ptr<ExprOpr>>::const_iterator &end,
    bool return_if_get) {
  std::shared_ptr<ConditionNode> current = nullptr;

  while (it != end) {
    const std::unique_ptr<ExprOpr> &token = *it++;

    if (token->opr_type == ExprOprType::LOGICAL) {
      LogicalExprOpr *logical_node =
          dynamic_cast<LogicalExprOpr *>(token.get());
      auto opNode =
          std::make_shared<ConditionOperatorNode>(logical_node->cmp_type);
      if (logical_node->cmp_type == TripletCompType::AND or
          logical_node->cmp_type == TripletCompType::OR) {
        if (current) {
          opNode->addOperand(current);
        }
        opNode->addOperand(parseExprOprListExpression(it, end, true));
        current = opNode;
      } else {
        opNode->addOperand(current);
        opNode->addOperand(parseExprOprListValue(it, end));
        current = opNode;

        if (return_if_get)
          break;
      }
    } else if (token->opr_type == ExprOprType::BRACE) {
      BraceExprOpr *brace_node = dynamic_cast<BraceExprOpr *>(token.get());
      if (brace_node->left) {
        current = parseExprOprListExpression(it, end);
        if (return_if_get)
          break;
      } else
        break;
    } else {
      current = parseExprOprListValue(--it, end);
    }
  }

  return current;
}

std::shared_ptr<ConditionNode> AlgebraParser::parseExprOprListValue(
    std::vector<std::unique_ptr<ExprOpr>>::const_iterator &it,
    const std::vector<std::unique_ptr<ExprOpr>>::const_iterator &end) {
  if (it != end) {
    const std::unique_ptr<ExprOpr> &token = *it++;

    if (token->opr_type == ExprOprType::CONST) {
      ConstExprOpr *const_node = dynamic_cast<ConstExprOpr *>(token.get());
      Value val = const_node->value;
      auto condNode = std::make_shared<ConditionConstNode>(val);
      return condNode;
    } else if (token->opr_type == ExprOprType::VARIABLE) {
      VariableExprOpr *var_node = dynamic_cast<VariableExprOpr *>(token.get());
      auto condNode = std::make_shared<ConditionConstNode>(
          var_node->variable_name, var_node->variable_type,
          var_node->variable_index, var_node->table_id, var_node->table_name,
          var_node->table_alias);
      return condNode;
    }
  }

  return nullptr;
}

} // namespace relgo