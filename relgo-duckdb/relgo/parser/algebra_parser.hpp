#ifndef ALGEBRA_PARSER_H
#define ALGEBRA_PARSER_H

#include "../graph/graph_schema.hpp"
#include "../operators/condition_tree.hpp"
#include "../proto_generated/algebra.pb.h"
#include "../proto_generated/common.pb.h"
#include "../proto_generated/physical.pb.h"
#include "../utils/types.hpp"
#include "../utils/value.hpp"
#include "basic_type_parser.hpp"
#include "expr_parser.hpp"
#include "parse_info.hpp"

namespace relgo {

class AlgebraParser {
public:
  static std::shared_ptr<ConditionNode>
  parseIndexPredicate(const algebra::IndexPredicate &item,
                      ParseInfo &parse_info);
  static std::unique_ptr<std::vector<Property>>
  parseColumns(const algebra::QueryParams &item, ParseInfo &parse_info);
  static std::shared_ptr<ConditionNode>
  parsePredicate(const algebra::QueryParams &item, ParseInfo &parse_info);
  static std::shared_ptr<ConditionNode>
  parsePredicate(const common::Expression &predicate, ParseInfo &parse_info);
  static std::shared_ptr<ConditionNode>
  parseExprOprList(const std::vector<std::unique_ptr<ExprOpr>> &tokens);
  static std::shared_ptr<ConditionNode> parseExprOprListExpression(
      std::vector<std::unique_ptr<ExprOpr>>::const_iterator &it,
      const std::vector<std::unique_ptr<ExprOpr>>::const_iterator &end,
      bool return_if_get = false);
  static std::shared_ptr<ConditionNode> parseExprOprListValue(
      std::vector<std::unique_ptr<ExprOpr>>::const_iterator &it,
      const std::vector<std::unique_ptr<ExprOpr>>::const_iterator &end);
};

} // namespace relgo

#endif