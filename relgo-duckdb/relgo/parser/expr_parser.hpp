#ifndef EXPR_PARSER_H
#define EXPR_PARSER_H

#include "../operators/condition_tree.hpp"
#include "../proto_generated/common.pb.h"
#include "../proto_generated/expr.pb.h"
#include "../proto_generated/physical.pb.h"
#include "../proto_generated/type.pb.h"
#include "../utils/types.hpp"
#include "../utils/value.hpp"
#include "parse_info.hpp"

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace relgo {

enum class ExprOprType { NULLTYPE, LOGICAL, CONST, BRACE, VARIABLE };

class ExprOpr {
public:
  ExprOpr(ExprOprType _opr_type = ExprOprType::NULLTYPE)
      : opr_type(_opr_type) {}
  virtual ~ExprOpr() = default;

  ExprOprType opr_type;
  PhysicalType expr_type;
};

class LogicalExprOpr : public ExprOpr {
public:
  LogicalExprOpr(TripletCompType ct)
      : ExprOpr(ExprOprType::LOGICAL), cmp_type(ct),
        cmp_type_str(fromTripletCompTypeToStr(ct)) {}

  TripletCompType cmp_type;
  std::string cmp_type_str;
};

class ConstExprOpr : public ExprOpr {
public:
  ConstExprOpr(Value _value) : ExprOpr(ExprOprType::CONST), value(_value) {}

  Value value;
};

class VariableExprOpr : public ExprOpr {
public:
  VariableExprOpr(std::string _tid, std::string _tname, std::string _vname,
                  PhysicalType _vtype, int _vindex, int _table_alias)
      : ExprOpr(ExprOprType::VARIABLE), table_id(_tid), table_name(_tname),
        variable_name(_vname), variable_type(_vtype), variable_index(_vindex),
        table_alias(_table_alias) {}

  std::string table_id;
  std::string table_name;
  std::string variable_name;
  PhysicalType variable_type;
  int variable_index;
  int table_alias;
};

class BraceExprOpr : public ExprOpr {
public:
  BraceExprOpr(bool _left) : ExprOpr(ExprOprType::BRACE), left(_left) {}

  bool left;
};

class ExprParser {
public:
  static std::unique_ptr<BraceExprOpr>
  parseBrace(const common::ExprOpr_Brace &item, ParseInfo &parse_info);
  static std::unique_ptr<LogicalExprOpr>
  parseLogical(const common::Logical &item, ParseInfo &parse_info);
  static std::unique_ptr<ConstExprOpr> parseConst(const common::Value &item,
                                                  ParseInfo &parse_info);
  static std::unique_ptr<VariableExprOpr>
  parseVariable(const common::Variable &item, ParseInfo &parse_info);
  static std::vector<std::unique_ptr<VariableExprOpr>>
  parseVariables(const common::Variable &item, ParseInfo &parse_info);
  static std::unique_ptr<VariableExprOpr>
  parseDynamicParam(const common::DynamicParam &item, ParseInfo &parse_info);
  static std::string parseProperty(const common::Property &item,
                                   ParseInfo &parse_info);
  static std::unique_ptr<ExprOpr> parseExprOpr(const common::ExprOpr &item,
                                               ParseInfo &parse_info);
};

} // namespace relgo

#endif