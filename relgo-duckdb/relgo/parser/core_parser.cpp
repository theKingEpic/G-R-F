#include "core_parser.hpp"

namespace relgo {

std::unique_ptr<PhysicalOperator>
CoreParser::parse(const physical::PhysicalPlan &plan, ParseInfo &parse_info) {
  std::unique_ptr<PhysicalOperator> intermediate_result = nullptr;
  for (int i = 0; i < plan.plan_size(); ++i) {
    const physical::PhysicalOpr oper = plan.plan(i);
    std::cout << "the card is: " << oper.card() << std::endl;
    if (oper.has_opr()) {
      physical::PhysicalOpr_Operator op = oper.opr();
      if (op.has_scan()) {
        intermediate_result =
            move(ScanParser::parseScan(op.scan(), parse_info));
        parse_info.setCard(oper.card());
      } else if (op.has_vertex()) {
        intermediate_result = move(GetVParser::parseGetV(
            op.vertex(), move(intermediate_result), parse_info));
        parse_info.setCard(oper.card());
      } else if (op.has_edge()) {
        if (op.edge().expand_opt() == physical::EdgeExpand_ExpandOpt_VERTEX &&
            i != plan.plan_size() - 1 && plan.plan(i + 1).opr().has_vertex()) {
          intermediate_result = move(EdgeExpandParser::parseEdgeExpand(
              op.edge(), oper, plan.plan(i + 1).opr().vertex(),
              move(intermediate_result), parse_info));
          ++i;
        } else {
          intermediate_result = move(EdgeExpandParser::parseEdgeExpand(
              op.edge(), oper, move(intermediate_result), parse_info));
        }
        parse_info.setCard(oper.card());
      } else if (op.has_project()) {
        intermediate_result = move(ProjectParser::parseProject(
            op.project(), move(intermediate_result), parse_info));
      } else if (op.has_intersect()) {
        intermediate_result = move(IntersectParser::parseIntersect(
            op.intersect(), move(intermediate_result), parse_info));
      } else if (op.has_group_by()) {
        intermediate_result = move(AggregateParser::parseAggregate(
            op.group_by(), move(intermediate_result), parse_info));
      } else if (op.has_select()) {
        intermediate_result = move(SelectParser::parseSelect(
            op.select(), move(intermediate_result), parse_info));
      }
    }
  }

  return move(intermediate_result);
}

} // namespace relgo