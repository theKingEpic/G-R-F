#include "project_parser.hpp"

#include "../operators/physical_project.hpp"
#include "algebra_parser.hpp"

namespace relgo {

std::unique_ptr<PhysicalProject>
ProjectParser::parseProject(const physical::Project &op,
                            std::unique_ptr<PhysicalOperator> child_op,
                            ParseInfo &parse_info) {

  std::unique_ptr<PhysicalProject> project_op =
      std::unique_ptr<PhysicalProject>(new PhysicalProject());

  for (int i = 0; i < op.mappings_size(); ++i) {
    const physical::Project_ExprAlias mapping = op.mappings(i);

    std::vector<std::unique_ptr<ExprOpr>> opr_list;
    for (int i = 0; i < mapping.expr().operators_size(); ++i) {
      std::unique_ptr<ExprOpr> opr =
          ExprParser::parseExprOpr(mapping.expr().operators(i), parse_info);
      opr_list.push_back(move(opr));
    }

    std::shared_ptr<ConditionNode> result =
        AlgebraParser::parseExprOprList(opr_list);
    project_op->projection.push_back(result);
  }

  std::vector<std::string> col_names;
  std::vector<int> col_alias;
  for (int i = 0; i < project_op->projection.size(); ++i) {
    if (project_op->projection[i]->node_type ==
        ConditionNodeType::CONST_VARIABLE_NODE) {
      ConditionConstNode *const_node =
          dynamic_cast<ConditionConstNode *>(project_op->projection[i].get());
      col_names.push_back(const_node->field);
      col_alias.push_back(const_node->table_alias);
    } else if (project_op->projection[i]->node_type ==
               ConditionNodeType::CONST_VALUE_NODE) {
      ConditionConstNode *const_node =
          dynamic_cast<ConditionConstNode *>(project_op->projection[i].get());
      std::cout << "Const value projection is not supported now" << std::endl;
    }
  }

  parse_info.project(col_names, col_alias);

  project_op->child = move(child_op);
  return move(project_op);
}

} // namespace relgo