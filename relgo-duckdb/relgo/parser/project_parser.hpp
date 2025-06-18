#ifndef PROJECT_PARSER_HPP
#define PROJECT_PARSER_HPP

#include "../operators/physical_project.hpp"
#include "parse_info.hpp"

namespace relgo {

class ProjectParser {
public:
  static std::unique_ptr<PhysicalProject>
  parseProject(const physical::Project &op,
               std::unique_ptr<PhysicalOperator> child_op,
               ParseInfo &parse_info);
};

} // namespace relgo

#endif