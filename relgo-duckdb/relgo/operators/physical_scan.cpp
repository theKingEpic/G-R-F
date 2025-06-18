#include "physical_scan.hpp"

namespace relgo {

std::string PhysicalScan::toString() {
  std::ostringstream oss;
  oss << "[Scan Operator]"
      << "\n";
  oss << "Table ID: " << table_id << "\n"
      << "Table Alias: " << table_alias << "\n"
      << "Table Name: " << table_name << "\n"
      << "Column IDs: [";
  for (const int &id : column_ids) {
    oss << id << " ";
  }
  oss << "]\nColumn Names: [";
  for (const std::string &name : column_names) {
    oss << name << " ";
  }
  oss << "]\nAll Columns: " << (is_all_columns ? "true" : "false") << "\n";
  if (filter_conditions) {
    oss << "Filter Conditions:\n";
    oss << filter_conditions->toString() << "\n";
  }

  return oss.str();
}

} // namespace relgo