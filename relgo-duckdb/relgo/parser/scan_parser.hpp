#ifndef SCAN_PARSER_H
#define SCAN_PARSER_H

#include "../operators/physical_scan.hpp"
#include "parse_info.hpp"

namespace relgo {

class ScanParser {
public:
  ScanParser() {}
  static std::unique_ptr<PhysicalScan>
  resolveScanParams(const algebra::QueryParams &params,
                    std::unique_ptr<PhysicalScan> scan_op, int table_alias,
                    ParseInfo &parse_info);
  static std::unique_ptr<PhysicalScan>
  initializeScanByDefault(std::unique_ptr<PhysicalScan> scan_op,
                          std::string table_id, std::string table_name,
                          int table_alias, ParseInfo &parse_info);
  static std::unique_ptr<PhysicalScan> parseScan(const physical::Scan &scan,
                                                 ParseInfo &parse_info);
};

} // namespace relgo

#endif