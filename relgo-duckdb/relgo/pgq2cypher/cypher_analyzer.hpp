#pragma once

#include <iostream>
#include <regex>
#include <string>
#include <vector>

namespace duckdb {
class CypherAnalyzer {
public:
  static std::vector<std::string> extractReturn(std::string query);
};
} // namespace duckdb