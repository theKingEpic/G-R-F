#include "cypher_analyzer.hpp"
#include <algorithm>

namespace duckdb {
std::vector<std::string> CypherAnalyzer::extractReturn(std::string query) {
  std::regex columnRegex(R"(RETURN\s*(([^\(\)]|\(.*\))*))", std::regex::icase);
  std::regex aliasRegex(R"((?:^|\s)AS\s+([^\s,]+))",
                        std::regex_constants::icase);

  std::smatch match;
  std::string column_clause = "";
  if (std::regex_search(query, match, columnRegex)) {
    column_clause = match[1];
  }

  std::string returns = "";
  std::vector<std::string> return_names;
  std::string current = "";

  for (size_t i = 0; i < column_clause.size(); ++i) {
    // if (columnsClause[i] == '(' || columnsClause[i] == ')')
    // 	continue;
    // else
    if (column_clause[i] != ',') {
      current += column_clause[i];
    } else {
      returns += current + ",";
      std::smatch alias_match;
      std::string alias = "";

      if (std::regex_search(current, alias_match, aliasRegex)) {
        if (alias_match.size() > 1) {
          alias = alias_match[1].str();
          return_names.push_back(alias);
        }
      } else {
        std::replace_if(
            current.begin(), current.end(),
            [](char c) {
              return !std::isalnum(
                  static_cast<unsigned char>(c)); // 检查字符是否是字母或数字
            },
            '_');
        return_names.push_back(current);
      }

      current = "";
    }
  }

  if (!current.empty()) {
    returns += current;
    std::smatch alias_match;
    std::string alias = "";

    if (std::regex_search(current, alias_match, aliasRegex)) {
      if (alias_match.size() > 1) {
        alias = alias_match[1].str();
        return_names.push_back(alias);
      }
    } else {
      std::replace_if(
          current.begin(), current.end(),
          [](char c) {
            return !std::isalnum(
                static_cast<unsigned char>(c)); // 检查字符是否是字母或数字
          },
          '_');
      return_names.push_back(current);
    }
  }

  return return_names;
}
} // namespace duckdb