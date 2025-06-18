#ifndef FUNCTIONS
#define FUNCTIONS

#include <algorithm>
#include <iostream>
#include <string>

#include "../third_party/nlohmann_json/json.hpp"
#include "yaml-cpp/yaml.h"

namespace relgo {

inline bool inString(std::string &base_str, std::string query_str) {
  std::string base_str_lower = base_str;
  std::string query_str_lower = query_str;

  std::transform(base_str_lower.begin(), base_str_lower.end(),
                 base_str_lower.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  std::transform(query_str_lower.begin(), query_str_lower.end(),
                 query_str_lower.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (base_str_lower.find(query_str_lower) != std::string::npos) {
    return true;
  } else {
    return false;
  }
}

inline bool startWith(const std::string& base_str, std::string query_str) {
  // Convert both query_string and prefix to lowercase
  std::string lower_query_string = base_str;
  std::string lower_prefix = query_str;

  std::transform(lower_query_string.begin(), lower_query_string.end(), lower_query_string.begin(), ::tolower);
  std::transform(lower_prefix.begin(), lower_prefix.end(), lower_prefix.begin(), ::tolower);

  // Check if the lowercase query_string starts with lowercase prefix
  if (lower_query_string.rfind(lower_prefix, 0) == 0) {
      return 1;
  } else {
      return 0;
  }
}

inline std::string yaml2str(YAML::Node& node) {
  std::stringstream ss;
  ss << node;

  std::string yaml_str = ss.str();
  return yaml_str;
}

inline std::string json2str(nlohmann::json& json_obj) {
  return json_obj.dump();
}

} // namespace relgo

#endif