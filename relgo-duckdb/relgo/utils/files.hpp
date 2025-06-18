#ifndef FILES_H
#define FILES_H

#include <iostream>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace relgo {

inline bool check_path_exits(const std::string &path) {
  // split path by ':'
  std::vector<std::string> paths;
  std::string::size_type start = 0;
  std::string::size_type end = path.find(':');
  while (end != std::string::npos) {
    auto sub_path = path.substr(start, end - start);
    paths.push_back(sub_path);
    start = end + 1;
    end = path.find(':', start);
  }
  auto sub_path = path.substr(start);
  paths.push_back(sub_path);

  for (const auto &p : paths) {
    struct stat buffer;
    if (stat(p.c_str(), &buffer) != 0) {
      std::cerr << "Path not exists: " << p << std::endl;
      return false;
    }
  }
  std::cout << "Path exists: " << path << std::endl;
  return true;
}

} // namespace relgo

#endif