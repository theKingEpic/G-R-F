#ifndef META_DATA_INFO_H
#define META_DATA_INFO_H

#include "../utils/types.hpp"
#include <iostream>
#include <sstream>
#include <string>

namespace relgo {

enum class MetaDataInfoType { DATA_TYPE = 0, GRAPH_DATA_TYPE = 1 };

class MetaDataInfo {
public:
  MetaDataInfo(std::string _label, PhysicalType _data_type,
               std::string _data_name)
      : meta_type(MetaDataInfoType::GRAPH_DATA_TYPE), label(_label),
        data_type(_data_type), data_name(_data_name) {}

  MetaDataInfo(PhysicalType _data_type)
      : meta_type(MetaDataInfoType::DATA_TYPE), data_type(_data_type) {}

  std::string toString() {
    std::ostringstream oss;
    if (meta_type == MetaDataInfoType::DATA_TYPE) {
      oss << "Type: " << physicalTypeToString(data_type) << "\n";
    } else {
      oss << "Label: " << label << "\n"
          << "Type: " << physicalTypeToString(data_type) << "\n"
          << "Name: " << data_name << "\n";
    }

    return oss.str();
  }

  MetaDataInfoType meta_type;
  std::string label;
  PhysicalType data_type;
  std::string data_name;
};

} // namespace relgo

#endif