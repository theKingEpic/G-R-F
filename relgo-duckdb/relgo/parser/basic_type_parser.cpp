#include "basic_type_parser.hpp"

namespace relgo {

std::unique_ptr<std::vector<MetaDataInfo>> BasicTypeParser::parseDataType(const common::DataType &item,
                                                                          ParseInfo &parse_info) {
	std::unique_ptr<std::vector<MetaDataInfo>> result =
	    std::unique_ptr<std::vector<MetaDataInfo>>(new std::vector<MetaDataInfo>());

	switch (item.item_case()) {
	case common::DataType::ItemCase::kPrimitiveType:;
		result->push_back(MetaDataInfo(pbPrimitiveTypeToPhysicalType(item.primitive_type())));
		break;
	case common::DataType::ItemCase::kString:
		result->push_back(MetaDataInfo(PhysicalType::VARCHAR));
		break;
	default:
		break;
	}

	return move(result);
}

} // namespace relgo