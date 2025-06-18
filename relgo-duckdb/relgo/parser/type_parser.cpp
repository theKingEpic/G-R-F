#include "type_parser.hpp"

namespace relgo {

std::unique_ptr<std::vector<MetaDataInfo>> TypeParser::parseGraphDataType(const common::GraphDataType &graph_type,
                                                                          ParseInfo &parse_info) {
	std::unique_ptr<std::vector<MetaDataInfo>> result =
	    std::unique_ptr<std::vector<MetaDataInfo>>(new std::vector<MetaDataInfo>());

	for (const common::GraphDataType_GraphElementType &graph_data_type : graph_type.graph_data_type()) {
		int table_id = graph_data_type.label().label();
		std::string table_id_str = std::to_string(table_id);

		for (const common::GraphDataType_GraphElementTypeField &prop : graph_data_type.props()) {
			Value prop_value = CommonParser::parseNameOrId(prop.prop_id());
			PhysicalType prop_type = BasicTypeParser::parseDataType(prop.type(), parse_info)->at(0).data_type;
			if (prop_value.type() == PhysicalType::VARCHAR) {
				MetaDataInfo meta(table_id_str, prop_type, prop_value.ToString());
				result->push_back(meta);
			} else {
				std::cout << "Unable to handle integer type value now" << std::endl;
			}
		}
	}

	return move(result);
}

} // namespace relgo