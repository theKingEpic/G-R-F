#include "graph_schema.hpp"
#include "../third_party/nlohmann_json/json.hpp"

namespace relgo {

void GraphSchema::print() {
  for (const auto &table_name : this->table_names) {
    std::cout << table_name << " ";
  }
  std::cout << std::endl;

  for (auto &table_pair : this->table_properties) {
    std::cout << table_pair.first << std::endl;
    table_pair.second->print();
  }
}

YAML::Node createPropertyNode(int id, const std::string &name,
                              relgo::PhysicalType type) {
  YAML::Node property_node = YAML::Node{};
  property_node["property_id"] = id;
  property_node["property_name"] = name;

  if (type == relgo::PhysicalType::VARCHAR) {
    YAML::Node long_text_node = YAML::Node{};
    long_text_node["long_text"] = "";

    YAML::Node property_type_node = YAML::Node{};
    property_type_node["string"] = long_text_node;

    property_node["property_type"] = property_type_node;
  } else {
    YAML::Node property_type_node = YAML::Node{};
    property_type_node["primitive_type"] =
        common::PrimitiveType_Name(relgo::physicalTypeToPbPrimitiveType(type));

    property_node["property_type"] = property_type_node;
  }

  return property_node;
}

YAML::Node GraphSchema::generateGraphYAML() {
  YAML::Node root;

  root["name"] = "modern_graph";
  root["version"] = "v0.1";
  root["store_type"] = "mutable_csr";
  root["description"] = "A graph with 2 vertex types and 2 edge types";

  YAML::Node schema;
  YAML::Node vertex_types = YAML::Node(YAML::NodeType::Sequence);

  // Vertex Type: person
  for (const auto &table_name : table_names) {
    std::string table_id = table_name2id[table_name];

    if (table_element_type.find(table_id) == table_element_type.end() ||
        table_element_type[table_id] == "edge")
      continue;

    YAML::Node vertex_table;
    vertex_table["type_id"] = table_id;
    vertex_table["type_name"] = table_name;

    YAML::Node vertex_properties = YAML::Node(YAML::NodeType::Sequence);
    for (size_t j = 0; j < table_properties[table_id]->properties.size(); ++j) {
      auto &property = table_properties[table_id]->properties[j];

      vertex_properties.push_back(
          createPropertyNode(j, property.name, property.type));
    }
    vertex_table["properties"] = vertex_properties;
    vertex_table["primary_keys"].push_back(
        table_properties[table_id]->properties[0].name);

    vertex_types.push_back(vertex_table);
  }

  schema["vertex_types"] = vertex_types;

  YAML::Node edge_types = YAML::Node(YAML::NodeType::Sequence);

  // Vertex Type: person
  for (const auto &table_name : table_names) {
    std::string table_id = table_name2id[table_name];

    if (table_element_type.find(table_id) == table_element_type.end() ||
        table_element_type[table_id] == "vertex")
      continue;

    YAML::Node edge_table;
    edge_table["type_id"] = table_id;
    edge_table["type_name"] = table_name;

    YAML::Node edge_relations = YAML::Node(YAML::NodeType::Sequence);

    YAML::Node edge_adj = YAML::Node{};
    edge_adj["source_vertex"] = edge_adjacency[table_id].first;
    edge_adj["destination_vertex"] = edge_adjacency[table_id].second;
    edge_adj["relation"] = "MANY_TO_MANY";

    edge_relations.push_back(edge_adj);
    edge_table["vertex_type_pair_relations"] = edge_relations;

    YAML::Node edge_properties = YAML::Node(YAML::NodeType::Sequence);
    for (size_t j = 0; j < table_properties[table_id]->properties.size(); ++j) {
      auto &property = table_properties[table_id]->properties[j];

      edge_properties.push_back(
          createPropertyNode(j, property.name, property.type));
    }
    edge_table["properties"] = edge_properties;

    edge_types.push_back(edge_table);
  }

  schema["edge_types"] = edge_types;

  root["schema"] = schema;

  std::ofstream fout("../../"
                     "resource/tmp/graph.yaml");
  fout << root;

  return root;
}

nlohmann::json createVertexStatistic(std::string type_id,
                                     const std::string &type_name, int count) {
  int table_id_int = atoi(type_id.c_str());

  nlohmann::json type_statistic = {
      {"type_id", table_id_int}, {"type_name", type_name}, {"count", count}};
  return type_statistic;
}

nlohmann::json GraphSchema::generateStatistics() {
  nlohmann::json graph_statistic;
  int64_t total_vertex = 0;
  int64_t total_edge = 0;

  nlohmann::json vertex_type_statistics = nlohmann::json::array();

  for (auto &table_name : table_names) {
    std::string table_id = table_name2id[table_name];
    if (table_element_type.find(table_id) == table_element_type.end() ||
        table_element_type[table_id] == "edge")
      continue;

    vertex_type_statistics.push_back(
        createVertexStatistic(table_id, table_name, table_row_count[table_id]));
    total_vertex += table_row_count[table_id];
  }
  graph_statistic["vertex_type_statistics"] = vertex_type_statistics;

  nlohmann::json edge_type_statistics = nlohmann::json::array();

  for (auto &table_name : table_names) {
    std::string table_id = table_name2id[table_name];
    if (table_element_type.find(table_id) == table_element_type.end() ||
        table_element_type[table_id] == "vertex")
      continue;
    std::string source_table_name = edge_adjacency[table_id].first;
    std::string target_table_name = edge_adjacency[table_id].second;

    nlohmann::json vertex_type_pair_statistics = nlohmann::json::array();
    vertex_type_pair_statistics.push_back(
        {{"source_vertex", source_table_name},
         {"destination_vertex", target_table_name},
         {"count", table_row_count[table_id]}});

    total_edge += table_row_count[table_id];

    nlohmann::json type_statistic = {
        {"type_id", table_id},
        {"type_name", table_name},
        {"vertex_type_pair_statistics", vertex_type_pair_statistics}};

    edge_type_statistics.push_back(type_statistic);
  }

  graph_statistic["edge_type_statistics"] = edge_type_statistics;
  graph_statistic["total_vertex_count"] = total_vertex;
  graph_statistic["total_edge_count"] = total_edge;

  std::ofstream file("../../resource/tmp/modern_statistics.json");
  file << graph_statistic.dump(4);
  file.close();

  // nlohmann::json read_from_file;
  // std::fstream file("../../resource/tmp/modern_statistics.json");
  // file >> read_from_file;
  // return read_from_file;

  return graph_statistic;
}

} // namespace relgo