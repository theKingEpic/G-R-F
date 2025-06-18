#ifndef GRAPH_SCHEMA_H
#define GRAPH_SCHEMA_H

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../utils/types.hpp"
#include "../utils/value.hpp"

#include <yaml-cpp/yaml.h>

namespace relgo {

enum class PropertyType { COLUMN_PROPERTY, CONST_VALUE_PROPERTY };

class Property {
public:
  Property() {}
  Property(int _id, std::string &_name, PhysicalType &_type,
           std::string _table_id, int _table_alias = -1)
      : id(_id), name(_name), type(_type), table_id(_table_id),
        table_alias(_table_alias), prop_type(PropertyType::COLUMN_PROPERTY) {}
  Property(Value &val)
      : id(-1), value(val), prop_type(PropertyType::CONST_VALUE_PROPERTY) {}

  int id; // column index
  std::string name;
  PhysicalType type;
  std::string table_id;
  int table_alias;
  Value value;
  PropertyType prop_type;

  friend std::ostream &operator<<(std::ostream &os, const Property &prop) {
    os << "{name: " << prop.name
       << ", type: " << physicalTypeToString(prop.type)
       << ", id: " << std::to_string(prop.id) << "}";
    return os;
  }

  bool isConst() { return prop_type == PropertyType::CONST_VALUE_PROPERTY; }
};

enum class SchemaElementType { SCHEMA_VERTEX_ELEMENT, SCHEMA_EDGE_ELEMENT };

class SchemaElement {
public:
  SchemaElement(){};
  SchemaElement(std::string _name, std::string _id,
                std::vector<Property> &_properties,
                std::unordered_map<std::string, std::string> &_info,
                SchemaElementType _element_type)
      : name(_name), id(_id), properties(_properties), info(_info),
        element_type(_element_type) {}
  SchemaElement(const SchemaElement &other)
      : name(other.name), id(other.id), properties(other.properties),
        info(other.info), element_type(other.element_type) {}

  void print(){};
  virtual ~SchemaElement() = default;

  SchemaElement &operator=(const SchemaElement &other) {
    if (this != &other) { // 避免自赋值
      name = other.name;
      id = other.id;
      properties = other.properties;
      info = other.info;
      element_type = other.element_type;
    }
    return *this;
  }

  std::string name;
  std::string id;
  std::vector<Property> properties;
  std::unordered_map<std::string, std::string> info;
  SchemaElementType element_type;

  int getPropertyColIndex(std::string prop_name,
                          std::string from_table = "null", int alias = -1) {
    for (size_t i = 0; i < properties.size(); ++i) {
      if (properties[i].name == prop_name &&
          (from_table == "null" || from_table == properties[i].table_id) &&
          (alias == -1 || alias == properties[i].table_alias))
        return i;
    }
    return -1;
  }

  void set_alias(int alias) {
    for (size_t i = 0; i < properties.size(); ++i) {
      properties[i].table_alias = alias;
    }
  }

  void merge(SchemaElement &other) {
    properties.insert(properties.end(), other.properties.begin(),
                      other.properties.end());
    name = name + "_join_" + other.name;
  }

  void merge_front(SchemaElement &other) {
    properties.insert(properties.begin(), other.properties.begin(),
                      other.properties.end());
    name = other.name + "_join_" + name;
  }

  void project(std::vector<int> &cols) {
    if (cols.empty()) {
      properties.clear();
      return;
    }

    int off = 0;
    for (size_t i = 0; i < properties.size() && off < cols.size(); ++i) {
      if (cols[off] == i) {
        properties[off++] = properties[i];
      }
    }

    properties.resize(off);
    name = name + "_project_col";
  }

  void project(std::vector<std::string> &col_names) {
    if (col_names.empty()) {
      properties.clear();
      return;
    }

    int off = 0;
    for (size_t i = 0; i < col_names.size(); ++i) {
      for (size_t j = 0; j < properties.size(); ++j) {
        if (properties[j].isConst())
          continue;
        if (properties[j].name == col_names[i]) {
          properties[off++] = properties[j];
          break;
        }
      }
    }

    properties.resize(off);
    name = name + "_project_colname";
  }

  void project(std::vector<std::string> &col_names,
               std::vector<int> &col_alias) {
    if (col_names.empty()) {
      properties.clear();
      return;
    }

    if (col_names.size() != col_alias.size()) {
      std::cout << "The size of col_names and col_alias does not match"
                << std::endl;
      return;
    }

    int off = 0;
    for (size_t i = 0; i < col_names.size(); ++i) {
      for (size_t j = 0; j < properties.size(); ++j) {
        if (properties[j].isConst())
          continue;
        if (properties[j].name == col_names[i] &&
            properties[j].table_alias == col_alias[i]) {
          properties[off++] = properties[j];
          break;
        }
      }
    }

    properties.resize(off);
    name = name + "_project_colname";
  }
};

class SchemaVertex : public SchemaElement {
public:
  SchemaVertex(){};
  SchemaVertex(std::string _name, std::string _id,
               std::vector<Property> &_properties,
               std::unordered_map<std::string, std::string> _info =
                   std::unordered_map<std::string, std::string>())
      : SchemaElement(_name, _id, _properties, _info,
                      SchemaElementType::SCHEMA_VERTEX_ELEMENT) {}

  void print() {
    std::cout << "SchemaVertex {"
              << "name: " << name << ", "
              << "id: " << id << ", "
              << "properties: [";
    for (const auto &prop : properties) {
      std::cout << prop << ", ";
    }

    std::cout << "], "
              << "info: {";
    for (const auto &pair : info) {
      std::cout << pair.first << ": " << pair.second << ", ";
    }

    std::cout << "}}" << std::endl;
  }
};

class SchemaEdge : public SchemaElement {
public:
  SchemaEdge() {}
  SchemaEdge(std::string _name, std::string _id,
             std::vector<Property> &_properties, std::string _source_id,
             std::string _target_id, std::string _source_table,
             std::string _target_table,
             std::unordered_map<std::string, std::string> _info =
                 std::unordered_map<std::string, std::string>())
      : SchemaElement(_name, _id, _properties, _info,
                      SchemaElementType::SCHEMA_EDGE_ELEMENT),
        source_id(_source_id), target_id(_target_id),
        source_table(_source_table), target_table(_target_table) {}

  std::string source_id, target_id;
  std::string source_table, target_table;

  void print() {
    std::cout << "SchemaEdge {"
              << "name: " << name << ", "
              << "id: " << id << ", "
              << "properties: [";
    for (const auto &prop : properties) {
      std::cout << prop << ", ";
    }

    std::cout << "], "
              << "source_id: " << source_id << ", "
              << "target_id: " << target_id << ", "
              << "source_table: " << source_table << ", "
              << "target_table: " << target_table << ", "
              << "info: {";
    for (const auto &pair : info) {
      std::cout << pair.first << ": " << pair.second << ", ";
    }

    std::cout << "}}" << std::endl;
  }
};

class IdManager {
public:
  IdManager() : current_index(0) {}
  int current_index;
  std::unordered_map<std::string, std::string> idmapping;
  std::unordered_map<std::string, std::string> idmapping_rev;

  std::string mapid(std::string &oid) {
    if (idmapping.find(oid) == idmapping.end()) {
      idmapping[oid] = std::to_string(current_index);
      idmapping_rev[std::to_string(current_index++)] = oid;
    }
    return idmapping[oid];
  }

  std::string getid(std::string &nid) {
    if (idmapping_rev.find(nid) == idmapping_rev.end())
      std::cerr << nid << " not found" << std::endl;
    return idmapping_rev[nid];
  }

  std::string createid() {
    std::string newid = std::to_string(current_index);

    while (idmapping_rev.find(newid) != idmapping_rev.end()) {
      current_index++;
      newid = std::to_string(current_index);
    }

    current_index++;
    return newid;
  }
};

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    auto hash1 = std::hash<T1>{}(pair.first);
    auto hash2 = std::hash<T2>{}(pair.second);
    return hash1 ^ hash2;
  }
};

struct pair_equal {
  bool operator()(const std::pair<std::string, std::string> &lhs,
                  const std::pair<std::string, std::string> &rhs) const {
    return lhs.first == rhs.first && lhs.second == rhs.second;
  }
};

class GraphSchema {
public:
  GraphSchema() {}
  void print();

  void addTable(std::string &table_name, std::string &table_oid,
                int row_count) {
    std::string table_id = id_manager.mapid(table_oid);

    table_names.insert(table_name);
    table_name2id[table_name] = table_id;
    table_id2name[table_id] = table_name;
    table_row_count[table_id] = row_count;
  }

  YAML::Node generateGraphYAML();
  nlohmann::json generateStatistics();

  std::unordered_set<std::string> table_names;
  std::unordered_map<std::string, std::string> table_name2id;
  std::unordered_map<std::string, std::string> table_id2name;
  std::unordered_map<std::string, std::shared_ptr<SchemaElement>>
      table_properties;
  std::unordered_map<std::string, int> table_row_count;

  std::unordered_map<std::string, std::string> table_element_type;
  std::unordered_map<std::string, std::pair<std::string, std::string>>
      edge_adjacency;

  IdManager id_manager;

  std::string output_folder;
};

} // namespace relgo

#endif