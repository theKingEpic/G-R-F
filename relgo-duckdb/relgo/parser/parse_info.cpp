#include "parse_info.hpp"

namespace relgo {

void ParseInfo::setCurrentTableSchema(std::string table_id, int table_alias) {
  if (gs.table_properties.find(table_id) != gs.table_properties.end()) {
    this->current_table_schema = *(gs.table_properties[table_id].get());
    current_table_schema.set_alias(table_alias);
  } else {
    std::cout << "Table with id " << table_id << " not found.";
  }

  this->current_table_id = table_id;

  if (index_manager.necessary_props.find(table_alias) !=
      index_manager.necessary_props.end()) {
    int off = 0;
    for (size_t prop_idx = 0; prop_idx < current_table_schema.properties.size();
         ++prop_idx) {
      std::string var_name = current_table_schema.properties[prop_idx].name;
      if (index_manager.necessary_props[table_alias].find(var_name) !=
          index_manager.necessary_props[table_alias].end()) {
        current_table_schema.properties[off++] =
            current_table_schema.properties[prop_idx];
      }
    }

    current_table_schema.properties.resize(off);
  } else {
    current_table_schema.properties.clear();
  }
}

void ParseInfo::join(ParseInfo &other) {
  checkpoint();
  current_table_schema.merge(other.current_table_schema);
}

void ParseInfo::join_front(ParseInfo &other) {
  checkpoint();
  current_table_schema.merge_front(other.current_table_schema);
}

void ParseInfo::project(std::vector<int> &cols) {
  checkpoint();
  current_table_schema.project(cols);
}

void ParseInfo::project(std::vector<std::string> &col_names) {
  checkpoint();
  current_table_schema.project(col_names);
}

void ParseInfo::project(std::vector<std::string> &col_names,
                        std::vector<int> &col_alias) {
  checkpoint();
  current_table_schema.project(col_names, col_alias);
}

void ParseInfo::checkpoint() {
  if (checkpoints.find(current_table_id) != checkpoints.end()) {
    std::cout << "the check point of table " << current_table_id
              << " is replaced" << std::endl;
  }
  checkpoints[current_table_id] = current_table_schema;
  current_table_id = inner_index_prefix + std::to_string(parse_info_index);
  parse_info_index++;
}

bool ParseInfo::isInternalTable(std::string &table_id) {
  if (inner_index_prefix.size() > table_id.size()) {
    return false;
  }
  return table_id.substr(0, inner_index_prefix.size()) == inner_index_prefix;
}

std::string ParseInfo::toString() {
  std::ostringstream oss;
  for (const auto &cp : checkpoints) {
    oss << cp.second.name << ":\n";
    for (int i = 0; i < cp.second.properties.size(); ++i) {
      oss << cp.second.properties[i] << "\n";
    }
    oss << "\n";
  }

  return oss.str();
}

void ParseInfo::appendIndex(int alias) {
  std::string table_id = current_table_schema.id;
  for (IndexColumn &index_column :
       index_manager.used_index_columns[table_id][alias]) {
    if (current_table_schema.getPropertyColIndex(index_column.column_name,
                                                 "null", alias) == -1) {
      current_table_schema.properties.push_back(
          Property(index_column.column_index, index_column.column_name,
                   index_column.column_type, table_id, alias));
    }
  }
}

void ParseInfo::setCard(idx_t _card) { current_card = _card; }

idx_t ParseInfo::getCard() { return current_card; }

} // namespace relgo