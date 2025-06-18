//
// Created by Louyk on 2025/2/11.
//

#ifndef DUCKDB_INDEX_INFO_HPP
#define DUCKDB_INDEX_INFO_HPP

#include "alist.hpp"

#define ENABLE_ALISTS true

#include <mutex>

namespace relgo {

enum class GraphIndexType : uint8_t {
  INVALID = 0,
  TARGET_EDGE = 2,
  SOURCE_EDGE = 3,
  EDGE_TARGET = 5,
  EDGE_SOURCE = 6,
  SELF = 7
};

//! Relational Adjacency Index
template <typename T, typename S> class GraphIndex {
public:
  GraphIndex(std::string name, std::string table_name,
             GraphIndexDirection rai_direction,
             std::vector<column_t> column_ids,
             std::vector<std::string> referenced_tables,
             std::vector<column_t> referenced_columns)
      : name(name), table_name(table_name), rai_direction(rai_direction),
        column_ids(column_ids), referenced_tables(referenced_tables),
        referenced_columns(referenced_columns) {
    // RAI::alist = make_unique<AList>(name + "_alist");
    alist = std::unique_ptr<AList<T, S>>(new AList<T, S>(name + "_alist"));
  }
  virtual ~GraphIndex() = default;

  std::mutex lock;
  std::string name;
  std::string table_name;
  GraphIndexDirection rai_direction;
  std::vector<column_t> column_ids;
  std::vector<std::string> referenced_tables;
  std::vector<column_t> referenced_columns;

  std::unique_ptr<AList<T, S>> alist;
};

enum AListDirection : uint8_t { FORWARD, BACKWARD, INVALID_DIR };

struct PushDownStatistics {
  std::shared_ptr<bitmask_vector> row_bitmask;
  std::shared_ptr<bitmask_vector> zone_bitmask;
  std::shared_ptr<bitmask_vector> extra_row_bitmask;
  std::shared_ptr<bitmask_vector> extra_zone_bitmask;
};

template <typename T, typename S> struct GraphIndexInfo {
  explicit GraphIndexInfo<T, S>()
      : rai(nullptr), rai_type(GraphIndexType::INVALID), forward(true),
        vertex_name("") {}

  GraphIndex<T, S> *rai;
  GraphIndexType rai_type;
  bool forward;
  std::string vertex_name; // vertex table table_pointer
  idx_t vertex_id;         // vertex table table_index
  std::vector<idx_t> passing_tables = {0, 0};
  std::vector<idx_t> left_cardinalities = {0, 0};
  CompactList *compact_list = nullptr; // csr alist storage used during join

  double GetAverageDegree(GraphIndexType rai_type_, bool forward_) const {
    switch (rai_type_) {
    case GraphIndexType::SELF: {
      return forward_ ? rai->alist->src_avg_degree : rai->alist->dst_avg_degree;
    }
    case GraphIndexType::EDGE_SOURCE: {
      return rai->alist->src_avg_degree;
    }
    case GraphIndexType::EDGE_TARGET: {
      return rai->alist->dst_avg_degree;
    }
    default:
      return 1.0;
    }
  }

  std::string RAITypeToString() {
    switch (rai_type) {
    case GraphIndexType::INVALID:
      return "INVALID";
    case GraphIndexType::TARGET_EDGE:
      return "TARGET_EDGE";
    case GraphIndexType::SOURCE_EDGE:
      return "SOURCE_EDGE";
    case GraphIndexType::EDGE_TARGET:
      return "EDGE_TARGET";
    case GraphIndexType::EDGE_SOURCE:
      return "EDGE_SOURCE";
    case GraphIndexType::SELF:
      return "SELF";
    default:
      throw "Invalid RAIType";
    }
  }

  GraphIndexType StringToRAIType(std::string type) {
    if (type == "INVALID")
      return GraphIndexType::INVALID;
    else if (type == "TARGET_EDGE")
      return GraphIndexType::TARGET_EDGE;
    else if (type == "SOURCE_EDGE")
      return GraphIndexType::SOURCE_EDGE;
    else if (type == "EDGE_TARGET")
      return GraphIndexType::EDGE_TARGET;
    else if (type == "EDGE_SOURCE")
      return GraphIndexType::EDGE_SOURCE;
    else if (type == "SELF")
      return GraphIndexType::SELF;
    else
      throw "Invalid RAIType " + type;
  }
};

} // namespace relgo

#endif // DUCKDB_INDEX_INFO_HPP
