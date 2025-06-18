//
// Created by Louyk on 2025/2/11.
//

#ifndef ALIST_HPP
#define ALIST_HPP

#include <algorithm>
#include <assert.h>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

#include "../utils/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace relgo {

static int64_t *input_transform(duckdb::DataChunk &data_chunk) {
  duckdb::UnifiedVectorFormat right_data;
  idx_t rsize = data_chunk.size();
  data_chunk.data[0].ToUnifiedFormat(rsize, right_data);
  auto rdata = (int64_t *)right_data.data;

  return rdata;
}

static std::vector<int64_t *> inputs_transform(duckdb::DataChunk &data_chunk,
                                               int input_num) {
  std::vector<int64_t *> rid_data;
  for (int i = 0; i < input_num; ++i) {
    duckdb::UnifiedVectorFormat right_data;
    idx_t rsize = data_chunk.size();
    data_chunk.data[i].ToUnifiedFormat(rsize, right_data);
    auto rdata = (int64_t *)right_data.data;
    rid_data.push_back(rdata);
  }

  return rid_data;
}

static idx_t input_getKthRID(duckdb::DataChunk &data_chunk, int64_t *&data,
                             idx_t k, duckdb::SelectionVector &sel) {
  idx_t right_position = sel.get_index(k);
  idx_t rid = data[right_position];
  return rid;
}

static int64_t *output_transform(duckdb::DataChunk &data_chunk, idx_t col) {
  return (int64_t *)duckdb::FlatVector::GetData(data_chunk.data[col]);
}

static void output_copy(duckdb::DataChunk &data_chunk, int64_t *&target,
                        int64_t *alist_s, idx_t offset, idx_t size) {
  memcpy(target + offset, alist_s, size * sizeof(int64_t));
}

static void output_set_sel(duckdb::DataChunk &data_chunk,
                           duckdb::SelectionVector &sel, idx_t index,
                           idx_t val) {
  sel.set_index(index, val);
}

enum class GraphIndexDirection : uint8_t { DIRECTED, UNDIRECTED, SELF, PKFK };

// List for each source vertex (with edges and destination vertices)
struct EList {
  EList() : size(0) {
    edges = std::unique_ptr<std::vector<int64_t>>(new std::vector<int64_t>());
    vertices =
        std::unique_ptr<std::vector<int64_t>>(new std::vector<int64_t>());
  }

  idx_t size;
  std::unique_ptr<std::vector<int64_t>> edges;
  std::unique_ptr<std::vector<int64_t>> vertices;
};

struct VList {
  VList() : size(0) {
    vertices =
        std::unique_ptr<std::vector<int64_t>>(new std::vector<int64_t>());
  }

  idx_t size;
  std::unique_ptr<std::vector<int64_t>> vertices;
};

// adjacency lists stored in CSR format
struct CompactList {
  std::unique_ptr<idx_t[]> offsets;
  std::unique_ptr<idx_t[]> sizes;
  std::unique_ptr<int64_t[]> edges;
  std::unique_ptr<int64_t[]> vertices;
};

class EdgeTuple {
public:
  EdgeTuple(idx_t _srid, idx_t _erid, idx_t _trid)
      : source_rid(_srid), edge_rid(_erid), target_rid(_trid) {}
  idx_t source_rid;
  idx_t edge_rid;
  idx_t target_rid;
};

template <typename T, typename S> struct GraphIndexInfo;

template <typename T, typename S> class AList {
public:
  AList(std::string alias)
      : alias(std::move(alias)), enabled(false), source_num(0), target_num(0),
        edge_num(0), src_avg_degree(0.0), dst_avg_degree(0.0) {}

  std::string alias;
  bool enabled; // if alist in stored in CSR format

  //! Append rai tuples (source, edge, target) into alist
  void Append(std::vector<EdgeTuple> &tuples, idx_t count,
              GraphIndexDirection direction) {
    for (idx_t i = 0; i < count; i++) {
      auto source_rid = tuples[i].source_rid;
      auto edge_rid = tuples[i].edge_rid;
      auto target_rid = tuples[i].target_rid;

      if (forward_map.find(source_rid) == forward_map.end()) {
        auto elist = std::unique_ptr<EList>(new EList());
        forward_map[source_rid] = move(elist);
      }
      forward_map[source_rid]->edges->push_back(edge_rid);
      forward_map[source_rid]->vertices->push_back(target_rid);
      forward_map[source_rid]->size++;

      if (forward_edge_map.find(edge_rid) == forward_edge_map.end()) {
        auto vlist = std::unique_ptr<VList>(new VList());
        forward_edge_map[edge_rid] = move(vlist);
      }
      forward_edge_map[edge_rid]->vertices->push_back(target_rid);
      forward_edge_map[edge_rid]->size++;

      if (backward_edge_map.find(edge_rid) == backward_edge_map.end()) {
        auto vlist = std::unique_ptr<VList>(new VList());
        backward_edge_map[edge_rid] = move(vlist);
      }
      backward_edge_map[edge_rid]->vertices->push_back(source_rid);
      backward_edge_map[edge_rid]->size++;
    }
    if (direction == GraphIndexDirection::SELF ||
        direction == GraphIndexDirection::UNDIRECTED) {
      for (idx_t i = 0; i < count; i++) {
        auto source_rid = tuples[i].source_rid;
        auto edge_rid = tuples[i].edge_rid;
        auto target_rid = tuples[i].target_rid;

        if (backward_map.find(target_rid) == backward_map.end()) {
          auto elist = std::unique_ptr<EList>(new EList());
          backward_map[target_rid] = move(elist);
        }
        backward_map[target_rid]->edges->push_back(edge_rid);
        backward_map[target_rid]->vertices->push_back(source_rid);
        backward_map[target_rid]->size++;
      }
    }
    edge_num += count;
  }
  void AppendPKFK(std::vector<EdgeTuple> &tuples, idx_t count,
                  GraphIndexDirection direction = GraphIndexDirection::PKFK) {
    for (idx_t i = 0; i < count; i++) {
      auto source_rid = tuples[i].source_rid;
      auto edge_rid = tuples[i].edge_rid;
      auto target_rid = tuples[i].target_rid;

      if (forward_map.find(source_rid) == forward_map.end()) {
        auto elist = std::unique_ptr<EList>(new EList());
        forward_map[source_rid] = move(elist);
      }
      forward_map[source_rid]->edges->push_back(edge_rid);
      forward_map[source_rid]->size++;

      if (forward_edge_map.find(edge_rid) == forward_edge_map.end()) {
        auto vlist = std::unique_ptr<VList>(new VList());
        forward_edge_map[edge_rid] = move(vlist);
      }
      forward_edge_map[edge_rid]->vertices->push_back(source_rid);
      forward_edge_map[edge_rid]->size++;
    }
    edge_num += count;
  }
  //! Finalize write to a compact immutable storage layout
  void Finalize(GraphIndexDirection direction) {
    if (direction == GraphIndexDirection::PKFK) {
      compact_forward_list.offsets =
          std::unique_ptr<idx_t[]>(new idx_t[source_num]);
      compact_forward_list.sizes =
          std::unique_ptr<idx_t[]>(new idx_t[source_num]);
      compact_forward_list.edges =
          std::unique_ptr<int64_t[]>(new int64_t[edge_num]);

      idx_t current_offset = 0;
      for (idx_t i = 0; i < source_num; i++) {
        if (forward_map.find(i) != forward_map.end()) {
          auto elist_size = forward_map[i]->size;
          std::memcpy((int64_t *)(compact_forward_list.edges.get()) +
                          current_offset,
                      (int64_t *)(&forward_map[i]->edges->operator[](0)),
                      elist_size * sizeof(int64_t));
          compact_forward_list.sizes[i] = elist_size;
          compact_forward_list.offsets[i] = current_offset;
          current_offset += elist_size;
        } else {
          compact_forward_list.sizes[i] = 0;
          compact_forward_list.offsets[i] = current_offset;
        }
      }
      forward_map.clear();

      compact_edge_forward_list.offsets =
          std::unique_ptr<idx_t[]>(new idx_t[edge_num]);
      compact_edge_forward_list.sizes =
          std::unique_ptr<idx_t[]>(new idx_t[edge_num]);
      compact_edge_forward_list.vertices =
          std::unique_ptr<int64_t[]>(new int64_t[edge_num]);

      current_offset = 0;
      for (idx_t i = 0; i < edge_num; i++) {
        if (forward_edge_map.find(i) != forward_edge_map.end()) {
          auto vlist_size = forward_edge_map[i]->size;
          std::memcpy(
              (int64_t *)(compact_edge_forward_list.vertices.get()) +
                  current_offset,
              (int64_t *)(&forward_edge_map[i]->vertices->operator[](0)),
              vlist_size * sizeof(int64_t));
          compact_edge_forward_list.sizes[i] = vlist_size;
          compact_edge_forward_list.offsets[i] = current_offset;
          current_offset += vlist_size;
        } else {
          compact_edge_forward_list.sizes[i] = 0;
          compact_edge_forward_list.offsets[i] = current_offset;
        }
      }
      forward_edge_map.clear();

    } else {
      compact_forward_list.offsets =
          std::unique_ptr<idx_t[]>(new idx_t[source_num]);
      compact_forward_list.sizes =
          std::unique_ptr<idx_t[]>(new idx_t[source_num]);
      compact_forward_list.edges =
          std::unique_ptr<int64_t[]>(new int64_t[edge_num]);
      compact_forward_list.vertices =
          std::unique_ptr<int64_t[]>(new int64_t[edge_num]);
      uint64_t current_offset = 0;
      for (idx_t i = 0; i < source_num; i++) {
        if (forward_map.find(i) != forward_map.end()) {
          auto elist_size = forward_map[i]->size;
          sort(forward_map[i]->vertices->begin(),
               forward_map[i]->vertices->begin() + elist_size);
          std::memcpy((int64_t *)(compact_forward_list.edges.get()) +
                          current_offset,
                      (int64_t *)(&forward_map[i]->edges->operator[](0)),
                      elist_size * sizeof(int64_t));
          std::memcpy((int64_t *)(compact_forward_list.vertices.get()) +
                          current_offset,
                      (int64_t *)(&forward_map[i]->vertices->operator[](0)),
                      elist_size * sizeof(int64_t));
          compact_forward_list.sizes[i] = elist_size;
          compact_forward_list.offsets[i] = current_offset;
          current_offset += elist_size;
        } else {
          compact_forward_list.sizes[i] = 0;
          compact_forward_list.offsets[i] = current_offset;
        }
      }
      if (backward_map.size() != 0) {
        compact_backward_list.offsets =
            std::unique_ptr<idx_t[]>(new idx_t[target_num]);
        compact_backward_list.sizes =
            std::unique_ptr<idx_t[]>(new idx_t[target_num]);
        compact_backward_list.edges =
            std::unique_ptr<int64_t[]>(new int64_t[edge_num]);
        compact_backward_list.vertices =
            std::unique_ptr<int64_t[]>(new int64_t[edge_num]);
        current_offset = 0;
        for (idx_t i = 0; i < target_num; i++) {
          if (backward_map.find(i) != backward_map.end()) {
            auto elist_size = backward_map[i]->size;
            sort(backward_map[i]->vertices->begin(),
                 backward_map[i]->vertices->begin() + elist_size);
            memcpy((int64_t *)(compact_backward_list.edges.get()) +
                       current_offset,
                   (int64_t *)(&backward_map[i]->edges->operator[](0)),
                   elist_size * sizeof(int64_t));
            memcpy((int64_t *)(compact_backward_list.vertices.get()) +
                       current_offset,
                   (int64_t *)(&backward_map[i]->vertices->operator[](0)),
                   elist_size * sizeof(int64_t));
            compact_backward_list.sizes[i] = elist_size;
            compact_backward_list.offsets[i] = current_offset;
            current_offset += elist_size;
          } else {
            compact_backward_list.sizes[i] = 0;
            compact_backward_list.offsets[i] = current_offset;
          }
        }
      }
      forward_map.clear();
      backward_map.clear();

      compact_edge_forward_list.offsets =
          std::unique_ptr<idx_t[]>(new idx_t[edge_num]);
      compact_edge_forward_list.sizes =
          std::unique_ptr<idx_t[]>(new idx_t[edge_num]);
      compact_edge_forward_list.vertices =
          std::unique_ptr<int64_t[]>(new int64_t[edge_num]);
      compact_edge_backward_list.offsets =
          std::unique_ptr<idx_t[]>(new idx_t[edge_num]);
      compact_edge_backward_list.sizes =
          std::unique_ptr<idx_t[]>(new idx_t[edge_num]);
      compact_edge_backward_list.vertices =
          std::unique_ptr<int64_t[]>(new int64_t[edge_num]);

      current_offset = 0;
      for (idx_t i = 0; i < edge_num; i++) {
        if (forward_edge_map.find(i) != forward_edge_map.end()) {
          auto vlist_size = forward_edge_map[i]->size;
          memcpy((int64_t *)(compact_edge_forward_list.vertices.get()) +
                     current_offset,
                 (int64_t *)(&forward_edge_map[i]->vertices->operator[](0)),
                 vlist_size * sizeof(int64_t));
          compact_edge_forward_list.sizes[i] = vlist_size;
          compact_edge_forward_list.offsets[i] = current_offset;
          current_offset += vlist_size;
        } else {
          compact_edge_forward_list.sizes[i] = 0;
          compact_edge_forward_list.offsets[i] = current_offset;
        }
      }
      if (backward_edge_map.size() != 0) {
        current_offset = 0;
        for (idx_t i = 0; i < edge_num; ++i) {
          if (backward_edge_map.find(i) != backward_edge_map.end()) {
            auto vlist_size = backward_edge_map[i]->size;
            memcpy((int64_t *)(compact_edge_backward_list.vertices.get()) +
                       current_offset,
                   (int64_t *)(&backward_edge_map[i]->vertices->operator[](0)),
                   vlist_size * sizeof(int64_t));
            compact_edge_backward_list.sizes[i] = vlist_size;
            compact_edge_backward_list.offsets[i] = current_offset;
            current_offset += vlist_size;
          } else {
            compact_edge_backward_list.sizes[i] = 0;
            compact_edge_backward_list.offsets[i] = current_offset;
          }
        }
      }
      forward_edge_map.clear();
      backward_edge_map.clear();
    }
    enabled = true;
    src_avg_degree = (double)edge_num / (double)source_num;
    dst_avg_degree = (double)edge_num / (double)target_num;
  }
  //! Fetch <edge, target> pairs from alist based on a given source id, and
  //! return selection vector for right chunk l refers to alist, r refers to rid
  idx_t Fetch(idx_t &lpos, idx_t &rpos, duckdb::DataChunk &start_vertex_list,
              idx_t rsize, S &input_sel, S &output_sel,
              duckdb::DataChunk &edge_list, duckdb::DataChunk &end_vertex_list,
              idx_t output_col, bool forward) {
    if (forward) {
      return FetchInternal(compact_forward_list, lpos, rpos, start_vertex_list,
                           rsize, input_sel, output_sel, edge_list,
                           end_vertex_list, output_col);
    }
    return FetchInternal(compact_backward_list, lpos, rpos, start_vertex_list,
                         rsize, input_sel, output_sel, edge_list,
                         end_vertex_list, output_col);
  }
  idx_t FetchVertexes(idx_t &lpos, idx_t &rpos,
                      duckdb::DataChunk &start_vertex_list, idx_t rsize,
                      S &input_sel, S &output_sel,
                      duckdb::DataChunk &end_vertex_list, idx_t output_col,
                      bool forward) {
    if (forward) {
      return FetchVertexesInternal(compact_forward_list, lpos, rpos,
                                   start_vertex_list, rsize, input_sel,
                                   output_sel, end_vertex_list, output_col);
    }
    return FetchVertexesInternal(compact_backward_list, lpos, rpos,
                                 start_vertex_list, rsize, input_sel,
                                 output_sel, end_vertex_list, output_col);
  }
  idx_t FetchVertexes(std::vector<idx_t> &lpos, idx_t &rpos,
                      duckdb::DataChunk &start_vertex_list, idx_t rsize,
                      std::vector<S> &input_sel, S &output_sel,
                      duckdb::DataChunk &end_vertex_list, idx_t output_col,
                      const std::vector<GraphIndexInfo<T, S> *> &merge_rais,
                      const std::vector<CompactList *> &compact_lists) {
    if (merge_rais[0]->forward) {
      return FetchVertexesInternalMerge(
          compact_forward_list, lpos, rpos, start_vertex_list, rsize, input_sel,
          output_sel, end_vertex_list, output_col, merge_rais, compact_lists);
    }
    return FetchVertexesInternalMerge(
        compact_backward_list, lpos, rpos, start_vertex_list, rsize, input_sel,
        output_sel, end_vertex_list, output_col, merge_rais, compact_lists);
  }

public:
  idx_t source_num;
  idx_t target_num;
  idx_t edge_num;
  double src_avg_degree;
  double dst_avg_degree;
  std::unordered_map<int64_t, std::unique_ptr<EList>> forward_map;
  std::unordered_map<int64_t, std::unique_ptr<EList>> backward_map;
  std::unordered_map<int64_t, std::unique_ptr<VList>> forward_edge_map;
  std::unordered_map<int64_t, std::unique_ptr<VList>> backward_edge_map;
  CompactList compact_forward_list;
  CompactList compact_backward_list;
  CompactList compact_edge_forward_list;
  CompactList compact_edge_backward_list;
};

template <typename S>
static idx_t
FetchInternal(CompactList &alist, idx_t &lpos, idx_t &rpos,
              duckdb::DataChunk &start_vertex_list, idx_t rsize, S &input_sel,
              S &output_sel, duckdb::DataChunk &edge_list,
              duckdb::DataChunk &end_vertex_list, idx_t output_col) {
  if (rpos >= rsize) {
    return 0;
  }

  idx_t result_count = 0;
  auto data = input_transform(start_vertex_list);
  auto r0data = output_transform(edge_list, output_col);
  auto r1data = output_transform(end_vertex_list, output_col);

  while (rpos < rsize) {
    auto rid = input_getKthRID(start_vertex_list, data, rpos, input_sel);
    auto offset = alist.offsets.operator[](rid);
    auto length = alist.sizes.operator[](rid);
    auto result_size =
        std::min(length - lpos, STANDARD_VECTOR_SIZE - result_count);
    output_copy(edge_list, r0data, alist.edges.get() + (offset + lpos),
                result_count, result_size);
    output_copy(end_vertex_list, r1data, alist.vertices.get() + (offset + lpos),
                result_count, result_size);

    for (idx_t i = 0; i < result_size; i++) {
      output_set_sel(edge_list, output_sel, result_count, rpos);
      result_count++;
    }
    lpos += result_size;
    if (lpos == length) {
      lpos = 0;
      rpos++;
    }
    if (result_count == STANDARD_VECTOR_SIZE) {
      return result_count;
    }
  }

  return result_count;
}

template <typename S>
static idx_t FetchVertexesInternal(CompactList &alist, idx_t &lpos, idx_t &rpos,
                                   duckdb::DataChunk &start_vertex_list,
                                   idx_t rsize, S &input_sel, S &output_sel,
                                   duckdb::DataChunk &end_vertex_list,
                                   idx_t output_col) {
  if (rpos >= rsize) {
    return 0;
  }

  idx_t result_count = 0;
  auto data = input_transform(start_vertex_list);
  auto r1data = output_transform(end_vertex_list, output_col);

  while (rpos < rsize) {
    auto rid = input_getKthRID(start_vertex_list, data, rpos, input_sel);
    auto offset = alist.offsets.operator[](rid);
    auto length = alist.sizes.operator[](rid);
    auto result_size =
        std::min(length - lpos, STANDARD_VECTOR_SIZE - result_count);
    output_copy(end_vertex_list, r1data, alist.vertices.get() + (offset + lpos),
                result_count, result_size);

    for (idx_t i = 0; i < result_size; i++) {
      output_set_sel(end_vertex_list, output_sel, result_count, rpos);
      result_count++;
    }
    lpos += result_size;
    if (lpos == length) {
      lpos = 0;
      rpos++;
    }
    if (result_count == STANDARD_VECTOR_SIZE) {
      return result_count;
    }
  }

  return result_count;
}

template <typename S>
static idx_t FetchVertexesInternalMerge(
    CompactList &alist, std::vector<idx_t> &lpos, idx_t &rpos,
    duckdb::DataChunk &start_vertex_list, idx_t rsize,
    std::vector<S> &input_sel, S &output_sel,
    duckdb::DataChunk &end_vertex_list, idx_t output_col,
    const std::vector<GraphIndexInfo<int64_t *, S> *> &merge_rais,
    const std::vector<CompactList *> &compact_lists) {
  if (rpos >= rsize) {
    return 0;
  }

  idx_t result_count = 0;
  std::vector<int64_t *> data_vec =
      inputs_transform(start_vertex_list, (int)merge_rais.size());
  auto r1data = output_transform(end_vertex_list, output_col);

  while (rpos < rsize) {
    auto rid =
        input_getKthRID(start_vertex_list, data_vec[0], rpos, input_sel[0]);
    auto offset = alist.offsets.operator[](rid);
    auto length = alist.sizes.operator[](rid);
    auto result_size =
        std::min(length - lpos[0], STANDARD_VECTOR_SIZE - result_count);

    std::vector<int> offsetk(merge_rais.size() + 1, 0);
    std::vector<int> lengthk(merge_rais.size() + 1, 0);
    for (int k = 1; k < merge_rais.size(); ++k) {
      auto rid_other =
          input_getKthRID(start_vertex_list, data_vec[k], rpos, input_sel[k]);
      offsetk[k] = compact_lists[k]->offsets.operator[](rid_other);
      lengthk[k] = compact_lists[k]->sizes.operator[](rid_other);
    }

    bool possible = true;
    for (int j = 0; j < result_size; ++j) {
      int64_t nid = *(alist.vertices.get() + (offset + lpos[0] + j));
      int agrees = 0;
      // check in other lists
      for (int k = 1; k < merge_rais.size(); ++k) {
        // if (right_position != right_position_k) {
        //     std::cout << rpos << " " << right_position << " " <<
        //     right_position_k << std::endl;
        // }
        if (lpos[k] >= lengthk[k]) {
          possible = false;
          break;
        }
        idx_t nid_other =
            *(compact_lists[k]->vertices.get() + (offsetk[k] + lpos[k]));
        if (nid_other < nid) {
          lpos[k]++;
          --k;
          continue;
        } else if (nid_other == nid) {
          agrees++;
          continue;
        } else
          break;
      }
      if (!possible)
        break;
      if (agrees == merge_rais.size() - 1) {
        output_copy(end_vertex_list, r1data, &nid, result_count, 1);
        output_set_sel(end_vertex_list, output_sel, result_count, rpos);
        result_count++;
      }
    }

    lpos[0] += result_size;
    if (lpos[0] == length) {
      for (int i = 0; i < lpos.size(); ++i)
        lpos[i] = 0;
      rpos++;
    }
    if (result_count == STANDARD_VECTOR_SIZE) {
      return result_count;
    }
  }

  return result_count;
}

} // namespace relgo

#endif // DUCKDB_GRAPH_INDEX_HPP
