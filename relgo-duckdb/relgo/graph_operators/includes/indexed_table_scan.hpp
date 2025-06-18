//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/indexed_table_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../../graph_index/physical_graph_index.hpp"
#include "../includes/indexed_table_function.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
class DuckTableEntry;
class TableCatalogEntry;

//! The table scan function represents a sequential scan over one of DuckDB's
//! base tables.
struct IndexedTableScanFunction {
  // static void RegisterFunction(BuiltinFunctions &set);
  static IndexedTableFunction GetFunction();
  static IndexedTableFunction GetIndexScanFunction();
  static optional_ptr<TableCatalogEntry>
  GetTableEntry(const IndexedTableFunction &function,
                const optional_ptr<FunctionData> bind_data);
};

} // namespace duckdb
