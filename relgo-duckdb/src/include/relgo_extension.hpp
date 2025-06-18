#pragma once

#include "duckdb/main/extension.hpp"
namespace duckdb {

class RelgoExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
  //   std::string Version() const override;
};

} // namespace duckdb