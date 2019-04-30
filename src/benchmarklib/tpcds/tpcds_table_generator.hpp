#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_table_generator.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace opossum {

class Chunk;
class Table;

enum class TpcdsTable {
    CallCenter,
    CatalogPage,
    CatalogReturns,
    CatalogSales,
    Customer,
    CustomerAddress,
    CustomerDemographics,
    Date,
    HouseholdDemographics,
    IncomeBand,
    Inventory,
    Item,
    Promotion,
    Reason,
    ShipMode,
    Store,
    StoreReturns,
    StoreSales,
    Time,
    Warehouse,
    WebPage,
    WebReturns,
    WebSales,
    WebSite,
    DbgenVersion,
};

extern std::unordered_map<TpcdsTable, std::string> tpcds_table_names;

/**
 * Wrapper around the official tpcds-dbgen tool, making it directly generate opossum::Table instances without having
 * to generate and then load .tbl files.
 *
 * NOT thread safe because the underlying tpcds-dbgen is not (since it has global data and malloc races).
 */
class TpcdsTableGenerator final : public AbstractTableGenerator {
 public:
  // Convenience constructor for creating a TpcdsTableGenerator out of a benchmarking context
  explicit TpcdsTableGenerator(float scale_factor, uint32_t chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a TpcdsTableGenerator in a benchmark
  explicit TpcdsTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 private:
  float _scale_factor;
};
}  // namespace opossum
