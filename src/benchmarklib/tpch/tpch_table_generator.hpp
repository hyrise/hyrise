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
#include "tpch_constants.hpp"
#include "types.hpp"

namespace hyrise {

enum class TPCHTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };

extern const std::unordered_map<TPCHTable, std::string> tpch_table_names;

/**
 * Wrapper around the official tpch-dbgen tool, making it directly generate hyrise::Table instances without having
 * to generate and then load .tbl files.
 *
 * NOT thread safe because the underlying tpch-dbgen is not (since it has global data and malloc races).
 */
class TPCHTableGenerator : virtual public AbstractTableGenerator {
 public:
  // Convenience constructor for creating a TPCHTableGenerator without a benchmarking context
  explicit TPCHTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                              ChunkOffset chunk_size = Chunk::DEFAULT_SIZE);

  // Constructor for creating a TPCHTableGenerator in a benchmark
  explicit TPCHTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                              const std::shared_ptr<BenchmarkConfig>& benchmark_config);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

  std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> create_orders_and_lineitem_tables(const size_t order_count, const size_t index_offset) const;
  std::shared_ptr<Table> create_customer_table(const size_t customer_count, const size_t index_offset) const;

  size_t customer_row_count() const;
  size_t orders_row_count() const;
  size_t part_row_count() const;
  size_t supplier_row_count() const;
  size_t nation_row_count() const;
  size_t region_row_count() const;

  std::tuple<std::vector<DataType>, std::vector<std::string>, std::vector<bool>> get_table_column_information(const auto& table_name) const;

  std::shared_ptr<Table> create_empty_table(std::string&& table_name) const;

  void reset_and_initialize();

 protected:
  IndexesByTable _indexes_by_table() const override;
  SortOrderByTable _sort_order_by_table() const override;
  void _add_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const override;

  const float _scale_factor;
  const ClusteringConfiguration _clustering_configuration;
};
}  // namespace hyrise
