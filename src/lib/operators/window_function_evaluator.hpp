#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "all_type_variant.hpp"
#include "concurrency/transaction_context.hpp"
#include "storage/table.hpp"

namespace hyrise {

class WindowFunctionEvaluator : public AbstractReadOnlyOperator {
 public:
  using AbstractReadOnlyOperator::AbstractReadOnlyOperator;

  static constexpr uint8_t hash_partition_bits = 8;
  static constexpr size_t hash_partition_mask = (1u << hash_partition_bits) - 1;
  static constexpr uint32_t hash_partition_partition_count = 1u << hash_partition_bits;

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  using PartitionedData = std::vector<std::pair<std::vector<AllTypeVariant>, RowID>>;

  PartitionedData partition_and_sort() const;
  void compute_window_function(const PartitionedData& partitioned_data, auto&& emit_computed_value) const;
  template <typename T>
  std::shared_ptr<const Table> annotate_input_table(std::vector<ValueSegment<T>> segments_for_output_column) const;

  ColumnID _partition_by_column_id;
  ColumnID _order_by_column_id;
};

}  // namespace hyrise
