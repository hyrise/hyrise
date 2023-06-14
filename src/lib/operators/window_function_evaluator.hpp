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
  // TODO: Think about not copying the data, but adding segments to all chunks of the input table

 public:
  using AbstractReadOnlyOperator::AbstractReadOnlyOperator;

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  using PartitionedData = std::vector<std::vector<std::pair<AllTypeVariant, RowID>>>;

  PartitionedData partition_and_sort() const;
  void compute_window_function(const PartitionedData& partitioned_data, auto&& emit_computed_value) const;
  template <typename T>
  std::shared_ptr<const Table> annotate_input_table(std::vector<ValueSegment<T>> segments_for_output_column) const;
};

}  // namespace hyrise
