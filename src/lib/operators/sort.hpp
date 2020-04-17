#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Defines in which order a certain column should be sorted.
 */
struct SortColumnDefinition final {
  explicit SortColumnDefinition(const ColumnID& init_column,
                                const OrderByMode init_order_by_mode = OrderByMode::Ascending)
      : column(init_column), order_by_mode(init_order_by_mode) {}

  const ColumnID column;
  const OrderByMode order_by_mode;
};

/**
 * Operator to sort a table by one or multiple columns. This implements a stable sort, i.e., rows that share the same
 * value will maintain their relative order.
 * By passing multiple sort column definitions it is possible to sort multiple columns with one operator run.
 */
class Sort : public AbstractReadOnlyOperator {
 public:
  Sort(const std::shared_ptr<const AbstractOperator>& in, const std::vector<SortColumnDefinition>& sort_definitions,
       const ChunkOffset output_chunk_size = Chunk::DEFAULT_SIZE);

  const std::vector<SortColumnDefinition>& sort_definitions() const;

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  template <typename SortColumnType>
  class SortImpl;

  template <typename SortColumnType>
  class SortImplMaterializeOutput;

  const std::vector<SortColumnDefinition> _sort_definitions;

  const ChunkOffset _output_chunk_size;
};

}  // namespace opossum
