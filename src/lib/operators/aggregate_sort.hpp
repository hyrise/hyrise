#pragma once

#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/functional/hash.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_aggregate_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "expression/aggregate_expression.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment_visitor.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/*
 Operator to aggregate columns by certain functions such as min, max, sum, average, and count with a sort-based approach.
 The output is a table with value segments.

 As with most operators we do not guarantee a stable operation with regards to positions - i.e. your sorting order.
 This might sound surprising, since this is the sort-based aggregate, so here the reasoning:
 The output table of this operator contains only the columns we have grouped by, and the aggregates.
 The aggregate columns contain new values, so there is nothing they could be stable to.
 The input table is sorted after the group by columns, using the Sort operator.
 Thus we cannot expect the group by columns to keep their original order (unless they were sorted).

 The following wiki entry contains some information about the aggregate operator:
 https://github.com/hyrise/hyrise/wiki/Operators_Aggregate .
 While most of this page refers to the hash-based aggregate, it also explains common features like aggregate traits.
*/

/**
 * Note: Aggregate does not support null values at the moment
 */
class AggregateSort : public AbstractAggregateOperator {
 public:
  AggregateSort(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string name() const override;

  template <typename ColumnType, AggregateFunction function>
  void write_aggregate_output(ColumnID column_index);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  template <typename ColumnType, typename AggregateType>
  using AggregateFunctor = std::function<void(const ColumnType&, std::optional<AggregateType>&)>;

  template <typename ColumnType, typename AggregateType, AggregateFunction function>
  void _aggregate_values(std::set<RowID>& aggregate_group_offsets, uint64_t aggregate_index,
                         const std::shared_ptr<const Table>& sorted_table);

  template <typename ColumnType>
  void _write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                               AggregateFunction function);

  /*
   * Some of the parameters are marked as [[maybe_unused]].
   * This is required because it depends on the <code>function</code> template parameter whether arguments are used or not.
   */
  template <typename AggregateType, AggregateFunction function>
  void _set_and_write_aggregate_value(std::vector<AggregateType>& aggregate_results,
                                      std::vector<bool>& aggregate_null_values, uint64_t aggregate_group_index,
                                      [[maybe_unused]] uint64_t aggregate_index,
                                      std::optional<AggregateType>& current_aggregate_value,
                                      [[maybe_unused]] uint64_t value_count,
                                      [[maybe_unused]] uint64_t value_count_with_null,
                                      [[maybe_unused]] const uint64_t unique_value_count) const;
};

}  // namespace opossum
