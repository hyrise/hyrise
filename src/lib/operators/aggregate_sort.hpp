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

#include "abstract_read_only_operator.hpp"
#include "abstract_aggregate_operator.hpp"
#include "expression/aggregate_expression.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment_visitor.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/*
Operator to aggregate columns sort-based by certain functions, such as min, max, sum, average, and count. The output is a table
 with reference segments. As with most operators we do not guarantee a stable operation with regards to positions -
 i.e. your sorting order.

For implementation details, please check the wiki: https://github.com/hyrise/hyrise/wiki/Aggregate-Operator
*/

/**
 * Note: Aggregate does not support null values at the moment
 */
class AggregateSort : public AbstractAggregateOperator {
 public:
  AggregateSort(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
            const std::vector<ColumnID>& groupby_column_ids);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  // write the aggregated output for a given aggregate column

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

  template<typename ColumnType, typename AggregateType, AggregateFunction function>
  void _aggregate_values(std::vector<RowID>& aggregate_group_offsets, uint64_t aggregate_index, AggregateFunctor<ColumnType, AggregateType> aggregate_function, std::shared_ptr<const Table> sorted_table);

  template <typename ColumnType>
  void _write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index, AggregateFunction function);


  template<typename ColumnType, typename AggregateType, AggregateFunction function>
  void _set_and_write_aggregate_value(std::vector<AggregateType> &aggregate_results,
                                                       std::vector<bool> &aggregate_null_values,
                                                       uint64_t aggregate_group_index,
                                                       uint64_t aggregate_index __attribute__((unused)),
                                                       std::optional<AggregateType> &current_aggregate_value,
                                                       uint64_t value_count  __attribute__((unused)),
                                                       uint64_t value_count_with_null  __attribute__((unused)),
                                                       const std::unordered_set<ColumnType> &unique_values) const;

};

}  // namespace opossum