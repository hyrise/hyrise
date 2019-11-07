#pragma once

#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/functional/hash.hpp>

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
 * Operator to aggregate columns by certain functions such as min, max, sum, average, and count with a sort-based approach.
 * The output is a table with value segments.
 *
 * As with most operators we do not guarantee a stable operation with regards to positions - i.e. your sorting order.
 * This might sound surprising, since this is the sort-based aggregate, so here the reasoning:
 * The output table of this operator contains only the columns we have grouped by, and the aggregates.
 * The aggregate columns contain new values, so there is nothing they could be stable to.
 * The input table is sorted after the group by columns, using the Sort operator.
 * Thus we cannot expect the group by columns to keep their original order (unless they were sorted).
 *
 * However, you can expect the group by columns to be sorted, currently from the last to the first one.
 *
 * The following wiki entry contains some information about the aggregate operator:
 * https://github.com/hyrise/hyrise/wiki/Operators_Aggregate .
 * While most of this page refers to the hash-based aggregate, it also explains common features like aggregate traits.
 *
 * Some notes regarding future optimization:
 * Currently, we always sort the input table by the group by columns.
 * In some cases this might be unnecessary, as the table could already be sorted.
 * There is an issue that discusses how such information as sortedness should be propagated:
 *  https://github.com/hyrise/hyrise/issues/1519
 * As soon as this issue is decided, the sort aggregate operator should be adapted to benefit from sortedness.
 *  If the issue comes to the conclusion that it is the optimizer's responsibility to be aware of sortedness,
 *  this operator might be refactored to expect sorted input and to not sort at all;
 *  and let the optimizer add the required sort operators to the LQP.
 *
 *  To be precise: We do NOT need the input to be sorted.
 *  What we actually need is that all rows belonging to the same group are consecutive,
 *  and sorting is merely a technique to achieve consecutiveness.
 *   There is one minor thing we lose if the input is consecutive but not sorted: the output will not be sorted either.
 *   However, the output of an aggregate is usually much smaller than the input, so it is still more efficient
 *   to aggregate on the consecutive input and then sort the output, than sorting the input and then aggregating it.
 */
class AggregateSort : public AbstractAggregateOperator {
 public:
  AggregateSort(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

  /**
   * Creates the aggregate column definitions and appends it to <code>_output_column_definitions</code>
   * We need the input column data type because the aggregate type can depend on it.
   * For example, MAX on an int column yields ints, while MAX on a string column yield string values.
   *
   * @tparam ColumnType the data type of the input column
   * @tparam function the aggregate function used, e.g. AggregateFunction::Sum
   * @param column_index determines for which aggregate column definitions should be created
   */
  template <typename ColumnType, AggregateFunction function>
  void create_aggregate_column_definitions(ColumnID column_index);

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
  void _aggregate_values(const std::set<RowID>& group_boundaries, const uint64_t aggregate_index,
                         const std::shared_ptr<const Table>& sorted_table);

  template <typename ColumnType>
  void _create_aggregate_column_definitions(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                                            AggregateFunction function);

  /*
   * Some of the parameters are marked as [[maybe_unused]].
   * This is required because it depends on the <code>function</code> template parameter whether arguments are used or not.
   */
  template <typename AggregateType, AggregateFunction function>
  void _set_and_write_aggregate_value(std::vector<AggregateType>& aggregate_results,
                                      std::vector<bool>& aggregate_null_values, const uint64_t aggregate_group_index,
                                      [[maybe_unused]] const uint64_t aggregate_index,
                                      std::optional<AggregateType>& current_primary_aggregate,
                                      std::vector<AggregateType>& current_secondary_aggregates,
                                      [[maybe_unused]] const uint64_t value_count,
                                      [[maybe_unused]] const uint64_t value_count_with_null,
                                      [[maybe_unused]] const uint64_t unique_value_count) const;
};

}  // namespace opossum
