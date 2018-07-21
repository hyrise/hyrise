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
#include "resolve_type.hpp"
#include "storage/column_visitable.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

struct GroupByContext;

/**
 * Aggregates are defined by the Column (ColumnID for Operators, ColumnReference in LQP) they operate on and the aggregate
 * function they use. COUNT() is the exception that doesn't use a Column, which is why column is optional
 * Optionally, an alias can be specified to use as the output name.
 */
template <typename ColumnReferenceType>
struct AggregateColumnDefinitionTemplate {
  AggregateColumnDefinitionTemplate(const std::optional<ColumnReferenceType>& column, const AggregateFunction function,
                                    const std::optional<std::string>& alias = std::nullopt)
      : column(column), function(function), alias(alias) {}

  std::optional<ColumnReferenceType> column{std::nullopt};
  AggregateFunction function;
  std::optional<std::string> alias;
};

/*
Operator to aggregate columns by certain functions, such as min, max, sum, average, and count. The output is a table
 with reference columns. As with most operators we do not guarantee a stable operation with regards to positions -
 i.e. your sorting order.

For implementation details, please check the wiki: https://github.com/hyrise/hyrise/wiki/Aggregate-Operator
*/

/*
Current aggregated value and the number of rows that were used.
The latter is used for AVG and COUNT.
*/
template <typename AggregateType, typename ColumnDataType>
struct AggregateResult {
  std::optional<AggregateType> current_aggregate;
  size_t aggregate_count = 0;
  std::set<ColumnDataType> distinct_values;
  RowID row_id;
};

/*
The key type that is used for the aggregation map.
*/
using AggregateKeyEntry = uint64_t;
using AggregateKey = pmr_vector<AggregateKeyEntry>;
using AggregateKeys = pmr_vector<AggregateKey>;
using KeysPerChunk = pmr_vector<AggregateKeys>;

// We use monotonic_buffer_resource for the vector of vectors that hold the aggregate keys. That is so that we can
// save time when allocating and we can throw away everything in this temporary structure at once (once the resource
// gets deleted). Also, we use the scoped_allocator_adaptor to propagate the allocator to all inner vectors.
// This is suitable here because the amount of memory needed is known from the start. In other places with frequent
// reallocations, this might make less sense.
// We use boost over std because libc++ does not yet (July 2018) support monotonic_buffer_resource:
// https://libcxx.llvm.org/ts1z_status.html
using AggregateKeysAllocator = boost::container::scoped_allocator_adaptor<PolymorphicAllocator<AggregateKeys>>;

using AggregateColumnDefinition = AggregateColumnDefinitionTemplate<ColumnID>;

/**
 * Types that are used for the special COUNT(*) and DISTINCT implementations
 */
using CountColumnType = int32_t;
using CountAggregateType = int64_t;
using DistinctColumnType = int8_t;
using DistinctAggregateType = int8_t;

/**
 * Note: Aggregate does not support null values at the moment
 */
class Aggregate : public AbstractReadOnlyOperator {
 public:
  Aggregate(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
            const std::vector<ColumnID>& groupby_column_ids);

  const std::vector<AggregateColumnDefinition>& aggregates() const;
  const std::vector<ColumnID>& groupby_column_ids() const;

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  // write the aggregated output for a given aggregate column
  template <typename ColumnType, AggregateFunction function>
  void write_aggregate_output(ColumnID column_index);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

  void _on_cleanup() override;

  template <typename ColumnType>
  static void _create_aggregate_context(boost::hana::basic_type<ColumnType> type,
                                        std::shared_ptr<ColumnVisitableContext>& aggregate_context,
                                        AggregateFunction function);

  template <typename ColumnType>
  static void _create_aggregate_visitor(boost::hana::basic_type<ColumnType> type,
                                        std::shared_ptr<ColumnVisitable>& builder,
                                        std::shared_ptr<ColumnVisitableContext> context,
                                        std::shared_ptr<GroupByContext> groupby_context, AggregateFunction function);

  template <typename ColumnType>
  void _write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                               AggregateFunction function);

  void _write_groupby_output(PosList& pos_list);

  template <typename ColumnDataType, AggregateFunction function>
  void _aggregate_column(ChunkID chunk_id, ColumnID column_index, const BaseColumn& base_column,
                         const KeysPerChunk& keys_per_chunk);

  std::shared_ptr<ColumnVisitableContext> _create_aggregate_context(const DataType data_type,
                                                                    const AggregateFunction function) const;

  template <typename ColumnDataType, AggregateFunction aggregate_function>
  std::shared_ptr<ColumnVisitableContext> _create_aggregate_context_impl() const;

  const std::vector<AggregateColumnDefinition> _aggregates;
  const std::vector<ColumnID> _groupby_column_ids;

  TableColumnDefinitions _output_column_definitions;
  ChunkColumns _output_columns;

  ChunkColumns _groupby_columns;
  std::vector<std::shared_ptr<ColumnVisitableContext>> _contexts_per_column;
};

}  // namespace opossum

namespace std {
template <>
struct hash<opossum::AggregateKey> {
  size_t operator()(const opossum::AggregateKey& key) const { return boost::hash_range(key.begin(), key.end()); }
};
}  // namespace std
