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

#include <boost/container/scoped_allocator.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "uninitialized_vector.hpp"

#include "abstract_aggregate_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "aggregate/window_function_traits.hpp"
#include "expression/window_function_expression.hpp"
#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// empty base class for AggregateResultContext
class SegmentVisitorContext {};

template <typename AggregateKey>
struct GroupByContext;

/*
Operator to aggregate columns by certain functions, such as min, max, sum, average, count and stddev_samp. The output is a table
 with value segments. As with most operators we do not guarantee a stable operation with regards to positions -
 i.e. your sorting order.

For implementation details, please check the wiki: https://github.com/hyrise/hyrise/wiki/Operators_Aggregate
*/

/*


For each group in the output, one AggregateResult is created per aggregate function. If no GROUP BY columns are used,
one AggregateResult exists per aggregate function.

This result contains:
- the current (primary) aggregated value,
- the number of rows that were used, which are used for AVG, COUNT, and STDDEV_SAMP,
- a RowID, pointing into the input data, for any row that belongs into this group. This is needed to fill the GROUP BY
  columns later

Optionally, the result may also contain:
- a set of DISTINCT values OR
- secondary aggregates, which are currently only used by STDDEV_SAMP
*/
template <typename ColumnDataType, WindowFunction aggregate_function>
struct AggregateResult {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;

  using DistinctValues = boost::unordered_flat_set<ColumnDataType, std::hash<ColumnDataType>,
                                                   std::equal_to<ColumnDataType>, PolymorphicAllocator<ColumnDataType>>;

  // Find the correct accumulator type using nested conditionals.
  using AccumulatorType = std::conditional_t<
      // For StandardDeviationSample, use StandardDeviationSampleData as the accumulator,
      aggregate_function == WindowFunction::StandardDeviationSample, StandardDeviationSampleData,
      // for CountDistinct, use DistinctValues, otherwise use AggregateType
      std::conditional_t<aggregate_function == WindowFunction::CountDistinct, DistinctValues, AggregateType>>;

  AccumulatorType accumulator{};
  size_t aggregate_count = 0;

  // As described above, this stores a pointer into the input data that is used to later restore the GROUP BY values.
  // A NULL_ROW_ID means that the aggregate result is not (yet) valid and should be skipped when materializing the
  // results. There is no ambiguity with actual NULLs because the aggregate operator is not NULL-producing. As such,
  // we know that each valid GROUP BY-group has at least one valid input RowID. As the input may be a ReferenceSegment,
  // a valid RowID may *point to* a row that is NULL.
  RowID row_id{NULL_ROW_ID};

  // Note that the size of this struct is a significant performance factor (see #2252). Be careful when adding fields or
  // changing data types.
};

// This vector holds the results for every group that was encountered and is indexed by AggregateResultId.
template <typename ColumnDataType, WindowFunction aggregate_function>
using AggregateResults = pmr_vector<AggregateResult<ColumnDataType, aggregate_function>>;
using AggregateResultId = size_t;

// The AggregateResultIdMap maps AggregateKeys to their index in the list of aggregate results.
template <typename AggregateKey>
using AggregateResultIdMapAllocator = PolymorphicAllocator<std::pair<const AggregateKey, AggregateResultId>>;

template <typename AggregateKey>
using AggregateResultIdMap =
    boost::unordered_flat_map<AggregateKey, AggregateResultId, std::hash<AggregateKey>, std::equal_to<AggregateKey>,
                              AggregateResultIdMapAllocator<AggregateKey>>;

// The key type that is used for the aggregation map.
using AggregateKeyEntry = uint64_t;

// A dummy type used as AggregateKey if no GROUP BY columns are present
struct EmptyAggregateKey {};

// Used to store AggregateKeys if more than 2 GROUP BY columns are used. The size is a trade-off between the memory
// consumption in the AggregateKeys vector (which becomes more expensive to read) and the cost of heap lookups.
// It could also be, e.g., 5, which would be better for queries with 5 GROUP BY columns but worse for queries with 3 or
// 4 GROUP BY columns.
constexpr auto AGGREGATE_KEY_SMALL_VECTOR_SIZE = 4;
using AggregateKeySmallVector = boost::container::small_vector<AggregateKeyEntry, AGGREGATE_KEY_SMALL_VECTOR_SIZE,
                                                               PolymorphicAllocator<AggregateKeyEntry>>;

// Conceptually, this is a vector<AggregateKey> that, for each row of the input, holds the AggregateKey. For trivially
// constructible AggregateKey types, we use an uninitialized_vector, which is cheaper to construct.
template <typename AggregateKey>
using AggregateKeys =
    std::conditional_t<std::is_same_v<AggregateKey, AggregateKeySmallVector>, pmr_vector<AggregateKey>,
                       uninitialized_vector<AggregateKey, PolymorphicAllocator<AggregateKey>>>;

template <typename AggregateKey>
using KeysPerChunk = pmr_vector<AggregateKeys<AggregateKey>>;

/**
 * Types that are used for the special COUNT(*) and DISTINCT implementations
 */
using CountColumnType = int32_t;
using CountAggregateType = int64_t;
using DistinctColumnType = int8_t;
using DistinctAggregateType = int8_t;

class AggregateHash : public AbstractAggregateOperator {
 public:
  AggregateHash(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

  enum class OperatorSteps : uint8_t {
    GroupByKeyPartitioning,
    Aggregating,
    GroupByColumnsWriting,
    AggregateColumnsWriting,
    OutputWriting
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template <typename AggregateKey>
  KeysPerChunk<AggregateKey> _partition_by_groupby_keys();

  template <typename AggregateKey>
  void _aggregate();

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  void _write_groupby_output(RowIDPosList& pos_list);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  void _write_aggregate_output(ColumnID aggregate_index);

  template <typename ColumnDataType, WindowFunction aggregate_function, typename AggregateKey>
  void _aggregate_segment(ChunkID chunk_id, ColumnID column_index, const AbstractSegment& abstract_segment,
                          KeysPerChunk<AggregateKey>& keys_per_chunk);

  template <typename AggregateKey>
  std::shared_ptr<SegmentVisitorContext> _create_aggregate_context(const DataType data_type,
                                                                   const WindowFunction aggregate_function) const;

  // Data structure used to gather intermediate results of grouping and aggregation. This data structure stores both
  // the PosLists for group-by columns as well as the materialized aggregate results that are later returned as
  // EntirePosList ReferenceSegments in the output table.
  std::vector<Segments> _intermediate_result;

  std::vector<std::shared_ptr<BaseValueSegment>> _groupby_segments;
  std::vector<std::shared_ptr<SegmentVisitorContext>> _contexts_per_column;
  bool _has_aggregate_functions;

  std::atomic_size_t _expected_result_size{};
  bool _use_immediate_key_shortcut{};

  std::chrono::nanoseconds groupby_columns_writing_duration{};
  std::chrono::nanoseconds aggregate_columns_writing_duration{};
};

}  // namespace hyrise

namespace std {
template <>
struct hash<hyrise::EmptyAggregateKey> {
  size_t operator()(const hyrise::EmptyAggregateKey& key) const {
    return 0;
  }
};

template <>
struct hash<hyrise::AggregateKeySmallVector> {
  size_t operator()(const hyrise::AggregateKeySmallVector& key) const {
    return boost::hash_range(key.begin(), key.end());
  }
};

template <>
struct hash<std::array<hyrise::AggregateKeyEntry, 2>> {
  // gcc9 doesn't support templating by `int N` here.
  size_t operator()(const std::array<hyrise::AggregateKeyEntry, 2>& key) const {
    return boost::hash_range(key.begin(), key.end());
  }
};
}  // namespace std
