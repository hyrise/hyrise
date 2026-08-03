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
#include "dyod_window_function_builder.hpp"
#include "expression/window_function_expression.hpp"
#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

template <typename AggregateKey>
struct GroupByContext;

/*
Operator to aggregate columns by certain functions, such as min, max, sum, average, count and stddev_samp. The output is a table
 with value segments. As with most operators we do not guarantee a stable operation with regards to positions -
 i.e. your sorting order.

For implementation details, please check the wiki: https://github.com/hyrise/hyrise/wiki/Operators_Aggregate
*/

/*


For each group in the output, one DYODAggregateResult is created per aggregate function. If no GROUP BY columns are used,
one DYODAggregateResult exists per aggregate function.

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
struct DYODAggregateResult {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;

  using DistinctValues = boost::unordered_flat_set<ColumnDataType, std::hash<ColumnDataType>, std::equal_to<>,
                                                   PolymorphicAllocator<ColumnDataType>>;

  // Find the correct accumulator type using nested conditionals.
  using AccumulatorType = std::conditional_t<
      // For StandardDeviationSample, use DYODStandardDeviationSampleData as the accumulator,
      aggregate_function == WindowFunction::StandardDeviationSample, DYODStandardDeviationSampleData,
      // for Avg, use DYODAvgData
      std::conditional_t<
          aggregate_function == WindowFunction::Avg, DYODAvgData<AggregateType>,
          // for CountDistinct, use DistinctValues, otherwise use AggregateType
          std::conditional_t<aggregate_function == WindowFunction::CountDistinct, DistinctValues, AggregateType>>>;

  AccumulatorType accumulator{};

  // As described above, this stores a pointer into the input data that is used to later restore the GROUP BY values.
  // A NULL_ROW_ID means that the aggregate result is not (yet) valid and should be skipped when materializing the
  // results. There is no ambiguity with actual NULLs because the aggregate operator is not NULL-producing. As such,
  // we know that each valid GROUP BY-group has at least one valid input RowID. As the input may be a ReferenceSegment,
  // a valid RowID may *point to* a row that is NULL.
  RowID row_id{NULL_ROW_ID};

  bool has_aggregates = false;
  // Note that the size of this struct is a significant performance factor (see #2252). Be careful when adding fields or
  // changing data types.
};

// empty base class for DYODAggregateResultContext
class DYODSegmentVisitorContext {};

// This vector holds the results for every group that was encountered and is indexed by DYODAggregateResultId.
template <typename ColumnDataType, WindowFunction aggregate_function>
using DYODAggregateResults = pmr_vector<DYODAggregateResult<ColumnDataType, aggregate_function>>;
using DYODAggregateResultId = size_t;
using ContextsPerColumn = std::vector<std::shared_ptr<DYODSegmentVisitorContext>>;

// The DYODAggregateResultIdMap maps AggregateKeys to their index in the list of aggregate results.
template <typename AggregateKey>
using DYODAggregateResultIdMapAllocator = PolymorphicAllocator<std::pair<const AggregateKey, DYODAggregateResultId>>;

template <typename AggregateKey>
using DYODAggregateResultIdMap =
    boost::unordered_flat_map<AggregateKey, DYODAggregateResultId, std::hash<AggregateKey>, std::equal_to<AggregateKey>,
                              DYODAggregateResultIdMapAllocator<AggregateKey>>;

// The key type that is used for the aggregation map.
using DYODAggregateKeyEntry = uint64_t;

// A dummy type used as AggregateKey if no GROUP BY columns are present
struct DYODEmptyAggregateKey {};

// Used to store AggregateKeys if more than 2 GROUP BY columns are used. The size is a trade-off between the memory
// consumption in the AggregateKeys vector (which becomes more expensive to read) and the cost of heap lookups.
// It could also be, e.g., 5, which would be better for queries with 5 GROUP BY columns but worse for queries with 3 or
// 4 GROUP BY columns.
constexpr auto DYOD_AGGREGATE_KEY_SMALL_VECTOR_SIZE = 4;
using DYODAggregateKeySmallVector =
    boost::container::small_vector<DYODAggregateKeyEntry, DYOD_AGGREGATE_KEY_SMALL_VECTOR_SIZE,
                                   PolymorphicAllocator<DYODAggregateKeyEntry>>;

// Conceptually, this is a vector<AggregateKey> that, for each row of the input, holds the AggregateKey. For trivially
// constructible AggregateKey types, we use an uninitialized_vector, which is cheaper to construct.
template <typename AggregateKey>
using AggregateKeys =
    std::conditional_t<std::is_same_v<AggregateKey, DYODAggregateKeySmallVector>, pmr_vector<AggregateKey>,
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

class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
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

  template <typename CheckForSingleKey, typename AggregateKey>
    requires(std::is_same_v<AggregateKey, DYODEmptyAggregateKey>)
  KeysPerChunk<AggregateKey> _partition_by_groupby_keys(const std::shared_ptr<const Table>& input_table,
                                                        std::atomic_size_t& expected_result_size,
                                                        bool& use_immediate_key_shortcut, bool& guarantee_single_key);

  template <typename CheckForSingleKey, typename AggregateKey>
    requires(!std::is_same_v<AggregateKey, DYODEmptyAggregateKey>)
  KeysPerChunk<AggregateKey> _partition_by_groupby_keys(const std::shared_ptr<const Table>& input_table,
                                                        std::atomic_size_t& expected_result_size,
                                                        bool& use_immediate_key_shortcut, bool& guarantee_single_key);

  template <typename AggregateKey>
  void _aggregate(ContextsPerColumn& contexts_per_column, const std::shared_ptr<const Table>& input_table,
                  bool check_for_single_keys);

  template <typename AggregateKey>
  void _aggregate(ContextsPerColumn& contexts_per_column, const std::shared_ptr<const Table>& input_table,
                  std::atomic_size_t& expected_result_size, bool& use_immediate_key_shortcut,
                  KeysPerChunk<AggregateKey>& keys_per_chunk, ChunkID start, ChunkID end);

  template <typename ColumnDataType, WindowFunction aggregate_function, typename AggregateKey>
  void _merge_contexts(std::shared_ptr<DYODSegmentVisitorContext>& target,
                       std::shared_ptr<DYODSegmentVisitorContext>& other);

  template <typename AggregateKey>
  std::shared_ptr<Table> _partition_and_aggregate();

  template <typename IsReferenceTable, typename AggregateKey>
  std::shared_ptr<Table> _partition_and_aggregate();

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  void _write_groupby_output(RowIDPosList& pos_list);

  template <typename ColumnDataType, WindowFunction aggregate_function>
  void _write_aggregate_output(ColumnID aggregate_index, ContextsPerColumn& contexts_per_column,
                               std::vector<Segments>& intermediate_result,
                               const std::shared_ptr<const Table>& input_table,
                               TableColumnDefinitions& output_column_definitions);

  void _write_output(ContextsPerColumn& contexts_per_column, const std::shared_ptr<const Table>& input_table,
                     std::shared_ptr<Table>& output_table, std::shared_ptr<Table>& aggregate_result_table);

  template <typename ColumnDataType, WindowFunction aggregate_function, typename AggregateKey>
  void _aggregate_segment(ChunkID chunk_id, ColumnID column_index, const AbstractSegment& abstract_segment,
                          KeysPerChunk<AggregateKey>& keys_per_chunk, ContextsPerColumn& contexts_per_column,
                          bool use_immediate_key_shortcut);

  template <typename AggregateKey>
  std::shared_ptr<DYODSegmentVisitorContext> _create_aggregate_context(const DataType data_type,
                                                                       const WindowFunction aggregate_function,
                                                                       std::atomic_size_t& expected_result_size) const;

  bool _has_aggregate_functions;
  bool _aggregate_writing_started = false;
  bool _output_writing_started = false;
  std::mutex _aggregate_mutex = std::mutex{};
  std::mutex _output_mutex = std::mutex{};
};

}  // namespace hyrise

namespace std {
template <>
struct hash<hyrise::DYODEmptyAggregateKey> {
  size_t operator()(const hyrise::DYODEmptyAggregateKey& /*key*/) const {
    return 0;
  }
};

}  // namespace std
