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

#include <robin_map.h>
#include <robin_set.h>
#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/functional/hash.hpp>
#include <uninitialized_vector.hpp>

#include "abstract_aggregate_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "bytell_hash_map.hpp"
#include "expression/aggregate_expression.hpp"
#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

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
For each group in the output, one AggregateResult is created.
This result contains:
[1] the current (primary) aggregated value,
[2] the number of rows that were used,
[3] a vector for additional current (secondary) aggregated values.

[2] is used for AVG and COUNT.
[3] is used for STDDEV_SAMP.

*/
template <typename ColumnDataType, typename AggregateType>
class AggregateResult {
 public:
  std::optional<AggregateType> current_primary_aggregate;
  size_t aggregate_count = 0;
  RowID row_id;

  using DistinctValues = tsl::robin_set<ColumnDataType, std::hash<ColumnDataType>, std::equal_to<ColumnDataType>,
                                        PolymorphicAllocator<ColumnDataType>>;

  // SecondaryAggregates and DistinctValues are unused in most cases. We store them in a separate variant that is
  // initialized as needed. This saves us a lot of memory in this critical data structure.
  using Details = std::variant<SecondaryAggregates<AggregateType>, DistinctValues>;

  void ensure_secondary_aggregates_initialized(boost::container::pmr::monotonic_buffer_resource& buffer) {
    if (_details) return;
    _details = std::make_unique<Details>(
        SecondaryAggregates<AggregateType>{/*PolymorphicAllocator<AggregateType>(&buffer)*/});  // TODO move to cpp
  }

  SecondaryAggregates<AggregateType>& current_secondary_aggregates() {
    DebugAssert(_details, "AggregateResult::_details has not been properly initialized for this aggregation function");
    return std::get<SecondaryAggregates<AggregateType>>(*_details);
  }

  const SecondaryAggregates<AggregateType>& current_secondary_aggregates() const {
    DebugAssert(_details, "AggregateResult::_details has not been properly initialized for this aggregation function");
    return std::get<SecondaryAggregates<AggregateType>>(*_details);
  }

  void ensure_distinct_values_initialized(boost::container::pmr::monotonic_buffer_resource& buffer) {
    if (_details) return;
    _details = std::make_unique<Details>(DistinctValues{&buffer});  // TODO move to cpp
  }

  DistinctValues& distinct_values() {
    DebugAssert(_details, "AggregateResult::_details has not been properly initialized for this aggregation function");
    return std::get<DistinctValues>(*_details);
  }

  const DistinctValues& distinct_values() const {
    DebugAssert(_details, "AggregateResult::_details has not been properly initialized for this aggregation function");
    return std::get<DistinctValues>(*_details);
  }

 protected:
  std::unique_ptr<Details> _details;
};

// This vector holds the results for every group that was encountered and is indexed by AggregateResultId.
template <typename ColumnDataType, typename AggregateType>
using AggregateResults = pmr_vector<AggregateResult<ColumnDataType, AggregateType>>;
using AggregateResultId = size_t;

// The AggregateResultIdMap maps AggregateKeys to their index in the list of aggregate results.
template <typename AggregateKey>
using AggregateResultIdMapAllocator = PolymorphicAllocator<std::pair<const AggregateKey, AggregateResultId>>;

template <typename AggregateKey>
using AggregateResultIdMap =
    ska::bytell_hash_map<AggregateKey, AggregateResultId, std::hash<AggregateKey>, std::equal_to<AggregateKey>,
                         AggregateResultIdMapAllocator<AggregateKey>>;

// The key type that is used for the aggregation map.
using AggregateKeyEntry = uint64_t;

// A dummy type used as AggregateKey if no GROUP BY columns are present
struct EmptyAggregateKey {};

// Used to store AggregateKeys if more than 2 GROUP BY columns are used
constexpr auto AGGREGATE_KEY_SMALL_VECTOR_SIZE = 4;
using AggregateKeySmallVector = boost::container::small_vector<AggregateKeyEntry, AGGREGATE_KEY_SMALL_VECTOR_SIZE,
                                                               PolymorphicAllocator<AggregateKeyEntry>>;

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
  AggregateHash(const std::shared_ptr<AbstractOperator>& in,
                const std::vector<std::shared_ptr<AggregateExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

  // write the aggregated output for a given aggregate column
  template <typename ColumnDataType, AggregateFunction function>
  void write_aggregate_output(ColumnID aggregate_index);

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
  KeysPerChunk<AggregateKey> _partition_by_groupby_keys() const;

  template <typename AggregateKey>
  void _aggregate();

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  template <typename ColumnDataType>
  void _write_aggregate_output(boost::hana::basic_type<ColumnDataType> type, ColumnID column_index,
                               AggregateFunction function);

  void _write_groupby_output(RowIDPosList& pos_list);

  template <typename ColumnDataType, AggregateFunction function, typename AggregateKey>
  void _aggregate_segment(ChunkID chunk_id, ColumnID column_index, const AbstractSegment& abstract_segment,
                          const KeysPerChunk<AggregateKey>& keys_per_chunk);

  template <typename AggregateKey>
  std::shared_ptr<SegmentVisitorContext> _create_aggregate_context(const DataType data_type,
                                                                   const AggregateFunction function) const;

  std::vector<std::shared_ptr<BaseValueSegment>> _groupby_segments;
  std::vector<std::shared_ptr<SegmentVisitorContext>> _contexts_per_column;
  bool _has_aggregate_functions;
};

}  // namespace opossum

namespace std {
template <>
struct hash<opossum::EmptyAggregateKey> {
  size_t operator()(const opossum::EmptyAggregateKey& key) const { return 0; }
};

template <>
struct hash<opossum::AggregateKeySmallVector> {
  size_t operator()(const opossum::AggregateKeySmallVector& key) const {
    return boost::hash_range(key.begin(), key.end());
  }
};

template <>
struct hash<std::array<opossum::AggregateKeyEntry, 2>> {
  // gcc9 doesn't support templating by `int N` here.
  size_t operator()(const std::array<opossum::AggregateKeyEntry, 2>& key) const {
    return boost::hash_range(key.begin(), key.end());
  }
};
}  // namespace std
