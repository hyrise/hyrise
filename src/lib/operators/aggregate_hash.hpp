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
#include "bytell_hash_map.hpp"
#include "expression/aggregate_expression.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment_visitor.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

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
struct AggregateResult {
  std::optional<AggregateType> current_primary_aggregate;
  std::vector<AggregateType> current_secondary_aggregates;
  size_t aggregate_count = 0;
  std::set<ColumnDataType> distinct_values;
  RowID row_id;
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

/*
The key type that is used for the aggregation map.
*/
using AggregateKeyEntry = uint64_t;

template <typename AggregateKey>
using AggregateKeys = std::vector<AggregateKey>;

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
  AggregateHash(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string name() const override;

  // write the aggregated output for a given aggregate column
  template <typename ColumnDataType, AggregateFunction function>
  void write_aggregate_output(ColumnID column_index);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template <typename AggregateKey>
  void _aggregate();

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  template <typename ColumnDataType>
  void _write_aggregate_output(boost::hana::basic_type<ColumnDataType> type, ColumnID column_index,
                               AggregateFunction function);

  void _write_groupby_output(PosList& pos_list);

  template <typename ColumnDataType, AggregateFunction function, typename AggregateKey>
  void _aggregate_segment(ChunkID chunk_id, ColumnID column_index, const BaseSegment& base_segment,
                          const KeysPerChunk<AggregateKey>& keys_per_chunk);

  template <typename AggregateKey>
  std::shared_ptr<SegmentVisitorContext> _create_aggregate_context(const DataType data_type,
                                                                   const AggregateFunction function) const;

  std::vector<std::shared_ptr<BaseValueSegment>> _groupby_segments;
  std::vector<std::shared_ptr<SegmentVisitorContext>> _contexts_per_column;
};

}  // namespace opossum

namespace std {
template <>
struct hash<std::vector<opossum::AggregateKeyEntry>> {
  size_t operator()(const std::vector<opossum::AggregateKeyEntry>& key) const {
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
