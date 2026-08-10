#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "uninitialized_vector.hpp"

#include "abstract_aggregate_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "aggregate/window_function_traits.hpp"
#include "all_type_variant.hpp"
#include "expression/window_function_expression.hpp"
#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

 protected:
  template <WindowFunction aggregate_function, typename AggregateType>
  using AggregateAccumulator = std::conditional_t<aggregate_function == WindowFunction::StandardDeviationSample,
                                                  StandardDeviationSampleData, AggregateType>;

  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  using NormalizedKey = uint64_t;

  /*
    BaseAggregateResults provides a superclass to the templated versions of AggregteResults
    so that we can store pointers to AggregateResults of different types in the same vector.
  */
  struct BaseAggregateResults {
    virtual ~BaseAggregateResults() = default;
  };

  /*
    AggregateResults holds a vector of accumulators for aggregate_function for each group of
    a Morsel it is held in. It additionally stores the sizes for each group.
  */
  template <WindowFunction aggregate_function, typename AggregateType>
  struct AggregateResults : public BaseAggregateResults {
    explicit AggregateResults(const uint64_t group_count) : accumulators(group_count), counts(group_count) {}

    pmr_vector<AggregateAccumulator<aggregate_function, AggregateType>> accumulators;
    pmr_vector<uint64_t> counts;
  };

  /*
    This specialization for the CountDistinct function is needed as this requires a non-standard type of
    accumulator, namely, each group contains a vector of the unique values in this group.
  */
  template <typename ColumnType>
  struct AggregateResults<WindowFunction::CountDistinct, ColumnType> : public BaseAggregateResults {
    explicit AggregateResults(const uint64_t group_count) : accumulators(group_count), counts(group_count) {}

    pmr_vector<pmr_vector<ColumnType>> accumulators;
    pmr_vector<uint64_t> counts;
  };

  /*
    BaseMaterializedColumn provides a superclass to the templated versions of MaterializedColumn
    so that we can store pointers to MaterializedColumns of different types in the same vector.
  */
  struct BaseMaterializedColumn {
    virtual ~BaseMaterializedColumn() = default;
  };

  /*
    A MaterializedColumn holds values of type ColumnType and one byte for each value indicating
    whether it is null. This is basically only the core of a ValueSegment.
  */
  template <typename ColumnDataType>
  struct MaterializedColumn : public BaseMaterializedColumn {
    explicit MaterializedColumn(const size_t row_count) : values(row_count), null_values(row_count) {}

    pmr_vector<ColumnDataType> values;
    pmr_vector<uint8_t> null_values;
  };

  /*
    A Morsel refers to a contiguous subset of rows in the input table of the operator.
    It can independently sort the rows according to the normalized keys, aggregate columns
    and merge other morsels into itself to combine their results.
  */
  struct Morsel : public hyrise::Noncopyable {
    // We use a reference to the operator to avoid passing more fields
    // like the Table, AggregateColumns, UniqueAggregtaeColumns etc.
    const AggregateDYOD& morsel_operator;
    const uint64_t row_count;
    const uint64_t initial_row_offset;

    std::span<const uint8_t> key_bytes;
    std::span<NormalizedKey> normalized_keys;
    std::span<pmr_string> groupby_strings;

    uint64_t group_count;
    std::vector<uint64_t> group_sizes;

    Morsel(AggregateDYOD& init_morsel_operator, uint64_t init_row_count, uint64_t init_row_offset,
           std::span<const uint8_t> init_key_bytes, std::span<NormalizedKey> init_normalized_keys,
           std::span<pmr_string> init_groupby_strings)
        : morsel_operator(init_morsel_operator),
          row_count(init_row_count),
          initial_row_offset(init_row_offset),
          key_bytes(init_key_bytes),
          normalized_keys(init_normalized_keys),
          groupby_strings(init_groupby_strings) {
      aggregate_results.resize(init_morsel_operator._aggregates.size());
    }

    pmr_vector<NormalizedKey> group_keys;

    // Contains the aggregation results. Is filled by _aggregate_morsel().
    pmr_vector<std::shared_ptr<BaseAggregateResults>> aggregate_results;

    void _sort_morsel_range();
    std::weak_ordering _compare_keys(const NormalizedKey& first, const NormalizedKey& second);

    template <typename ColumnType, WindowFunction aggregate_function, typename AggregateType>
    void _aggregate_morsel(const uint64_t aggregate_index);

    // Used for the merging of another morsel.
    // If one of these indices is -1 then there is no element referring to the same group as the other,
    // non-negative index.
    struct MergeStep {
      int64_t source_index;
      int64_t other_index;
    };

    void _merge_morsel(std::shared_ptr<Morsel>& other);

    template <WindowFunction aggregate_function, typename AggregateType>
    void _merge_single_aggregate(std::shared_ptr<Morsel>& other, const uint64_t aggregate_index,
                                 const pmr_vector<MergeStep>& merge_plan);

    template <WindowFunction aggregate_function, typename AggregateType>
    std::shared_ptr<ValueSegment<AggregateType>> _to_value_segment(uint64_t aggregte_index, bool nullable);

    template <typename ColumnType>
    std::shared_ptr<ValueSegment<int64_t>> _distinct_to_value_segment(uint64_t aggregate_index);
  };

  void _normalize_chunk_groupby(const std::shared_ptr<const Chunk>& input_chunk, const ChunkID chunk_id,
                                const uint64_t row_offset, uninitialized_vector<NormalizedKey>& key_vector,
                                uninitialized_vector<uint8_t>& byte_vector, pmr_vector<pmr_string>& groupby_strings);

  void _materialize_chunk_aggregates(const std::shared_ptr<const Chunk>& input_chunk, const uint64_t row_offset);

  template <typename ColumnType, typename AggregateType, WindowFunction aggregate_function>
  std::shared_ptr<ValueSegment<AggregateType>> _aggregate_values_without_groups(uint64_t aggregate_index);

  void _on_cleanup() override;

  template <typename ColumnType, WindowFunction aggregate_function>
  void create_aggregate_column_definitions(ColumnID column_index);

  template <typename ColumnType>
  void _create_aggregate_column_definitions(boost::hana::basic_type<ColumnType> /*type*/, ColumnID column_index,
                                            WindowFunction aggregate_function);

  uint64_t _groupby_string_count = 0;
  uint64_t _normalized_key_size = 0;

  std::unordered_map<ColumnID, uint64_t> _aggregate_column_position;
  std::vector<ColumnID> _unique_aggregate_columns;
  std::vector<std::shared_ptr<BaseMaterializedColumn>> _materialized_aggregate_columns;
};
}  // namespace hyrise
