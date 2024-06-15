#include "aggregate_hash.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container/pmr/monotonic_buffer_resource.hpp>

#include "aggregate/window_function_traits.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/operator_performance_data.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/abstract_pos_list.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace {
using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * Helper to split results into chunks and prepare output vectors. Callers pass a function to consume the split results.
 * This consumer function receives iterators to the result split and is executed via the scheduler (potentially
 * concurrently). Helper is used either to process RowIDs (for GROUP BY columns) or values (for aggregation results).
 */
template <typename ColumnDataType, WindowFunction aggregate_func, typename ResultConsumer, typename ValueVectorType>
void split_results_chunk_wise(const bool write_nulls, const AggregateResults<ColumnDataType, aggregate_func>& results,
                              std::vector<ValueVectorType>& value_vectors, std::vector<pmr_vector<bool>>& null_vectors,
                              const ResultConsumer consumer_function) {
  if (results.empty()) {
    return;
  }

  auto results_begin = results.cbegin();

  const auto result_count = static_cast<ChunkID::base_type>(results.size());
  const auto output_chunk_count = static_cast<ChunkID::base_type>(
      std::ceil(static_cast<double>(result_count) / static_cast<double>(Chunk::DEFAULT_SIZE)));

  value_vectors.resize(output_chunk_count);
  if (write_nulls) {
    null_vectors.resize(output_chunk_count);
  }

  if constexpr (!std::is_same_v<ValueVectorType, std::shared_ptr<RowIDPosList>>) {
    // Check that are are dealing with expected input data, which is either pos lists (for writing the GROUP BY outputs)
    // or `pmr_vector<DataType::*>` for the aggregate results.
    using AggregateType = typename ValueVectorType::value_type;
    static_assert(std::is_same_v<ValueVectorType, pmr_vector<AggregateType>>);
  }

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(output_chunk_count);
  for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
    const auto write_split_data = [&, output_chunk_id, consumer_function]() {
      auto begin = results_begin + output_chunk_id * Chunk::DEFAULT_SIZE;
      auto end = results_begin + std::min(result_count, (output_chunk_id + 1) * Chunk::DEFAULT_SIZE);

      const auto element_count = std::distance(begin, end);
      if constexpr (std::is_same_v<ValueVectorType, std::shared_ptr<RowIDPosList>>) {
        value_vectors[output_chunk_id] = std::make_shared<RowIDPosList>();
        value_vectors[output_chunk_id]->reserve(element_count);
      } else {
        value_vectors[output_chunk_id].reserve(element_count);
      }

      if (write_nulls) {
        null_vectors[output_chunk_id].reserve(element_count);
      }

      consumer_function(begin, end, output_chunk_id);
    };

    if (output_chunk_count < 2) {
      // No reason to spawn a job and wait when there is only a single job.
      write_split_data();
    } else {
      jobs.emplace_back(std::make_shared<JobTask>(write_split_data));
    }
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);  // No-op for `output_chunk_count` < 2.
}

void prepare_output(std::vector<Segments>& output, const size_t chunk_count, const size_t column_count) {
  DebugAssert(output.empty() || output.size() == chunk_count,
              "Output data structure should be either empty or already prepared.");

  if (output.size() == chunk_count) {
    return;
  }

  while (output.size() < chunk_count) {
    output.emplace_back(column_count);
  }
}

// `get_or_add_result` is called once per row when iterating over a column that is to be aggregated. The row's `key` has
// been calculated as part of `_partition_by_groupby_keys`. We also pass in the `row_id` of that row. This row id is
// stored in `Results` so that we can later use it to reconstruct the values in the GROUP BY columns. If the operator
// calculates multiple aggregate functions, we only need to perform this lookup as part of the first aggregate function.
// By setting CacheResultIds to true_type, we can store the result of the lookup in the AggregateKey. Following
// aggregate functions can then retrieve the index from the AggregateKey.
constexpr auto CACHE_MASK = AggregateKeyEntry{1} << 63u;  // See explanation below

template <typename CacheResultIds, typename ResultIds, typename Results, typename AggregateKey>
typename Results::reference get_or_add_result(CacheResultIds /*cache_result_ids*/, ResultIds& result_ids,
                                              Results& results, AggregateKey& key, const RowID& row_id) {
  if constexpr (std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    // No GROUP BY columns are defined for this aggregate operator. We still want to keep most code paths similar and
    // avoid special handling. Thus, get_or_add_result is still called, however, we always return the same result
    // reference.
    if (results.empty()) {
      results.emplace_back();
      results[0].row_id = row_id;
    }
    return results[0];
  } else {
    // As described above, we may store the index into the results vector in the AggregateKey. If the AggregateKey
    // contains multiple entries, we use the first one. As such, we store a (non-owning, raw) pointer to either the only
    // or the first entry in first_key_entry. We need a raw pointer as a reference cannot be null or reset.
    AggregateKeyEntry* first_key_entry = nullptr;
    if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
      first_key_entry = &key;
    } else {
      first_key_entry = &key[0];
    }

    // Explanation for CACHE_MASK (placed here because it has to be defined outside but the explanation makes more sense
    // at this place):
    // If we store the result of the hashmap lookup (i.e., the index into results) in the AggregateKeyEntry, we do this
    // by storing the index in the lower 63 bits of first_key_entry and setting the most significant bit to 1 as a
    // marker that the AggregateKeyEntry now contains a cached result. We can do this because AggregateKeyEntry can not
    // become larger than the maximum size of a table (i.e., the maximum representable RowID), which is 2^31 * 2^31 ==
    // 2^62. This avoids making the AggregateKey bigger: Adding another 64-bit value (for an index of 2^62 values) for
    // the cached value would double the size of the AggregateKey in the case of a single GROUP BY column, thus halving
    // the utilization of the CPU cache. Same for a discriminating union, where the data structure alignment would also
    // result in another 8 bytes being used.
    static_assert(std::is_same_v<AggregateKeyEntry, uint64_t>,
                  "Expected AggregateKeyEntry to be unsigned 64-bit value.");

    // Check if the AggregateKey already contains a stored index.
    if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
      if (*first_key_entry & CACHE_MASK) {
        // The most significant bit is a 1, remove it by XORing the mask gives us the index into the results vector.
        const auto result_id = *first_key_entry ^ CACHE_MASK;

        // If we have not seen this index as part of the current aggregate function, the results vector may not yet have
        // the correct size. Resize it if necessary and write the current row_id so that we can recover the GroupBy
        // column(s) later. By default, the newly created values have a NULL_ROW_ID and are later ignored. We grow the
        // vector slightly more than necessary. Otherwise, monotonically increasing keys would lead to one resize per
        // row.
        if (result_id >= results.size()) {
          results.resize(static_cast<size_t>(static_cast<double>(result_id + 1) * 1.5));
        }
        results[result_id].row_id = row_id;

        return results[result_id];
      }
    } else {
      Assert(!(*first_key_entry & CACHE_MASK),
             "CacheResultIds is set to false, but a cached or immediate key shortcut entry was found.");
    }

    // Lookup the key in the result_ids map
    auto it = result_ids.find(key);
    if (it != result_ids.end()) {
      // We have already seen this group and need to return a reference to the group's result.
      const auto result_id = it->second;
      if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
        // If requested, store the index the the first_key_entry and set the most significant bit to 1.
        *first_key_entry = CACHE_MASK | result_id;
      }
      return results[result_id];
    }

    // We are seeing this group (i.e., this AggregateKey) for the first time, so we need to add it to the list of
    // results and set the row_id needed for restoring the GroupBy column(s).
    const auto result_id = results.size();
    result_ids.emplace_hint(it, key, result_id);

    results.emplace_back();
    results[result_id].row_id = row_id;

    if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
      // If requested, store the index the the first_key_entry and set the most significant bit to 1.
      *first_key_entry = CACHE_MASK | result_id;
    }

    return results[result_id];
  }
}

template <typename AggregateKey>
AggregateKey& get_aggregate_key([[maybe_unused]] KeysPerChunk<AggregateKey>& keys_per_chunk,
                                [[maybe_unused]] const ChunkID chunk_id,
                                [[maybe_unused]] const ChunkOffset chunk_offset) {
  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    auto& hash_keys = keys_per_chunk[chunk_id];

    return hash_keys[chunk_offset];
  } else {
    // We have to return a reference to something, so we create a static EmptyAggregateKey here which is used by every
    // call.
    static EmptyAggregateKey empty_aggregate_key;
    return empty_aggregate_key;
  }
}

template <typename Results>
void write_groupby_output(const std::shared_ptr<const Table>& input_table,
                          const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                          const std::vector<ColumnID>& groupby_column_ids, const Results& results,
                          TableColumnDefinitions& intermediate_result_column_definitions,
                          std::vector<Segments>& intermediate_result) {
  DebugAssert(intermediate_result.empty(), "Expected output data structure to be empty.");

  // Mapping from input to output ColumnIDs for unaggregated columns (i.e., GROUP BY columns and ANY aggregates).
  auto unaggregated_columns = std::vector<std::pair<ColumnID, ColumnID>>{};
  unaggregated_columns.reserve(groupby_column_ids.size() + aggregates.size());
  {
    auto output_column_id = ColumnID{0};
    for (const auto& input_column_id : groupby_column_ids) {
      unaggregated_columns.emplace_back(input_column_id, output_column_id);
      ++output_column_id;
    }
    for (const auto& aggregate : aggregates) {
      if (aggregate->window_function == WindowFunction::Any) {
        const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
        const auto input_column_id = pqp_column.column_id;
        unaggregated_columns.emplace_back(input_column_id, output_column_id);
      }
      ++output_column_id;
    }
  }

  // Determine type of input table. For reference tables, we need to point the RowID to the referenced table. If the
  // table is a data table, we can directly use the RowID.
  const auto input_is_data_table = (input_table->type() == TableType::Data);

  for (const auto& unaggregated_column : unaggregated_columns) {
    // Structured bindings do not work with the capture below.
    const auto input_column_id = unaggregated_column.first;
    const auto output_column_id = unaggregated_column.second;

    intermediate_result_column_definitions[output_column_id] =
        TableColumnDefinition{input_table->column_name(input_column_id), input_table->column_data_type(input_column_id),
                              input_table->column_is_nullable(input_column_id)};

    auto pos_lists = std::vector<std::shared_ptr<RowIDPosList>>{};
    auto unused_nulls = std::vector<pmr_vector<bool>>{};  // Not used for PosList writing.

    auto referenced_table = std::shared_ptr<const Table>{};
    auto referenced_column_id = input_column_id;

    // In both following loops, we skip each NULL_ROW_ID (just a marker, not literally NULL), which means that this
    // result is either a gap (in the case of an unused immediate key) or the result of overallocating the result
    // vector. As such, it must be skipped.
    if (input_is_data_table) {
      referenced_table = input_table;

      split_results_chunk_wise(false, results, pos_lists, unused_nulls,
                               [&](auto begin, const auto end, const ChunkID chunk_id) {
                                 auto& pos_list = *pos_lists[chunk_id];

                                 for (; begin != end; ++begin) {
                                   const auto& row_id = begin->row_id;
                                   if (row_id.is_null()) {
                                     continue;
                                   }
                                   pos_list.push_back(row_id);
                                 }
                               });
    } else {
      if (input_table->chunk_count() > 0) {
        // Unless we are processing an empty input, obtain the referenced table and column from the first chunk. We
        // assume that segments of the same column do not reference different tables (checked in the Table constructor).
        // When this assumption changes (e.g., due to a better support of Unions), this code needs to be revisited.
        const auto& first_reference_segment =
            static_cast<const ReferenceSegment&>(*input_table->get_chunk(ChunkID{0})->get_segment(input_column_id));
        referenced_table = first_reference_segment.referenced_table();
        referenced_column_id = first_reference_segment.referenced_column_id();
      }

      split_results_chunk_wise(
          false, results, pos_lists, unused_nulls, [&](auto begin, const auto end, const ChunkID chunk_id) {
            // Map to cache references to PosLists (avoids frequent dynamic casts to obtain position list of reference
            // segments).
            auto pos_list_mapping = boost::unordered_flat_map<ChunkID, const AbstractPosList*>{};
            auto& pos_list = *pos_lists[chunk_id];

            for (; begin != end; ++begin) {
              const auto& row_id = begin->row_id;
              if (row_id.is_null()) {
                continue;
              }

              const auto cached_poslist = pos_list_mapping.find(row_id.chunk_id);
              if (cached_poslist == pos_list_mapping.end()) {
                const auto& segment = input_table->get_chunk(row_id.chunk_id)->get_segment(input_column_id);
                DebugAssert(std::dynamic_pointer_cast<const ReferenceSegment>(segment), "Expected a ReferenceSegment.");
                const auto& reference_segment = static_cast<const ReferenceSegment&>(*segment);
                const auto& ref_segment_pos_list = *reference_segment.pos_list();

                pos_list.push_back(ref_segment_pos_list[row_id.chunk_offset]);
                pos_list_mapping.emplace(row_id.chunk_id, static_cast<const AbstractPosList*>(&ref_segment_pos_list));
              } else {
                pos_list.push_back((*cached_poslist->second)[row_id.chunk_offset]);
              }
            }
          });
    }

    // `referenced_table` is unset for empty inputs. No reason to prepare and create output.
    if (referenced_table) {
      const auto intermediate_result_chunk_count = pos_lists.size();
      prepare_output(intermediate_result, intermediate_result_chunk_count,
                     intermediate_result_column_definitions.size());
      for (auto output_chunk_id = ChunkID{0}; output_chunk_id < intermediate_result_chunk_count; ++output_chunk_id) {
        const auto& pos_list = pos_lists[output_chunk_id];
        intermediate_result[output_chunk_id][output_column_id] =
            std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, pos_list);
      }
    }
  }
}

}  // namespace

namespace hyrise {

AggregateHash::AggregateHash(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids,
                                std::make_unique<OperatorPerformanceData<OperatorSteps>>()) {
  // NOLINTNEXTLINE - clang-tidy wants _has_aggregate_functions in the member initializer list.
  _has_aggregate_functions =
      !_aggregates.empty() && !std::all_of(_aggregates.begin(), _aggregates.end(), [](const auto aggregate_expression) {
        return aggregate_expression->window_function == WindowFunction::Any;
      });
}

const std::string& AggregateHash::name() const {
  static const auto name = std::string{"AggregateHash"};
  return name;
}

std::shared_ptr<AbstractOperator> AggregateHash::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateHash>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateHash::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateHash::_on_cleanup() {
  _contexts_per_column.clear();
}

/*
Visitor context for the AggregateVisitor. The AggregateResultContext can be used without knowing the AggregateKey, the
AggregateContext is the "full" version.
*/
template <typename ColumnDataType, WindowFunction aggregate_function>
struct AggregateResultContext : SegmentVisitorContext {
  using AggregateResultAllocator = PolymorphicAllocator<AggregateResults<ColumnDataType, aggregate_function>>;

  // In cases where we know how many values to expect, we can preallocate the context in order to avoid later
  // re-allocations.
  explicit AggregateResultContext(const size_t preallocated_size = 0)
      : results(preallocated_size, AggregateResultAllocator{&buffer}) {}

  boost::container::pmr::monotonic_buffer_resource buffer;
  AggregateResults<ColumnDataType, aggregate_function> results;
};

template <typename ColumnDataType, WindowFunction aggregate_function, typename AggregateKey>
struct AggregateContext : public AggregateResultContext<ColumnDataType, aggregate_function> {
  explicit AggregateContext(const size_t preallocated_size = 0)
      : AggregateResultContext<ColumnDataType, aggregate_function>(preallocated_size) {
    auto allocator = AggregateResultIdMapAllocator<AggregateKey>{&this->buffer};

    // Unused if AggregateKey == EmptyAggregateKey, but we initialize it anyway to reduce the number of diverging code
    // paths.
    result_ids = std::make_unique<AggregateResultIdMap<AggregateKey>>(allocator);
  }

  std::unique_ptr<AggregateResultIdMap<AggregateKey>> result_ids;
};

template <typename ColumnDataType, WindowFunction aggregate_function, typename AggregateKey>
__attribute__((hot)) void AggregateHash::_aggregate_segment(ChunkID chunk_id, ColumnID column_index,
                                                            const AbstractSegment& abstract_segment,
                                                            KeysPerChunk<AggregateKey>& keys_per_chunk) {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;

  auto aggregator = WindowFunctionBuilder<ColumnDataType, AggregateType, aggregate_function>().get_aggregate_function();

  auto& context = *std::static_pointer_cast<AggregateContext<ColumnDataType, aggregate_function, AggregateKey>>(
      _contexts_per_column[column_index]);

  auto& result_ids = *context.result_ids;
  auto& results = context.results;

  auto chunk_offset = ChunkOffset{0};

  // CacheResultIds is a boolean type parameter that is forwarded to get_or_add_result, see the documentation over there
  // for details.
  const auto process_position = [&](const auto cache_result_ids, const auto& position) {
    auto& result = get_or_add_result(cache_result_ids, result_ids, results,
                                     get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                                     RowID{chunk_id, chunk_offset});

    // If the value is NULL, the current aggregate value does not change.
    if (!position.is_null()) {
      if constexpr (aggregate_function == WindowFunction::CountDistinct) {
        // For the case of CountDistinct, insert the current value into the set to keep track of distinct values.
        result.accumulator.emplace(position.value());
      } else {
        aggregator(ColumnDataType{position.value()}, result.aggregate_count, result.accumulator);
      }

      ++result.aggregate_count;
    }

    ++chunk_offset;
  };

  // Pass true_type into get_or_add_result to enable certain optimizations: If we have more than one aggregate function
  // (and thus more than one context), it makes sense to cache the results indexes, see get_or_add_result for details.
  // Furthermore, if we use the immediate key shortcut (which uses the same code path as caching), we need to pass
  // true_type so that the aggregate keys are checked for immediate access values.
  if (_contexts_per_column.size() > 1 || _use_immediate_key_shortcut) {
    segment_iterate<ColumnDataType>(abstract_segment, [&](const auto& position) {
      process_position(std::true_type{}, position);
    });
  } else {
    segment_iterate<ColumnDataType>(abstract_segment, [&](const auto& position) {
      process_position(std::false_type{}, position);
    });
  }
}

/**
 * Partition the input chunks by the given group key(s). This is done by creating a vector that contains the
 * AggregateKey for each row. It is gradually built by visitors, one for each group segment.
 */
template <typename AggregateKey>
KeysPerChunk<AggregateKey> AggregateHash::_partition_by_groupby_keys() {
  auto keys_per_chunk = KeysPerChunk<AggregateKey>{};

  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    const auto& input_table = left_input_table();
    const auto chunk_count = input_table->chunk_count();

    // Create the actual data structure
    keys_per_chunk.reserve(chunk_count);
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table->get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }

      if constexpr (std::is_same_v<AggregateKey, AggregateKeySmallVector>) {
        keys_per_chunk.emplace_back(chunk->size(), AggregateKey(_groupby_column_ids.size()));
      } else {
        keys_per_chunk.emplace_back(chunk->size(), AggregateKey{});
      }
    }

    // Now that we have the data structures in place, we can start the actual work. We want to fill
    // keys_per_chunk[chunk_id][chunk_offset] with something that uniquely identifies the group into which that
    // position belongs. There are a couple of options here (cf. AggregateHash::_on_execute):
    //
    // 0 GROUP BY columns:   No partitioning needed; we do not reach this point because of the check for
    //                       EmptyAggregateKey above
    // 1 GROUP BY column:    The AggregateKey is one dimensional, i.e., the same as AggregateKeyEntry
    // > 1 GROUP BY columns: The AggregateKey is multi-dimensional. The value in
    //                       keys_per_chunk[chunk_id][chunk_offset] is subscripted with the index of the GROUP BY
    //                       columns (not the same as the GROUP BY column_id)
    //
    // To generate a unique identifier, we create a map from the value found in the respective GROUP BY column to a
    // unique uint64_t. The value 0 is reserved for NULL.
    //
    // This has the cost of a hashmap lookup and potential insert for each row and each GROUP BY column. There are some
    // cases in which we can avoid this. These make use of the fact that we can only have 2^64 - 2*2^32 values in a
    // table (due to INVALID_VALUE_ID and INVALID_CHUNK_OFFSET limiting the range of RowIDs).
    //
    // (1) For types smaller than AggregateKeyEntry, such as int32_t, their value range can be immediately mapped into
    //     uint64_t. We cannot do the same for int64_t because we need to account for NULL values.
    // (2) For strings not longer than five characters, there are 1+2^(1*8)+2^(2*8)+2^(3*8)+2^(4*8) potential values.
    //     We can immediately map these into a numerical representation by reinterpreting their byte storage as an
    //     integer. The calculation is described below. Note that this is done on a per-string basis and does not
    //     require all strings in the given column to be that short.
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(_groupby_column_ids.size());

    const auto groupby_column_count = _groupby_column_ids.size();
    for (auto group_column_index = size_t{0}; group_column_index < groupby_column_count; ++group_column_index) {
      jobs.emplace_back(std::make_shared<JobTask>([&input_table, group_column_index, &keys_per_chunk, chunk_count,
                                                   this]() {
        const auto groupby_column_id = _groupby_column_ids.at(group_column_index);
        const auto data_type = input_table->column_data_type(groupby_column_id);

        resolve_data_type(data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          if constexpr (std::is_same_v<ColumnDataType, int32_t>) {
            // For values with a smaller type than AggregateKeyEntry, we can use the value itself as an
            // AggregateKeyEntry. We cannot do this for types with the same size as AggregateKeyEntry as we need to have
            // a special NULL value. By using the value itself, we can save us the effort of building the id_map.

            // Track the minimum and maximum key for the immediate key optimization. Search this cpp file for the last
            // use of `min_key` for a longer explanation.
            auto min_key = std::numeric_limits<AggregateKeyEntry>::max();
            auto max_key = uint64_t{0};

            for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto chunk_in = input_table->get_chunk(chunk_id);
              const auto abstract_segment = chunk_in->get_segment(groupby_column_id);
              ChunkOffset chunk_offset{0};
              auto& keys = keys_per_chunk[chunk_id];
              segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& position) {
                const auto int_to_uint = [](const int32_t value) {
                  // We need to convert a potentially negative int32_t value into the uint64_t space. We do not care
                  // about preserving the value, just its uniqueness. Subtract the minimum value in int32_t (which is
                  // negative itself) to get a positive number.
                  const auto shifted_value = static_cast<int64_t>(value) - std::numeric_limits<int32_t>::min();
                  DebugAssert(shifted_value >= 0, "Type conversion failed");
                  return static_cast<uint64_t>(shifted_value);
                };

                if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                  // Single GROUP BY column
                  if (position.is_null()) {
                    keys[chunk_offset] = 0;
                  } else {
                    const auto key = int_to_uint(position.value()) + 1;

                    keys[chunk_offset] = key;

                    min_key = std::min(min_key, key);
                    max_key = std::max(max_key, key);
                  }
                } else {
                  // Multiple GROUP BY columns
                  if (position.is_null()) {
                    keys[chunk_offset][group_column_index] = 0;
                  } else {
                    keys[chunk_offset][group_column_index] = int_to_uint(position.value()) + 1;
                  }
                }
                ++chunk_offset;
              });
            }

            if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
              // In some cases (e.g., TPC-H Q18), we aggregate with consecutive int32_t values being used as a GROUP BY
              // key. Notably, this is the case when aggregating on the serial primary key of a table without filtering
              // the table before. In these cases, we do not need to perform a full hash-based aggregation, but can use
              // the values as immediate indexes into the list of results. To handle smaller gaps, we include cases up
              // to a certain threshold, but at some point these gaps make the approach less beneficial than a proper
              // hash-based approach. Both min_key and max_key do not correspond to the original int32_t value, but are
              // the result of the int_to_uint transformation. As such, they are guaranteed to be positive. This
              // shortcut only works if we are aggregating with a single GROUP BY column (i.e., when we use
              // AggregateKeyEntry) - otherwise, we cannot establish a 1:1 mapping from keys_per_chunk to the result id.
              // TODO(anyone): Find a reasonable threshold.
              if (max_key > 0 &&
                  static_cast<double>(max_key - min_key) < static_cast<double>(input_table->row_count()) * 1.2) {
                // Include space for min, max, and NULL
                _expected_result_size = static_cast<size_t>(max_key - min_key) + 2;
                _use_immediate_key_shortcut = true;

                // Rewrite the keys and (1) subtract min so that we can also handle consecutive keys that do not start
                // at 1* and (2) set the first bit which indicates that the key is an immediate index into the result
                // vector (see get_or_add_result).
                // *) Note: Because of int_to_uint above, the values do not start at 1, anyway.

                for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
                  const auto chunk_size = input_table->get_chunk(chunk_id)->size();
                  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
                    auto& key = keys_per_chunk[chunk_id][chunk_offset];
                    if (key == 0) {
                      // Key that denotes NULL, do not rewrite but set the cached flag
                      key = key | CACHE_MASK;
                    } else {
                      key = (key - min_key + 1) | CACHE_MASK;
                    }
                  }
                }
              }
            }
          } else {
            /*
            Store unique IDs for equal values in the groupby column (similar to dictionary encoding).
            The ID 0 is reserved for NULL values. The combined IDs build an AggregateKey for each row.
            */

            // This time, we have no idea how much space we need, so we take some memory and then rely on the automatic
            // resizing. The size is quite random, but since single memory allocations do not cost too much, we rather
            // allocate a bit too much.
            auto temp_buffer = boost::container::pmr::monotonic_buffer_resource(1'000'000);
            auto allocator = PolymorphicAllocator<std::pair<const ColumnDataType, AggregateKeyEntry>>{&temp_buffer};

            auto id_map = boost::unordered_flat_map<ColumnDataType, AggregateKeyEntry, std::hash<ColumnDataType>,
                                                    std::equal_to<>, decltype(allocator)>(allocator);
            auto id_counter = AggregateKeyEntry{1};

            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              // We store strings shorter than five characters without using the id_map. For that, we need to reserve
              // the IDs used for short strings (see below).
              id_counter = 5'000'000'000;
            }

            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto chunk_in = input_table->get_chunk(chunk_id);
              if (!chunk_in) {
                continue;
              }

              auto& keys = keys_per_chunk[chunk_id];

              const auto abstract_segment = chunk_in->get_segment(groupby_column_id);
              auto chunk_offset = ChunkOffset{0};
              segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& position) {
                if (position.is_null()) {
                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys[chunk_offset] = 0u;
                  } else {
                    keys[chunk_offset][group_column_index] = 0u;
                  }
                } else {
                  // We need to generate an ID that is unique for the value. In some cases, we can use an optimization,
                  // in others, we cannot. We need to somehow track whether we have found an ID or not. For this, we
                  // first set `value_id` to its maximum value. If after all branches it is still that max value, no
                  // optimized  ID generation was applied and we need to generate the ID using the value->ID map.
                  auto value_id = std::numeric_limits<AggregateKeyEntry>::max();

                  if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
                    const auto& string = position.value();
                    if (string.size() < 5) {
                      static_assert(std::is_same_v<AggregateKeyEntry, uint64_t>, "Calculation only valid for uint64_t");

                      const auto char_to_uint = [](const char char_in, const uint32_t bits) {
                        // chars may be signed or unsigned. For the calculation as described below, we need signed
                        // chars.
                        return static_cast<uint64_t>(*reinterpret_cast<const uint8_t*>(&char_in)) << bits;
                      };

                      switch (string.size()) {
                          // Optimization for short strings (see above):
                          //
                          // NULL:              0
                          // str.length() == 0: 1
                          // str.length() == 1: 2 + (uint8_t) str            // maximum: 257 (2 + 0xff)
                          // str.length() == 2: 258 + (uint16_t) str         // maximum: 65'793 (258 + 0xffff)
                          // str.length() == 3: 65'794 + (uint24_t) str      // maximum: 16'843'009
                          // str.length() == 4: 16'843'010 + (uint32_t) str  // maximum: 4'311'810'305
                          // str.length() >= 5: map-based identifiers, starting at 5'000'000'000 for better distinction
                          //
                          // This could be extended to longer strings if the size of the input table (and thus the
                          // maximum number of distinct strings) is taken into account. For now, let's not make it even
                          // more complicated.

                        case 0: {
                          value_id = uint64_t{1};
                        } break;

                        case 1: {
                          value_id = uint64_t{2} + char_to_uint(string[0], 0);
                        } break;

                        case 2: {
                          value_id = uint64_t{258} + char_to_uint(string[1], 8) + char_to_uint(string[0], 0);
                        } break;

                        case 3: {
                          value_id = uint64_t{65'794} + char_to_uint(string[2], 16) + char_to_uint(string[1], 8) +
                                     char_to_uint(string[0], 0);
                        } break;

                        case 4: {
                          value_id = uint64_t{16'843'010} + char_to_uint(string[3], 24) + char_to_uint(string[2], 16) +
                                     char_to_uint(string[1], 8) + char_to_uint(string[0], 0);
                        } break;
                      }
                    }
                  }

                  if (value_id == std::numeric_limits<AggregateKeyEntry>::max()) {
                    // Could not take the shortcut above, either because we don't have a string or because it is too
                    // long.
                    auto inserted = id_map.try_emplace(position.value(), id_counter);

                    value_id = inserted.first->second;

                    // If the id_map did not have the value as a key and a new element was inserted.
                    if (inserted.second) {
                      ++id_counter;
                    }
                  }

                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys[chunk_offset] = value_id;
                  } else {
                    keys[chunk_offset][group_column_index] = value_id;
                  }
                }

                ++chunk_offset;
              });
            }

            // We will see at least `id_map.size()` different groups. We can use this knowledge to preallocate memory
            // for the results. Estimating the number of groups for multiple GROUP BY columns is somewhat hard, so we
            // simply take the number of groups created by the GROUP BY column with the highest number of distinct
            // values.
            auto previous_max = _expected_result_size.load();
            while (previous_max < id_map.size()) {
              // _expected_result_size needs to be atomatically updated as the GROUP BY columns are processed in
              // parallel. How to atomically update a maximum value? from https://stackoverflow.com/a/16190791/2204581
              if (_expected_result_size.compare_exchange_strong(previous_max, id_map.size())) {
                break;
              }
            }
          }
        });
      }));
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  return keys_per_chunk;
}

template <typename AggregateKey>
void AggregateHash::_aggregate() {
  const auto& input_table = left_input_table();

  if constexpr (HYRISE_DEBUG) {
    for (const auto& groupby_column_id : _groupby_column_ids) {
      Assert(groupby_column_id < input_table->column_count(), "GroupBy column index out of bounds.");
    }
  }

  // Check for invalid aggregates
  _validate_aggregates();

  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  auto timer = Timer{};

  /**
   * PARTITIONING STEP
   */
  auto keys_per_chunk = _partition_by_groupby_keys<AggregateKey>();
  step_performance_data.set_step_runtime(OperatorSteps::GroupByKeyPartitioning, timer.lap());

  /**
   * AGGREGATION STEP
   */
  _contexts_per_column = std::vector<std::shared_ptr<SegmentVisitorContext>>(_aggregates.size());

  if (!_has_aggregate_functions) {
    /*
    Insert a dummy context for the DISTINCT implementation. That way, `_contexts_per_column` will always have at least
    one context with results. This is important later on when we write the group keys into the table. The template
    parameters (int32_t, WindowFunction::Min) do not matter, as we do not calculate an aggregate anyway.
    */
    auto context =
        std::make_shared<AggregateContext<int32_t, WindowFunction::Min, AggregateKey>>(_expected_result_size);

    _contexts_per_column.push_back(context);
  }

  /**
   * Create an AggregateContext for each column in the input table that a normal (i.e. non-DISTINCT) aggregate is
   * created on. We do this here, and not in the per-chunk-loop below, because there might be no Chunks in the input
   * and _write_aggregate_output() needs these contexts anyway.
   */
  const auto aggregate_count = _aggregates.size();
  for (auto aggregate_idx = ColumnID{0}; aggregate_idx < aggregate_count; ++aggregate_idx) {
    const auto& aggregate = _aggregates[aggregate_idx];

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    if (input_column_id == INVALID_COLUMN_ID) {
      Assert(aggregate->window_function == WindowFunction::Count, "Only COUNT may have an invalid ColumnID.");
      // SELECT COUNT(*) - we know the template arguments, so we do not need a visitor.
      auto context = std::make_shared<AggregateContext<CountColumnType, WindowFunction::Count, AggregateKey>>(
          _expected_result_size);

      _contexts_per_column[aggregate_idx] = context;
      continue;
    }
    const auto data_type = input_table->column_data_type(input_column_id);
    _contexts_per_column[aggregate_idx] =
        _create_aggregate_context<AggregateKey>(data_type, aggregate->window_function);
  }

  // Process chunks and perform aggregations.
  const auto chunk_count = input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk_in = input_table->get_chunk(chunk_id);
    if (!chunk_in) {
      continue;
    }

    const auto input_chunk_size = chunk_in->size();
    if (!_has_aggregate_functions) {
      /**
       * DISTINCT implementation
       *
       * In Hyrise we handle the SQL keyword DISTINCT by using an aggregate operator with grouping but without 
       * aggregate functions. All input columns (either explicitly specified as `SELECT DISTINCT a, b, c` OR implicitly
       * as `SELECT DISTINCT *` are passed as `groupby_column_ids`).
       *
       * As the grouping happens as part of the aggregation but no aggregate function exists, we use
       * `WindowFunction::Min` as a fake aggregate function whose result will be discarded. From here on, the steps
       * are the same as they are for a regular grouped aggregate.
       */

      auto context = std::static_pointer_cast<AggregateContext<DistinctColumnType, WindowFunction::Min, AggregateKey>>(
          _contexts_per_column[0]);

      auto& result_ids = *context->result_ids;
      auto& results = context->results;

      // Add value or combination of values is added to the list of distinct value(s). This is done by calling
      // get_or_add_result, which adds the corresponding entry in the list of GROUP BY values.
      if (_use_immediate_key_shortcut) {
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk_size; ++chunk_offset) {
          // We are able to use immediate keys, so pass true_type so that the combined caching/immediate key code path
          // is enabled in get_or_add_result.
          get_or_add_result(std::true_type{}, result_ids, results,
                            get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                            RowID{chunk_id, chunk_offset});
        }
      } else {
        // Same as above, but we do not have immediate keys, so we disable that code path to reduce the complexity of
        // get_aggregate_key.
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk_size; ++chunk_offset) {
          get_or_add_result(std::false_type{}, result_ids, results,
                            get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                            RowID{chunk_id, chunk_offset});
        }
      }
    } else {
      auto aggregate_idx = ColumnID{0};
      for (const auto& aggregate : _aggregates) {
        /**
         * Special COUNT(*) implementation.
         * Because COUNT(*) does not have a specific target column, we use the maximum ColumnID. We then go through the
         * `keys_per_chunk` map and count the occurrences of each group key. The results are saved in the regular
         * `aggregate_count` variable so that we do not need a specific output logic for COUNT(*).
         */

        const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
        const auto input_column_id = pqp_column.column_id;

        if (input_column_id == INVALID_COLUMN_ID) {
          Assert(aggregate->window_function == WindowFunction::Count, "Only COUNT may have an invalid ColumnID.");
          auto context =
              std::static_pointer_cast<AggregateContext<CountColumnType, WindowFunction::Count, AggregateKey>>(
                  _contexts_per_column[aggregate_idx]);

          auto& result_ids = *context->result_ids;
          auto& results = context->results;

          if constexpr (std::is_same_v<AggregateKey, EmptyAggregateKey>) {
            // Not grouped by anything, simply count the number of rows.
            results.resize(1);
            results[0].aggregate_count += input_chunk_size;

            // We need to set any RowID because the default value (NULL_ROW_ID) would later be skipped. As we are not
            // reconstructing the GROUP BY values later, the exact value of this row_id does not matter, as long as it
            // not NULL_ROW_ID.
            results[0].row_id = RowID{ChunkID{0}, ChunkOffset{0}};
          } else {
            // Count occurrences for each group key -  If we have more than one aggregate function (and thus more than
            // one context), it makes sense to cache the results indexes, see get_or_add_result for details.
            if (_contexts_per_column.size() > 1 || _use_immediate_key_shortcut) {
              for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk_size; ++chunk_offset) {
                // Use CacheResultIds==true_type if we have more than one group by column or if the cached result ids
                // have been written by the immediate key shortcut
                auto& result =
                    get_or_add_result(std::true_type{}, result_ids, results,
                                      get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                                      RowID{chunk_id, chunk_offset});
                ++result.aggregate_count;
              }
            } else {
              for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk_size; ++chunk_offset) {
                auto& result =
                    get_or_add_result(std::false_type{}, result_ids, results,
                                      get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                                      RowID{chunk_id, chunk_offset});
                ++result.aggregate_count;
              }
            }
          }

          ++aggregate_idx;
          continue;
        }

        const auto abstract_segment = chunk_in->get_segment(input_column_id);
        const auto data_type = input_table->column_data_type(input_column_id);

        /*
        Invoke correct aggregator for each segment
        */

        resolve_data_type(data_type, [&, aggregate](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          switch (aggregate->window_function) {
            case WindowFunction::Min:
              _aggregate_segment<ColumnDataType, WindowFunction::Min, AggregateKey>(chunk_id, aggregate_idx,
                                                                                    *abstract_segment, keys_per_chunk);
              break;
            case WindowFunction::Max:
              _aggregate_segment<ColumnDataType, WindowFunction::Max, AggregateKey>(chunk_id, aggregate_idx,
                                                                                    *abstract_segment, keys_per_chunk);
              break;
            case WindowFunction::Sum:
              _aggregate_segment<ColumnDataType, WindowFunction::Sum, AggregateKey>(chunk_id, aggregate_idx,
                                                                                    *abstract_segment, keys_per_chunk);
              break;
            case WindowFunction::Avg:
              _aggregate_segment<ColumnDataType, WindowFunction::Avg, AggregateKey>(chunk_id, aggregate_idx,
                                                                                    *abstract_segment, keys_per_chunk);
              break;
            case WindowFunction::Count:
              _aggregate_segment<ColumnDataType, WindowFunction::Count, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case WindowFunction::CountDistinct:
              _aggregate_segment<ColumnDataType, WindowFunction::CountDistinct, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case WindowFunction::StandardDeviationSample:
              _aggregate_segment<ColumnDataType, WindowFunction::StandardDeviationSample, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case WindowFunction::Any:
              // ANY is a pseudo-function and is handled by `write_groupby_output`.
              break;
            case WindowFunction::CumeDist:
            case WindowFunction::DenseRank:
            case WindowFunction::PercentRank:
            case WindowFunction::Rank:
            case WindowFunction::RowNumber:
              Fail("Unsupported aggregate function " + window_function_to_string.left.at(aggregate->window_function) +
                   ".");
          }
        });

        ++aggregate_idx;
      }
    }
  }
  step_performance_data.set_step_runtime(OperatorSteps::Aggregating, timer.lap());
}  // NOLINT(readability/fn_size)

std::shared_ptr<const Table> AggregateHash::_on_execute() {
  // We do not want the overhead of a vector with heap storage when we have a limited number of aggregate columns.
  // However, more specializations mean more compile time. We now have specializations for 0, 1, 2, and >2 GROUP BY
  // columns.
  switch (_groupby_column_ids.size()) {
    case 0:
      _aggregate<EmptyAggregateKey>();
      break;
    case 1:
      // No need for a complex data structure if we only have one entry.
      _aggregate<AggregateKeyEntry>();
      break;
    case 2:
      _aggregate<std::array<AggregateKeyEntry, 2>>();
      break;
    default:
      _aggregate<AggregateKeySmallVector>();
      break;
  }

  const auto num_output_columns = _groupby_column_ids.size() + _aggregates.size();
  _output_column_definitions.resize(num_output_columns);

  /**
   * If only GROUP BY columns (including ANY pseudo-aggregates) are written, we need to call `write_groupby_output`.
   *   Example: SELECT c_custkey, c_name FROM customer GROUP BY c_custkey, c_name (same as SELECT DISTINCT), which
   *            is rewritten to group only on c_custkey and collect c_name as an ANY pseudo-aggregate.
   * Otherwise, it is called by the first call to `_write_aggregate_output`.
   **/
  if (!_has_aggregate_functions) {
    auto context = std::static_pointer_cast<AggregateResultContext<DistinctColumnType, WindowFunction::Min>>(
        _contexts_per_column[0]);
    auto groupby_columns_writing_timer = Timer{};
    write_groupby_output(left_input_table(), _aggregates, _groupby_column_ids, context->results,
                         _output_column_definitions, _intermediate_result);
    DebugAssert(groupby_columns_writing_duration == std::chrono::nanoseconds{0},
                "groupby_columns_writing_duration() was apparently called more than once.");
    groupby_columns_writing_duration = groupby_columns_writing_timer.lap();
  }

  /*
  Write the aggregated columns to the output
  */
  const auto& input_table = left_input_table();
  auto aggregate_idx = ColumnID{0};
  for (const auto& aggregate : _aggregates) {
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    // Output column for COUNT(*).
    const auto data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      switch (aggregate->window_function) {
        case WindowFunction::Min:
          _write_aggregate_output<ColumnDataType, WindowFunction::Min>(aggregate_idx);
          break;
        case WindowFunction::Max:
          _write_aggregate_output<ColumnDataType, WindowFunction::Max>(aggregate_idx);
          break;
        case WindowFunction::Sum:
          _write_aggregate_output<ColumnDataType, WindowFunction::Sum>(aggregate_idx);
          break;
        case WindowFunction::Avg:
          _write_aggregate_output<ColumnDataType, WindowFunction::Avg>(aggregate_idx);
          break;
        case WindowFunction::Count:
          _write_aggregate_output<ColumnDataType, WindowFunction::Count>(aggregate_idx);
          break;
        case WindowFunction::CountDistinct:
          _write_aggregate_output<ColumnDataType, WindowFunction::CountDistinct>(aggregate_idx);
          break;
        case WindowFunction::StandardDeviationSample:
          _write_aggregate_output<ColumnDataType, WindowFunction::StandardDeviationSample>(aggregate_idx);
          break;
        case WindowFunction::Any:
          // Pseudo-aggregates are written by write_groupby_output.
          break;
        case WindowFunction::CumeDist:
        case WindowFunction::DenseRank:
        case WindowFunction::PercentRank:
        case WindowFunction::Rank:
        case WindowFunction::RowNumber:
          Fail("Unsupported aggregate function " + window_function_to_string.left.at(aggregate->window_function) + ".");
      }
    });

    ++aggregate_idx;
  }

  /**
   * Write the output.
   *
   * At this point, we collected the GROUP BY columns as reference segments, which are split using the default chunk
   * size (minus gap rows, see comments on NULL_ID). Similarly, the aggregate values are split into chunks. We create a
   * temporary table of reference segments (temporary as life time is set by `shared_ptr` via
   * `ReferenceSegment::_referenced_table`). This temporary table stores reference segments for the GROUP BY columns and
   * reference segments to materialized columns (of the temporary table, using `EntireChunkPosList`) for the aggregate
   * columns.
  */
  auto timer = Timer{};

  auto reference_segment_indexes = std::vector<ColumnID>(_groupby_column_ids.size());
  auto entireposlist_indexes = std::vector<ColumnID>{};
  entireposlist_indexes.reserve(_aggregates.size());

  std::iota(reference_segment_indexes.begin(), reference_segment_indexes.end(), ColumnID{0});
  auto output_column_id = ColumnID{static_cast<ColumnID::base_type>(_groupby_column_ids.size())};
  for (const auto& aggregate : _aggregates) {
    if (aggregate->window_function == WindowFunction::Any) {
      reference_segment_indexes.push_back(output_column_id);
    } else {
      entireposlist_indexes.push_back(output_column_id);
    }
    ++output_column_id;
  }

  // Create temporary table storing materialized columns. The operator output references this table's columns via
  // `EntireChunkPosList` reference segments.
  auto aggregate_columns_result_table = std::shared_ptr<Table>{};
  if (!entireposlist_indexes.empty()) {
    const auto materialized_column_count = entireposlist_indexes.size();
    auto aggregate_column_definitions = std::vector<TableColumnDefinition>{};
    aggregate_column_definitions.reserve(materialized_column_count);

    for (const auto entireposlist_index : entireposlist_indexes) {
      aggregate_column_definitions.emplace_back(_output_column_definitions[entireposlist_index]);
    }

    aggregate_columns_result_table = std::make_shared<Table>(aggregate_column_definitions, TableType::Data);
    for (const auto& materialized_result_chunk : _intermediate_result) {
      auto aggregate_segments = Segments{};
      aggregate_segments.reserve(materialized_column_count);

      for (const auto entireposlist_index : entireposlist_indexes) {
        aggregate_segments.emplace_back(materialized_result_chunk[entireposlist_index]);
      }

      aggregate_columns_result_table->append_chunk(aggregate_segments);
    }
  }

  // Create final operator output. We now combine actual reference segments (e.g., of GROUP BY columns) with segments
  // that reference the temporary materialized table created above.
  auto operator_output = std::make_shared<Table>(_output_column_definitions, TableType::References);
  if (!_intermediate_result.empty() && _intermediate_result.front()[0]->size() > 0) {
    const auto output_table_chunk_count = _intermediate_result.size();
    for (auto chunk_id = ChunkID{0}; chunk_id < output_table_chunk_count; ++chunk_id) {
      if (!_intermediate_result[chunk_id][0]) {
        // When vectors have been oversized (see get_or_add_result()), intermediate chunks might be completely empty.
        continue;
      }

      auto reference_segments = Segments(num_output_columns);

      for (const auto column_id : reference_segment_indexes) {
        DebugAssert(std::dynamic_pointer_cast<const ReferenceSegment>(_intermediate_result[chunk_id][column_id]),
                    "Expected a ReferenceSegment at this position.");
        reference_segments[column_id] = _intermediate_result[chunk_id][column_id];
      }

      const auto materialized_table_column_count = entireposlist_indexes.size();
      const auto chunk_size = _intermediate_result[chunk_id][0]->size();
      Assert(!_groupby_column_ids.empty() || materialized_table_column_count > 0,
             "Output does not contain any columns.");
      for (auto materialized_table_column_id = ColumnID{0};
           materialized_table_column_id < materialized_table_column_count; ++materialized_table_column_id) {
        DebugAssert(!std::dynamic_pointer_cast<const ReferenceSegment>(
                        aggregate_columns_result_table->get_chunk(chunk_id)->get_segment(
                            ColumnID{materialized_table_column_id})),
                    "Unexpected reference segment at this position.");
        const auto entire_chunk_pos_list = std::make_shared<EntireChunkPosList>(chunk_id, chunk_size);
        reference_segments[entireposlist_indexes[materialized_table_column_id]] = std::make_shared<ReferenceSegment>(
            aggregate_columns_result_table, materialized_table_column_id, entire_chunk_pos_list);
      }
      operator_output->append_chunk(reference_segments);
    }
  }

  // _aggregate has its own internal timer. As groupby/aggregate column writing can be interleaved, the runtime is
  // stored in members and later written to the operator performance data struct.
  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  step_performance_data.set_step_runtime(OperatorSteps::OutputWriting, timer.lap());

  step_performance_data.set_step_runtime(OperatorSteps::GroupByColumnsWriting, groupby_columns_writing_duration);
  step_performance_data.set_step_runtime(OperatorSteps::AggregateColumnsWriting, aggregate_columns_writing_duration);

  return operator_output;
}

/*
The following template functions write the aggregated values for the different aggregate functions.
They are separate and templated to avoid compiler errors for invalid type/function combinations.
*/
// MIN, MAX, SUM, ANY write the current aggregated value.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
std::enable_if_t<aggregate_func == WindowFunction::Min || aggregate_func == WindowFunction::Max ||
                     aggregate_func == WindowFunction::Sum || aggregate_func == WindowFunction::Any,
                 bool>
write_aggregate_values(const AggregateResults<ColumnDataType, aggregate_func>& results,
                       std::vector<pmr_vector<AggregateType>>& value_vectors,
                       std::vector<pmr_vector<bool>>& null_vectors) {
  auto null_written = std::atomic<bool>{};
  split_results_chunk_wise(
      true, results, value_vectors, null_vectors, [&](auto begin, const auto end, const ChunkID chunk_id) {
        auto& values = value_vectors[chunk_id];
        auto& null_values = null_vectors[chunk_id];

        for (; begin != end; ++begin) {
          const auto& result = *begin;

          // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either a gap (in the case of an
          // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
          if (result.row_id.is_null()) {
            continue;
          }

          if (result.aggregate_count > 0) {
            values.emplace_back(result.accumulator);
            null_values.emplace_back(false);
          } else {
            values.emplace_back();
            null_values.emplace_back(true);
            null_written = true;
          }
        }
      });
  return null_written;
}

// COUNT writes the aggregate counter.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
std::enable_if_t<aggregate_func == WindowFunction::Count, bool> write_aggregate_values(
    const AggregateResults<ColumnDataType, aggregate_func>& results,
    std::vector<pmr_vector<AggregateType>>& value_vectors, std::vector<pmr_vector<bool>>& null_vectors) {
  split_results_chunk_wise(
      false, results, value_vectors, null_vectors, [&](auto begin, const auto end, const ChunkID chunk_id) {
        auto& values = value_vectors[chunk_id];

        for (; begin != end; ++begin) {
          const auto& result = *begin;

          // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either a gap (in the case of an
          // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
          if (result.row_id.is_null()) {
            continue;
          }

          values.emplace_back(result.aggregate_count);
        }
      });
  return false;
}

// COUNT(DISTINCT) writes the number of distinct values.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
std::enable_if_t<aggregate_func == WindowFunction::CountDistinct, bool> write_aggregate_values(
    const AggregateResults<ColumnDataType, aggregate_func>& results,
    std::vector<pmr_vector<AggregateType>>& value_vectors, std::vector<pmr_vector<bool>>& null_vectors) {
  split_results_chunk_wise(
      false, results, value_vectors, null_vectors, [&](auto begin, const auto end, const ChunkID chunk_id) {
        auto& values = value_vectors[chunk_id];

        for (; begin != end; ++begin) {
          const auto& result = *begin;

          // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either a gap (in the case of an
          // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
          if (result.row_id.is_null()) {
            continue;
          }

          values.emplace_back(result.accumulator.size());
        }
      });
  return false;
}

// AVG writes the calculated average from current aggregate and the aggregate counter.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
std::enable_if_t<aggregate_func == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>, bool>
write_aggregate_values(const AggregateResults<ColumnDataType, aggregate_func>& results,
                       std::vector<pmr_vector<AggregateType>>& value_vectors,
                       std::vector<pmr_vector<bool>>& null_vectors) {
  auto null_written = std::atomic<bool>{};
  split_results_chunk_wise(
      true, results, value_vectors, null_vectors, [&](auto begin, const auto end, const ChunkID chunk_id) {
        auto& values = value_vectors[chunk_id];
        auto& null_values = null_vectors[chunk_id];

        for (; begin != end; ++begin) {
          const auto& result = *begin;

          // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either a gap (in the case of an
          // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
          if (result.row_id.is_null()) {
            continue;
          }

          if (result.aggregate_count > 0) {
            values.emplace_back(result.accumulator / static_cast<AggregateType>(result.aggregate_count));
            null_values.emplace_back(false);
          } else {
            values.emplace_back();
            null_values.emplace_back(true);
            null_written = true;
          }
        }
      });
  return null_written;
}

// AVG is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
std::enable_if_t<aggregate_func == WindowFunction::Avg && !std::is_arithmetic_v<AggregateType>, bool>
write_aggregate_values(const AggregateResults<ColumnDataType, aggregate_func>& /*results*/,
                       std::vector<pmr_vector<AggregateType>>& /* values */,
                       std::vector<pmr_vector<bool>>& /* null_vectors */) {
  Fail("Invalid aggregate.");
}

// STDDEV_SAMP writes the calculated standard deviation from current aggregate and the aggregate counter.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
std::enable_if_t<aggregate_func == WindowFunction::StandardDeviationSample && std::is_arithmetic_v<AggregateType>, bool>
write_aggregate_values(const AggregateResults<ColumnDataType, aggregate_func>& results,
                       std::vector<pmr_vector<AggregateType>>& value_vectors,
                       std::vector<pmr_vector<bool>>& null_vectors) {
  auto null_written = std::atomic<bool>{};
  split_results_chunk_wise(
      true, results, value_vectors, null_vectors, [&](auto begin, const auto end, const ChunkID chunk_id) {
        auto& values = value_vectors[chunk_id];
        auto& null_values = null_vectors[chunk_id];

        for (; begin != end; ++begin) {
          const auto& result = *begin;

          // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either a gap (in the case of an
          // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
          if (result.row_id.is_null()) {
            continue;
          }

          if (result.aggregate_count > 1) {
            values.emplace_back(result.accumulator[3]);
            null_values.emplace_back(false);
          } else {
            // STDDEV_SAMP is undefined for lists with less than two elements.
            values.emplace_back();
            null_values.emplace_back(true);
            null_written = true;
          }
        }
      });
  return null_written;
}

// STDDEV_SAMP is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
std::enable_if_t<aggregate_func == WindowFunction::StandardDeviationSample && !std::is_arithmetic_v<AggregateType>,
                 bool>
write_aggregate_values(const AggregateResults<ColumnDataType, aggregate_func>& /*results*/,
                       std::vector<pmr_vector<AggregateType>>& /* values */,
                       std::vector<pmr_vector<bool>>& /* null_vectors */) {
  Fail("Invalid aggregate.");
}

template <typename ColumnDataType, WindowFunction aggregate_function>
void AggregateHash::_write_aggregate_output(ColumnID aggregate_index) {
  // Used to track the duration of groupby columns writing, which is done for the first aggregate column only. Value is
  // subtracted from the runtime of this method (thus, it is either non-zero for the first aggregate column or zero for
  // the remaining columns).
  auto excluded_time = std::chrono::nanoseconds{};
  auto timer = Timer{};

  // Retrieve type information from the aggregation traits.
  using aggregate_type = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  auto result_type = WindowFunctionTraits<ColumnDataType, aggregate_function>::RESULT_TYPE;

  const auto& aggregate = _aggregates[aggregate_index];

  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  const auto input_column_id = pqp_column.column_id;

  if (result_type == DataType::Null) {
    // If not specified, it is the input column’s type.
    result_type = left_input_table()->column_data_type(input_column_id);
  }

  auto context = std::static_pointer_cast<AggregateResultContext<ColumnDataType, aggregate_function>>(
      _contexts_per_column[aggregate_index]);

  const auto& results = context->results;

  // Before writing the first aggregate column, write all group keys into the respective columns.
  if (aggregate_index == 0) {
    auto groupby_columns_writing_timer = Timer{};
    write_groupby_output(left_input_table(), _aggregates, _groupby_column_ids, results, _output_column_definitions,
                         _intermediate_result);
    const auto groupby_columns_writing_runtime = groupby_columns_writing_timer.lap();
    DebugAssert(groupby_columns_writing_duration == std::chrono::nanoseconds{0},
                "groupby_columns_writing_duration() was apparently called more than once.");
    groupby_columns_writing_duration = groupby_columns_writing_runtime;
    excluded_time = groupby_columns_writing_runtime;
  }

  constexpr auto NEEDS_NULL =
      (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct);
  const auto output_column_id = _groupby_column_ids.size() + aggregate_index;

  auto value_vectors = std::vector<pmr_vector<aggregate_type>>{};
  auto null_vectors = std::vector<pmr_vector<bool>>{};
  auto aggregate_result_contains_nulls =
      write_aggregate_values<ColumnDataType, aggregate_type, aggregate_function>(results, value_vectors, null_vectors);

  if (_groupby_column_ids.empty() && value_vectors.empty()) {
    // If we did not GROUP BY anything and we have no results, we need to add NULL for most aggregates and 0 for count.
    value_vectors.emplace_back();
    value_vectors[0].emplace_back();
    if constexpr (NEEDS_NULL) {
      Assert(null_vectors.empty(), "Unexpected non-empty state of NULL values.");
      null_vectors.emplace_back();
      null_vectors[0].emplace_back(true);
      aggregate_result_contains_nulls = true;
    }
  }

  DebugAssert(NEEDS_NULL || null_vectors.empty(), "write_aggregate_values unexpectedly wrote NULL values.");

  prepare_output(_intermediate_result, value_vectors.size(), _output_column_definitions.size());

  _output_column_definitions[output_column_id] =
      TableColumnDefinition{aggregate->as_column_name(), result_type, NEEDS_NULL};

  const auto materialized_segment_count = value_vectors.size();
  for (auto segment_id = ChunkID{0}; segment_id < materialized_segment_count; ++segment_id) {
    auto output_segment = std::shared_ptr<ValueSegment<aggregate_type>>{};
    if (!NEEDS_NULL || !aggregate_result_contains_nulls) {
      output_segment = std::make_shared<ValueSegment<aggregate_type>>(std::move(value_vectors[segment_id]));
    } else {
      DebugAssert(value_vectors[segment_id].size() == null_vectors[segment_id].size(),
                  "Sizes of value and NULL vectors differ.");
      output_segment = std::make_shared<ValueSegment<aggregate_type>>(std::move(value_vectors[segment_id]),
                                                                      std::move(null_vectors[segment_id]));
    }
    _intermediate_result[segment_id][output_column_id] = output_segment;
  }

  aggregate_columns_writing_duration += timer.lap() - excluded_time;
}

template <typename AggregateKey>
std::shared_ptr<SegmentVisitorContext> AggregateHash::_create_aggregate_context(
    const DataType data_type, const WindowFunction aggregate_function) const {
  std::shared_ptr<SegmentVisitorContext> context;
  resolve_data_type(data_type, [&](auto type) {
    const auto size = _expected_result_size.load();
    using ColumnDataType = typename decltype(type)::type;
    switch (aggregate_function) {
      case WindowFunction::Min:
        context = std::make_shared<AggregateContext<ColumnDataType, WindowFunction::Min, AggregateKey>>(size);
        break;
      case WindowFunction::Max:
        context = std::make_shared<AggregateContext<ColumnDataType, WindowFunction::Max, AggregateKey>>(size);
        break;
      case WindowFunction::Sum:
        context = std::make_shared<AggregateContext<ColumnDataType, WindowFunction::Sum, AggregateKey>>(size);
        break;
      case WindowFunction::Avg:
        context = std::make_shared<AggregateContext<ColumnDataType, WindowFunction::Avg, AggregateKey>>(size);
        break;
      case WindowFunction::Count:
        context = std::make_shared<AggregateContext<ColumnDataType, WindowFunction::Count, AggregateKey>>(size);
        break;
      case WindowFunction::CountDistinct:
        context = std::make_shared<AggregateContext<ColumnDataType, WindowFunction::CountDistinct, AggregateKey>>(size);
        break;
      case WindowFunction::StandardDeviationSample:
        context =
            std::make_shared<AggregateContext<ColumnDataType, WindowFunction::StandardDeviationSample, AggregateKey>>(
                size);
        break;
      case WindowFunction::Any:
        context = std::make_shared<AggregateContext<ColumnDataType, WindowFunction::Any, AggregateKey>>(size);
        break;
      case WindowFunction::CumeDist:
      case WindowFunction::DenseRank:
      case WindowFunction::PercentRank:
      case WindowFunction::Rank:
      case WindowFunction::RowNumber:
        Fail("Unsupported aggregate function '" + window_function_to_string.left.at(aggregate_function) + "'.");
    }
  });

  return context;
}

}  // namespace hyrise
