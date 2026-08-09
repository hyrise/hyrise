#include "aggregate_dyod.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "aggregate/window_function_traits.hpp"
#include "aggregate_hash.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/dyod_window_function_builder.hpp"
#include "operators/operator_performance_data.hpp"
#include "operators/print.hpp"
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
#include "table_scan.hpp"
#include "table_wrapper.hpp"
#include "type_comparison.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace {
using namespace hyrise;
using namespace hyrise::expression_functional;

// splitmix64 hash mix finalizer
// Steele, G. L., Lea, D., & Flood, C. H. (2014). Fast splittable pseudorandom number generators.
// ACM SIGPLAN Notices, 49(10), 453–472. https://doi.org/10.1145/2714064.2660195
inline size_t hash_mix(size_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  x = x ^ (x >> 31);
  return x;
}

/**
 * The following template functions write the aggregated values for the different aggregate functions. They are separate
 * and templated to avoid compiler errors for invalid type/function combinations.
 */

// MIN, MAX, SUM, ANY write the current aggregated value.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
  requires(aggregate_func == WindowFunction::Min || aggregate_func == WindowFunction::Max ||
           aggregate_func == WindowFunction::Sum || aggregate_func == WindowFunction::Any)
bool dyod_write_aggregate_values(const DYODAggregateResults<ColumnDataType, aggregate_func>& results,
                                 std::vector<pmr_vector<AggregateType>>& value_vectors,
                                 std::vector<pmr_vector<bool>>& null_vectors) {
  auto null_written = std::atomic<bool>{};
  dyod_split_results_chunk_wise(true, results, value_vectors, null_vectors,
                                [&](auto begin, const auto end, const ChunkID chunk_id) {
                                  auto& values = value_vectors[chunk_id];
                                  auto& null_values = null_vectors[chunk_id];

                                  for (; begin != end; ++begin) {
                                    const auto& result = *begin;

                                    // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either
                                    // a gap (in the case of an unused immediate key) or the result of overallocating
                                    // the result vector. As such, it must be skipped.
                                    if (result.row_id.is_null()) {
                                      continue;
                                    }

                                    if (result.has_aggregates) {
                                      values.emplace_back(result.accumulator);
                                      null_values.emplace_back(false);
                                    } else {
                                      values.emplace_back();
                                      null_values.emplace_back(true);
                                      null_written.store(true, std::memory_order_relaxed);
                                    }
                                  }
                                });
  return null_written;
}

// COUNT writes the aggregate counter.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
  requires(aggregate_func == WindowFunction::Count)
bool dyod_write_aggregate_values(const DYODAggregateResults<ColumnDataType, aggregate_func>& results,
                                 std::vector<pmr_vector<AggregateType>>& value_vectors,
                                 std::vector<pmr_vector<bool>>& null_vectors) {
  dyod_split_results_chunk_wise(false, results, value_vectors, null_vectors,
                                [&](auto begin, const auto end, const ChunkID chunk_id) {
                                  auto& values = value_vectors[chunk_id];

                                  for (; begin != end; ++begin) {
                                    const auto& result = *begin;

                                    // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either
                                    // a gap (in the case of an unused immediate key) or the result of overallocating
                                    // the result vector. As such, it must be skipped.
                                    if (result.row_id.is_null()) {
                                      continue;
                                    }

                                    values.emplace_back(result.accumulator);
                                  }
                                });
  return false;
}

// COUNT(DISTINCT) writes the number of distinct values.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
  requires(aggregate_func == WindowFunction::CountDistinct)
bool dyod_write_aggregate_values(const DYODAggregateResults<ColumnDataType, aggregate_func>& results,
                                 std::vector<pmr_vector<AggregateType>>& value_vectors,
                                 std::vector<pmr_vector<bool>>& null_vectors) {
  dyod_split_results_chunk_wise(false, results, value_vectors, null_vectors,
                                [&](auto begin, const auto end, const ChunkID chunk_id) {
                                  auto& values = value_vectors[chunk_id];

                                  for (; begin != end; ++begin) {
                                    const auto& result = *begin;

                                    // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either
                                    // a gap (in the case of an unused immediate key) or the result of overallocating
                                    // the result vector. As such, it must be skipped.
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
  requires(aggregate_func == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>)
bool dyod_write_aggregate_values(const DYODAggregateResults<ColumnDataType, aggregate_func>& results,
                                 std::vector<pmr_vector<AggregateType>>& value_vectors,
                                 std::vector<pmr_vector<bool>>& null_vectors) {
  auto null_written = std::atomic<bool>{};
  dyod_split_results_chunk_wise(
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

          if (result.has_aggregates) {
            values.emplace_back(result.accumulator.first / static_cast<AggregateType>(result.accumulator.second));
            null_values.emplace_back(false);
          } else {
            values.emplace_back();
            null_values.emplace_back(true);
            null_written.store(true, std::memory_order_relaxed);
          }
        }
      });
  return null_written;
}

// AVG is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
  requires(aggregate_func == WindowFunction::Avg && !std::is_arithmetic_v<AggregateType>)
bool dyod_write_aggregate_values(const DYODAggregateResults<ColumnDataType, aggregate_func>& /*results*/,
                                 std::vector<pmr_vector<AggregateType>>& /* values */,
                                 std::vector<pmr_vector<bool>>& /* null_vectors */) {
  Fail("Invalid aggregate.");
}

// STDDEV_SAMP writes the calculated standard deviation from current aggregate and the aggregate counter.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
  requires(aggregate_func == WindowFunction::StandardDeviationSample && std::is_arithmetic_v<AggregateType>)
bool dyod_write_aggregate_values(const DYODAggregateResults<ColumnDataType, aggregate_func>& results,
                                 std::vector<pmr_vector<AggregateType>>& value_vectors,
                                 std::vector<pmr_vector<bool>>& null_vectors) {
  auto null_written = std::atomic<bool>{};
  dyod_split_results_chunk_wise(true, results, value_vectors, null_vectors,
                                [&](auto begin, const auto end, const ChunkID chunk_id) {
                                  auto& values = value_vectors[chunk_id];
                                  auto& null_values = null_vectors[chunk_id];

                                  for (; begin != end; ++begin) {
                                    const auto& result = *begin;

                                    // NULL_ROW_ID (just a marker, not literally NULL) means that this result is either
                                    // a gap (in the case of an unused immediate key) or the result of overallocating
                                    // the result vector. As such, it must be skipped.
                                    if (result.row_id.is_null()) {
                                      continue;
                                    }

                                    // We have the count at index 0
                                    if (result.accumulator[0] > 1) {
                                      values.emplace_back(result.accumulator[3]);
                                      null_values.emplace_back(false);
                                    } else {
                                      // STDDEV_SAMP is undefined for lists with less than two elements.
                                      values.emplace_back();
                                      null_values.emplace_back(true);
                                      null_written.store(true, std::memory_order_relaxed);
                                    }
                                  }
                                });
  return null_written;
}

// STDDEV_SAMP is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnDataType, typename AggregateType, WindowFunction aggregate_func>
  requires(aggregate_func == WindowFunction::StandardDeviationSample && !std::is_arithmetic_v<AggregateType>)
bool dyod_write_aggregate_values(const DYODAggregateResults<ColumnDataType, aggregate_func>& /*results*/,
                                 std::vector<pmr_vector<AggregateType>>& /* values */,
                                 std::vector<pmr_vector<bool>>& /* null_vectors */) {
  Fail("Invalid aggregate.");
}

/**
 * Helper to split results into chunks and prepare output vectors. Callers pass a function to consume the split results.
 * This consumer function receives iterators to the result split and is executed via the scheduler (potentially
 * concurrently). Helper is used either to process RowIDs (for GROUP BY columns) or values (for aggregation results).
 */
template <typename ColumnDataType, WindowFunction aggregate_func, typename ResultConsumer, typename ValueVectorType>
void dyod_split_results_chunk_wise(const bool write_nulls,
                                   const DYODAggregateResults<ColumnDataType, aggregate_func>& results,
                                   std::vector<ValueVectorType>& value_vectors,
                                   std::vector<pmr_vector<bool>>& null_vectors,
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
      auto begin = results_begin + (output_chunk_id * Chunk::DEFAULT_SIZE);
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

void dyod_prepare_output(std::vector<Segments>& output, const size_t chunk_count, const size_t column_count) {
  DebugAssert(output.empty() || output.size() == chunk_count,
              "Output data structure should be either empty or already prepared.");

  if (output.size() == chunk_count) {
    return;
  }

  while (output.size() < chunk_count) {
    output.emplace_back(column_count);
  }
}

template <typename Functor>
void resolve_window_function_without_any(WindowFunction window_function, const Functor& functor) {
  switch (window_function) {
    case WindowFunction::Min:
      functor.template operator()<WindowFunction::Min>();
      break;
    case WindowFunction::Max:
      functor.template operator()<WindowFunction::Max>();
      break;
    case WindowFunction::Sum:
      functor.template operator()<WindowFunction::Sum>();
      break;
    case WindowFunction::Avg:
      functor.template operator()<WindowFunction::Avg>();
      break;
    case WindowFunction::Count:
      functor.template operator()<WindowFunction::Count>();
      break;
    case WindowFunction::CountDistinct:
      functor.template operator()<WindowFunction::CountDistinct>();
      break;
    case WindowFunction::StandardDeviationSample:
      functor.template operator()<WindowFunction::StandardDeviationSample>();
      break;
    case WindowFunction::Any:
      break;
    case WindowFunction::CumeDist:
    case WindowFunction::DenseRank:
    case WindowFunction::PercentRank:
    case WindowFunction::Rank:
    case WindowFunction::RowNumber:
      Fail(std::format("Unsupported aggregate function '{}'.", window_function_to_string.left.at(window_function)));
  }
}

template <typename Functor>
void resolve_window_function(WindowFunction window_function, const Functor& functor) {
  if (window_function == WindowFunction::Any) {
    functor.template operator()<WindowFunction::Any>();
  } else {
    resolve_window_function_without_any(window_function, functor);
  }
}

// `visit_and_get_result` is called once per row when iterating over a column that is to be aggregated. The row's `key`
// has been calculated as part of `_partition_by_groupby_keys`. We also pass in the `row_id` of that row. This row id
// is stored in `Results` so that we can later use it to reconstruct the values in the GROUP BY columns. If the operator
// calculates multiple aggregate functions, we only need to perform this lookup as part of the first aggregate function.
// By setting CacheResultIds to true_type, we can store the result of the lookup in the AggregateKey. Following
// aggregate functions can then retrieve the index from the AggregateKey.
constexpr auto DYOD_CACHE_MASK = DYODAggregateKeyEntry{1} << uint8_t{63};  // See explanation below

template <typename CacheResultIds, typename ResultIds, typename Results, typename AggregateKey>
typename Results::reference visit_and_get_result(CacheResultIds /*cache_result_ids*/, ResultIds& result_ids,
                                                 Results& results, AggregateKey& key, const RowID& row_id) {
  if constexpr (std::is_same_v<AggregateKey, DYODEmptyAggregateKey>) {
    // No GROUP BY columns are defined for this aggregate operator. We still want to keep most code paths similar and
    // avoid special handling. Thus, visit_and_get_result is still called, however, we always return the same result
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
    DYODAggregateKeyEntry* first_key_entry = nullptr;
    if constexpr (std::is_same_v<AggregateKey, DYODAggregateKeyEntry>) {
      first_key_entry = &key;
    } else {
      first_key_entry = &key[0];
    }

    // Explanation for DYOD_CACHE_MASK (placed here because it has to be defined outside but the explanation makes more
    // sense at this place):
    // If we store the result of the hashmap lookup (i.e., the index into results) in the DYODAggregateKeyEntry, we do
    // this by storing the index in the lower 63 bits of first_key_entry and setting the most significant bit to 1 as a
    // marker that the DYODAggregateKeyEntry now contains a cached result. We can do this because DYODAggregateKeyEntry
    // can not become larger than the maximum size of a table (i.e., the maximum representable RowID), which is
    // 2^31 * 2^31 == 2^62.
    // This avoids making the AggregateKey bigger: Adding another 64-bit value (for an index of 2^62 values) for
    // the cached value would double the size of the AggregateKey in the case of a single GROUP BY column, thus halving
    // the utilization of the CPU cache. Same for a discriminating union, where the data structure alignment would also
    // result in another 8 bytes being used.
    static_assert(std::is_same_v<DYODAggregateKeyEntry, uint64_t>,
                  "Expected DYODAggregateKeyEntry to be unsigned 64-bit value.");

    // Check if the AggregateKey already contains a stored index.
    if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
      if (*first_key_entry & DYOD_CACHE_MASK) {
        // The most significant bit is a 1, remove it by XORing the mask gives us the index into the results vector.
        const auto result_id = *first_key_entry ^ DYOD_CACHE_MASK;

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
      DebugAssert(!(*first_key_entry & DYOD_CACHE_MASK),
                  "CacheResultIds is set to false, but a cached or immediate key shortcut entry was found.");
    }

    // Lookup the key in the result_ids map
    auto it = result_ids.find(key);
    if (it != result_ids.end()) {
      // We have already seen this group and need to return a reference to the group's result.
      const auto result_id = it->second;
      if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
        // If requested, store the index the the first_key_entry and set the most significant bit to 1.
        *first_key_entry = DYOD_CACHE_MASK | result_id;
      }
      return results[result_id];
    }

    // We are seeing this group (i.e., this AggregateKey) for the first time, so we need to add it to the list of
    // results and set the row_id needed for restoring the GroupBy column(s).
    const auto result_id = result_ids.size();
    result_ids.emplace_hint(it, key, result_id);

    if (result_id >= results.size()) {
      results.resize(static_cast<size_t>(static_cast<double>(result_id + 1) * 1.5));
    }
    results[result_id].row_id = row_id;

    if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
      // If requested, store the index the the first_key_entry and set the most significant bit to 1.
      *first_key_entry = DYOD_CACHE_MASK | result_id;
    }

    return results[result_id];
  }
}

template <typename AggregateKey>
AggregateKey& dyod_get_aggregate_key([[maybe_unused]] KeysPerChunk<AggregateKey>& keys_per_chunk,
                                     [[maybe_unused]] const ChunkID chunk_id,
                                     [[maybe_unused]] const ChunkOffset chunk_offset) {
  if constexpr (!std::is_same_v<AggregateKey, DYODEmptyAggregateKey>) {
    auto& hash_keys = keys_per_chunk[chunk_id];

    return hash_keys[chunk_offset];
  } else {
    // We have to return a reference to something, so we create a static DYODEmptyAggregateKey here which is used by
    // every call.
    static DYODEmptyAggregateKey empty_aggregate_key;
    return empty_aggregate_key;
  }
}

template <typename Results>
void dyod_write_output_group_columns(const std::shared_ptr<const Table>& input_table,
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

      dyod_split_results_chunk_wise(false, results, pos_lists, unused_nulls,
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

      dyod_split_results_chunk_wise(
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
      dyod_prepare_output(intermediate_result, intermediate_result_chunk_count,
                          intermediate_result_column_definitions.size());
      for (auto output_chunk_id = ChunkID{0}; output_chunk_id < intermediate_result_chunk_count; ++output_chunk_id) {
        const auto& pos_list = pos_lists[output_chunk_id];
        intermediate_result[output_chunk_id][output_column_id] =
            std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, pos_list);
      }
    }
  }
}

inline bool has_aggregate_functions(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates) {
  return !aggregates.empty() && !std::ranges::all_of(aggregates, [](const auto& aggregate_expression) {
    return aggregate_expression->window_function == WindowFunction::Any;
  });
}

}  // namespace

namespace hyrise {

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids,
                                std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      _has_aggregate_functions(has_aggregate_functions(_aggregates)) {
  const auto num_cpus = Hyrise::get().topology.num_cpus();
  _max_job_size = left_input_table()->row_count() / (num_cpus * IDEAL_CPU_JOB_COUNT);
}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateDYOD::_on_cleanup() {
  // TODO(anyone): cleanup
}

/*
Visitor context for the AggregateVisitor. The DYODAggregateResultContext can be used without knowing the AggregateKey,
the DYODAggregateContext is the "full" version.
*/
template <typename ColumnDataType, WindowFunction aggregate_function>
struct DYODAggregateResultContext : DYODSegmentVisitorContext {
  using DYODAggregateResultAllocator = PolymorphicAllocator<DYODAggregateResults<ColumnDataType, aggregate_function>>;

  // In cases where we know how many values to expect, we can preallocate the context in order to avoid later
  // re-allocations.
  explicit DYODAggregateResultContext(const size_t preallocated_size = 0)
      : results(preallocated_size, DYODAggregateResultAllocator{&buffer}) {}

  std::pmr::monotonic_buffer_resource buffer;
  DYODAggregateResults<ColumnDataType, aggregate_function> results;
};

template <typename ColumnDataType, WindowFunction aggregate_function, typename AggregateKey>
struct DYODAggregateContext : public DYODAggregateResultContext<ColumnDataType, aggregate_function> {
  explicit DYODAggregateContext(const size_t preallocated_size = 0)
      : DYODAggregateResultContext<ColumnDataType, aggregate_function>(preallocated_size) {
    auto allocator = DYODAggregateResultIdMapAllocator<AggregateKey>{&this->buffer};

    // Unused if AggregateKey == DYODEmptyAggregateKey, but we initialize it anyway to reduce the number of diverging
    // code paths.
    result_ids = std::make_unique<DYODAggregateResultIdMap<AggregateKey>>(allocator);
  }

  std::unique_ptr<DYODAggregateResultIdMap<AggregateKey>> result_ids;

  void merge_results(DYODAggregateResult<ColumnDataType, aggregate_function>& target,
                     DYODAggregateResult<ColumnDataType, aggregate_function>& other) {
    // Merge DYODAggregateResults depending on their WindowFunction (special handling e.g. for the type of check to
    // perform)
    if constexpr (aggregate_function == WindowFunction::Min) {
      if (value_smaller(other.accumulator, target.accumulator)) {
        target.accumulator = other.accumulator;
      }
    }
    if constexpr (aggregate_function == WindowFunction::Max) {
      if (value_greater(other.accumulator, target.accumulator)) {
        target.accumulator = other.accumulator;
      }
    }
    if constexpr (aggregate_function == WindowFunction::Avg) {
      target.accumulator.first += other.accumulator.first;
      target.accumulator.second += other.accumulator.second;
    }
    if constexpr (aggregate_function == WindowFunction::Sum || aggregate_function == WindowFunction::Count) {
      target.accumulator += other.accumulator;
    }
    if constexpr (aggregate_function == WindowFunction::Any) {
      target.accumulator = other.accumulator;
    }
    if constexpr (aggregate_function == WindowFunction::CountDistinct) {
      target.accumulator.merge(other.accumulator);
    }
    if constexpr (aggregate_function == WindowFunction::StandardDeviationSample) {
      // Here, we merge two result sets of Welford's online algorithm for calculating the Standard Deviation
      // See https://en.wikipedia.org/w/index.php?title=Algorithms_for_calculating_variance&oldid=1367183160
      // for an introduction to Welford's online aglorithm. Under section "Parallel algorithm" there is an
      // introduction into the merging approach we use here. It is based on:
      // Chan et al. (1979), Updating formulae and a pairwise algorithm for computing sample variances
      // (Technical Report No. STAN-CS-79-773). Stanford University, Department of Computer Science,
      // https://i.stanford.edu/pub/cstr/reports/cs/tr/79/773/CS-TR-79-773.pdf

      auto a_count = target.accumulator[0];
      auto a_mean = target.accumulator[1];
      auto a_squared_distance_from_mean = target.accumulator[2];

      auto b_count = other.accumulator[0];
      auto b_mean = other.accumulator[1];
      auto b_squared_distance_from_mean = other.accumulator[2];

      auto ab_count = a_count + b_count;
      auto delta = b_mean - a_mean;
      auto ab_mean = (a_count * a_mean + b_count * b_mean) / ab_count;
      auto ab_squared_distance_from_mean = a_squared_distance_from_mean + b_squared_distance_from_mean +
                                           ((delta * delta * a_count * b_count) / ab_count);

      if (ab_count > 1) {
        // The SQL standard defines VAR_SAMP (which is the basis of STDDEV_SAMP) as NULL if the number of values is 1.
        const auto variance = ab_squared_distance_from_mean / (ab_count - 1);
        target.accumulator[3] = std::sqrt(variance);
      }

      target.accumulator[0] = ab_count;
      target.accumulator[1] = ab_mean;
      target.accumulator[2] = ab_squared_distance_from_mean;
    }
    target.has_aggregates |= other.has_aggregates;
  }

  // Currently we only merge two contexts with a single result i.e., a single group.
  void merge(std::shared_ptr<DYODAggregateContext<ColumnDataType, aggregate_function, AggregateKey>>& other) {
    DebugAssert(other->results.size() <= 1, "Expected other to have at most one result.");
    DebugAssert(other->results.size() >= 1, "Expected other to have at least one result.");
    DebugAssert(this->results.size() <= 1, "Expected this to have at most one result.");
    DebugAssert(this->results.size() >= 1, "Expected this to have at least one result.");
    auto& other_result = other->results[0];
    auto& result = this->results[0];
    if (!result.has_aggregates) {
      result = std::move(other_result);
      return;
    }
    if (!other_result.has_aggregates) {
      return;
    }
    merge_results(result, other_result);
  }
};

template <typename ColumnDataType, WindowFunction aggregate_function, typename AggregateKey>
void AggregateDYOD::_merge_contexts(std::shared_ptr<DYODSegmentVisitorContext>& target,
                                    std::shared_ptr<DYODSegmentVisitorContext>& other) {
  auto cast_target =
      std::static_pointer_cast<DYODAggregateContext<ColumnDataType, aggregate_function, AggregateKey>>(target);
  DebugAssert(cast_target, "Merged Context has unexpected template arguments.");

  auto cast_other =
      std::static_pointer_cast<DYODAggregateContext<ColumnDataType, aggregate_function, AggregateKey>>(other);
  DebugAssert(cast_other, "Merged Context has unexpected template arguments.");
  cast_target->merge(cast_other);
}

template <typename ColumnDataType, WindowFunction aggregate_function, typename AggregateKey>
void AggregateDYOD::_aggregate_segment(ChunkID chunk_id, ColumnID column_index, const AbstractSegment& abstract_segment,
                                       KeysPerChunk<AggregateKey>& keys_per_chunk,
                                       ContextsPerColumn& contexts_per_column, bool use_immediate_key_shortcut) {
  auto& context = *std::static_pointer_cast<DYODAggregateContext<ColumnDataType, aggregate_function, AggregateKey>>(
      contexts_per_column[column_index]);

  auto& result_ids = *context.result_ids;
  auto& results = context.results;

  // CacheResultIds is a boolean type parameter that is forwarded to visit_and_get_result, see the documentation over
  // there for details.
  const auto process_position = [&](const auto cache_result_ids, const auto& position) {
    const auto chunk_offset = position.chunk_offset();
    auto& result = visit_and_get_result(cache_result_ids, result_ids, results,
                                        dyod_get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                                        RowID{chunk_id, chunk_offset});

    using AggregateType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
    constexpr auto aggregator =
        DYODWindowFunctionBuilder<ColumnDataType, AggregateType, aggregate_function>().get_aggregate_function();
    // If the value is NULL, the current aggregate value does not change.
    if (!position.is_null()) {
      if constexpr (aggregate_function == WindowFunction::CountDistinct) {
        // For the case of CountDistinct, insert the current value into the set to keep track of distinct values.
        result.accumulator.emplace(position.value());
      } else {
        aggregator(ColumnDataType{position.value()}, result.has_aggregates, result.accumulator);
      }

      result.has_aggregates = true;
    }
  };

  // Pass true_type into prepare_output to enable certain optimizations: If we have more than one aggregate function
  // (and thus more than one context), it makes sense to cache the results indexes, see prepare_output for details.
  // Furthermore, if we use the immediate key shortcut (which uses the same code path as caching), we need to pass
  // true_type so that the aggregate keys are checked for immediate access values.
  if (contexts_per_column.size() > 1 || use_immediate_key_shortcut) {
    segment_iterate<ColumnDataType>(abstract_segment, [&](const auto& position) {
      process_position(std::true_type{}, position);
    });
  } else {
    segment_iterate<ColumnDataType>(abstract_segment, [&](const auto& position) {
      process_position(std::false_type{}, position);
    });
  }
}

template <typename CheckForSingleKey, typename AggregateKey>
  requires(std::is_same_v<AggregateKey, DYODEmptyAggregateKey>)
KeysPerChunk<AggregateKey> AggregateDYOD::_partition_by_groupby_keys(const std::shared_ptr<const Table>& input_table,
                                                                     std::atomic_size_t& expected_result_size,
                                                                     bool& use_immediate_key_shortcut,
                                                                     bool& guarantee_single_key) {
  if constexpr (std::is_same_v<CheckForSingleKey, std::true_type>) {
    guarantee_single_key = true;
  }
  return KeysPerChunk<AggregateKey>{};
}

/**
 * Partition the input chunks by the given group key(s). This is done by creating a vector that contains the
 * AggregateKey for each row. It is gradually built by visitors, one for each group segment.
 */
template <typename CheckForSingleKey, typename AggregateKey>
  requires(!std::is_same_v<AggregateKey, DYODEmptyAggregateKey>)
KeysPerChunk<AggregateKey> AggregateDYOD::_partition_by_groupby_keys(const std::shared_ptr<const Table>& input_table,
                                                                     std::atomic_size_t& expected_result_size,
                                                                     [[maybe_unused]] bool& use_immediate_key_shortcut,
                                                                     bool& guarantee_single_key) {
  auto keys_per_chunk = KeysPerChunk<AggregateKey>{};
  const auto chunk_count = input_table->chunk_count();

  // Create the actual data structure
  keys_per_chunk.reserve(chunk_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    if constexpr (std::is_same_v<AggregateKey, DYODAggregateKeySmallVector>) {
      keys_per_chunk.emplace_back(chunk->size(), AggregateKey(_groupby_column_ids.size()));
    } else {
      keys_per_chunk.emplace_back(chunk->size(), AggregateKey{});
    }
  }

  // Now that we have the data structures in place, we can start the actual work. We want to fill
  // keys_per_chunk[chunk_id][chunk_offset] with something that uniquely identifies the group into which that
  // position belongs. There are a couple of options here (cf. AggregateDYOD::_on_execute):
  //
  // 0 GROUP BY columns:   No partitioning needed; we do not reach this point because of the check for
  //                       DYODEmptyAggregateKey above
  // 1 GROUP BY column:    The AggregateKey is one dimensional, i.e., the same as DYODAggregateKeyEntry
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
  // (1) For types smaller than DYODAggregateKeyEntry, such as int32_t, their value range can be immediately mapped
  //     into uint64_t. We cannot do the same for int64_t because we need to account for NULL values.
  // (2) For strings not longer than five characters, there are 1+2^(1*8)+2^(2*8)+2^(3*8)+2^(4*8) potential values.
  //     We can immediately map these into a numerical representation by reinterpreting their byte storage as an
  //     integer. The calculation is described below. Note that this is done on a per-string basis and does not
  //     require all strings in the given column to be that short.
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(_groupby_column_ids.size());

  if constexpr (std::is_same_v<CheckForSingleKey, std::true_type>) {
    guarantee_single_key = true;
  }

  const auto groupby_column_count = _groupby_column_ids.size();
  for (auto group_column_index = size_t{0}; group_column_index < groupby_column_count; ++group_column_index) {
    jobs.emplace_back(std::make_shared<JobTask>([&input_table, group_column_index, &keys_per_chunk, &chunk_count,
                                                 &expected_result_size, &use_immediate_key_shortcut,
                                                 &guarantee_single_key, this]() {
      const auto groupby_column_id = _groupby_column_ids.at(group_column_index);
      const auto data_type = input_table->column_data_type(groupby_column_id);
      auto contains_nulls = false;

      // To avoid compiler errors for unused variable in certain template versions.
      (void)use_immediate_key_shortcut;

      // If we don't check for a singular key, we skip the overhead of null checks and just assume there are nulls.
      if constexpr (std::is_same_v<CheckForSingleKey, std::false_type>) {
        contains_nulls = input_table->column_is_nullable(groupby_column_id);
      }

      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        if constexpr (std::is_same_v<ColumnDataType, int32_t>) {
          // For values with a smaller type than DYODAggregateKeyEntry, we can use the value itself as an
          // DYODAggregateKeyEntry. We cannot do this for types with the same size as DYODAggregateKeyEntry as we need
          // to have a special NULL value. By using the value itself, we can save us the effort of building the
          // id_map.

          // Track the minimum and maximum key for the immediate key optimization. Search this cpp file for the last
          // use of `min_key` for a longer explanation.
          auto min_key = std::numeric_limits<DYODAggregateKeyEntry>::max();
          auto max_key = uint64_t{0};

          for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto chunk_in = input_table->get_chunk(chunk_id);
            const auto abstract_segment = chunk_in->get_segment(groupby_column_id);
            auto& keys = keys_per_chunk[chunk_id];
            segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& position) {
              const auto chunk_offset = position.chunk_offset();
              const auto int_to_uint = [](const int32_t value) {
                // We need to convert a potentially negative int32_t value into the uint64_t space. We do not care
                // about preserving the value, just its uniqueness. Subtract the minimum value in int32_t (which is
                // negative itself) to get a positive number.
                const auto shifted_value = static_cast<int64_t>(value) - std::numeric_limits<int32_t>::min();
                DebugAssert(shifted_value >= 0, "Type conversion failed");
                return static_cast<uint64_t>(shifted_value);
              };

              if (position.is_null()) {
                if constexpr (std::is_same_v<AggregateKey, DYODAggregateKeyEntry>) {
                  // Single GROUP BY column
                  keys[chunk_offset] = 0;
                } else {
                  // Multiple GROUP BY columns
                  keys[chunk_offset][group_column_index] = 0;
                }
                if constexpr (std::is_same_v<CheckForSingleKey, std::true_type>) {
                  contains_nulls = true;
                }
              } else {
                const auto key = int_to_uint(position.value()) + 1;

                if constexpr (std::is_same_v<AggregateKey, DYODAggregateKeyEntry>) {
                  // Single GROUP BY column
                  keys[chunk_offset] = key;
                } else {
                  // Multiple GROUP BY columns
                  keys[chunk_offset][group_column_index] = key;
                }

                min_key = std::min(min_key, key);
                max_key = std::max(max_key, key);
              }
            });
          }

          if (contains_nulls) {
            if (max_key != 0) {
              guarantee_single_key = false;
            }
          } else if (min_key != max_key) {
            guarantee_single_key = false;
          }

          if constexpr (std::is_same_v<AggregateKey, DYODAggregateKeyEntry>) {
            // In some cases (e.g., TPC-H Q18), we aggregate with consecutive int32_t values being used as a GROUP BY
            // key. Notably, this is the case when aggregating on the serial primary key of a table without filtering
            // the table before. In these cases, we do not need to perform a full hash-based aggregation, but can use
            // the values as immediate indexes into the list of results. To handle smaller gaps, we include cases up
            // to a certain threshold, but at some point these gaps make the approach less beneficial than a proper
            // hash-based approach. Both min_key and max_key do not correspond to the original int32_t value, but are
            // the result of the int_to_uint transformation. As such, they are guaranteed to be positive. This
            // shortcut only works if we are aggregating with a single GROUP BY column (i.e., when we use
            // DYODAggregateKeyEntry) - otherwise, we cannot establish a 1:1 mapping from keys_per_chunk to the result
            // id.
            // TODO(anyone): Find a reasonable threshold.
            if (max_key > 0 &&
                static_cast<double>(max_key - min_key) < static_cast<double>(input_table->row_count()) * 1.2) {
              // Include space for min, max, and NULL
              const auto null_offset = contains_nulls ? 1 : 0;
              expected_result_size = static_cast<size_t>(max_key - min_key) + 1 + null_offset;
              use_immediate_key_shortcut = true;

              // Rewrite the keys and (1) subtract min so that we can also handle consecutive keys that do not start
              // at 1* and (2) set the first bit which indicates that the key is an immediate index into the result
              // vector (see visit_and_get_result).
              // *) Note: Because of int_to_uint above, the values do not start at 1, anyway.

              for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
                const auto chunk_size = input_table->get_chunk(chunk_id)->size();
                for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
                  auto& key = keys_per_chunk[chunk_id][chunk_offset];
                  if (key == 0) {
                    // Key that denotes NULL, do not rewrite but set the cached flag
                    key = key | DYOD_CACHE_MASK;
                  } else {
                    key = (key - min_key + null_offset) | DYOD_CACHE_MASK;
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
          auto temp_buffer = std::pmr::monotonic_buffer_resource(1'000'000);
          auto allocator = PolymorphicAllocator<std::pair<const ColumnDataType, DYODAggregateKeyEntry>>{&temp_buffer};

          auto id_map = boost::unordered_flat_map<ColumnDataType, DYODAggregateKeyEntry, std::hash<ColumnDataType>,
                                                  std::equal_to<>, decltype(allocator)>(allocator);
          auto id_counter = DYODAggregateKeyEntry{1};

          if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
            // We store strings shorter than five characters without using the id_map. For that, we need to reserve
            // the IDs used for short strings (see below).
            id_counter = 5'000'000'000;
          }

          // We check if all value ids are the same to enable the optimization for single key aggregation.
          auto has_doubled_value_id = false;
          auto value_id_candidate = DYODAggregateKeyEntry{0};

          for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto chunk_in = input_table->get_chunk(chunk_id);
            if (!chunk_in) {
              continue;
            }

            auto& keys = keys_per_chunk[chunk_id];

            const auto abstract_segment = chunk_in->get_segment(groupby_column_id);
            segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& position) {
              auto chunk_offset = position.chunk_offset();
              if (position.is_null()) {
                if constexpr (std::is_same_v<CheckForSingleKey, std::true_type>) {
                  contains_nulls = true;
                }
                if constexpr (std::is_same_v<AggregateKey, DYODAggregateKeyEntry>) {
                  keys[chunk_offset] = 0;
                } else {
                  keys[chunk_offset][group_column_index] = 0;
                }
              } else {
                // We need to generate an ID that is unique for the value. In some cases, we can use an optimization,
                // in others, we cannot. We need to somehow track whether we have found an ID or not. For this, we
                // first set `value_id` to its maximum value. If after all branches it is still that max value, no
                // optimized  ID generation was applied and we need to generate the ID using the value->ID map.
                auto value_id = std::numeric_limits<DYODAggregateKeyEntry>::max();

                if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
                  const auto& string = position.value();
                  if (string.size() < 5) {
                    static_assert(std::is_same_v<DYODAggregateKeyEntry, uint64_t>,
                                  "Calculation only valid for uint64_t");

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

                if (value_id == std::numeric_limits<DYODAggregateKeyEntry>::max()) {
                  // Could not take the shortcut above, either because we don't have a string or because it is too
                  // long.
                  auto inserted = id_map.try_emplace(position.value(), id_counter);

                  value_id = inserted.first->second;

                  // If the id_map did not have the value as a key and a new element was inserted.
                  if (inserted.second) {
                    ++id_counter;
                  }
                }

                if constexpr (std::is_same_v<AggregateKey, DYODAggregateKeyEntry>) {
                  keys[chunk_offset] = value_id;
                } else {
                  keys[chunk_offset][group_column_index] = value_id;
                }

                if constexpr (std::is_same_v<CheckForSingleKey, std::true_type>) {
                  if (value_id_candidate == 0) {
                    value_id_candidate = value_id;
                  } else if (value_id_candidate != value_id) {
                    has_doubled_value_id = true;
                  }
                }
              }
            });
          }

          if (contains_nulls) {
            if (!(id_map.size() == 0 && value_id_candidate == 0)) {
              guarantee_single_key = false;
            }
          } else if (has_doubled_value_id) {
            guarantee_single_key = false;
          }

          // We will see at least `id_map.size()` different groups. We can use this knowledge to preallocate memory
          // for the results. Estimating the number of groups for multiple GROUP BY columns is somewhat hard, so we
          // simply take the number of groups created by the GROUP BY column with the highest number of distinct
          // values.
          auto previous_max = expected_result_size.load();
          while (previous_max < id_map.size()) {
            // The expected_result_size needs to be atomically updated as the GROUP BY columns are processed in
            // parallel. How to atomically update a maximum value? from https://stackoverflow.com/a/16190791/2204581
            if (expected_result_size.compare_exchange_strong(previous_max, id_map.size())) {
              break;
            }
          }
        }
      });
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  return keys_per_chunk;
}

// TODO(anyone): adaptive radix mask. Possibilities include:
//  (1) estimate distinct count and set mask accordingly.
//  (2) adapt mask recursively based on partition size.
//  (3) add low cardinality partitioning for more than one key.

// 32 buckets
constexpr auto RADIX_MASK = 0x1f;
constexpr auto RADIX_SPLIT_MAX_BUCKETS = RADIX_MASK + 1;

template <typename AggregateKey>
std::shared_ptr<Table> AggregateDYOD::_partition_and_aggregate() {
  if (left_input_table()->type() == TableType::Data) {
    return _partition_and_aggregate<std::false_type, AggregateKey>();
  } else {
    return _partition_and_aggregate<std::true_type, AggregateKey>();
  }
}

/**
 * When appropriate (using a multi-threaded scheduler and at least one groupby column), split the input table into buckets
 * and then spawn one job per bucket for the aggregation. Otherwise, simply aggregate on the entire input table. Create an
 * output table and write the results of the aggregation there.
 */
template <typename IsReferenceTable, typename AggregateKey>
std::shared_ptr<Table> AggregateDYOD::_partition_and_aggregate() {
  const auto aggregates_count = _aggregates.size();
  const auto& input_table = left_input_table();
  const auto column_count = input_table->column_count();
  if constexpr (HYRISE_DEBUG) {
    for (const auto& groupby_column_id : _groupby_column_ids) {
      Assert(groupby_column_id < column_count, "GroupBy column index out of bounds.");
    }
  }

  // Check for invalid aggregates
  _validate_aggregates();

  const auto is_multi_threaded = Hyrise::get().is_multi_threaded();
  const auto row_count = input_table->row_count();
  const auto groupby_column_count = _groupby_column_ids.size();

  // The operator output and the table in which the aggregate results will be materialized exist globally.
  // For multi-threading, write-access in guarded by _output_mutex and _aggregate_mutex respectively.
  auto output_table = std::shared_ptr<Table>{};
  auto aggregate_result_table = std::shared_ptr<Table>{};

  // If we only work on a single thread, have an empty table, or only a single group,
  // we don't bother splitting by groupby groups.
  if (!is_multi_threaded || row_count == 0 || groupby_column_count == 0) {
    auto contexts_per_column = ContextsPerColumn(aggregates_count);
    // We only enable the single group optimization if we have threads.
    _aggregate<AggregateKey>(contexts_per_column, input_table, is_multi_threaded);
    _write_output(contexts_per_column, input_table, output_table, aggregate_result_table);
    return output_table;
  }

  // First Split: Hash all groupby keys in parallel and populate the pos_list for each radix bucket with the
  // corresponding RowIDs for Data Tables or chunk_offsets for Reference tables.

  // If we have a Data table, we directly partition into PosLists and forward these to the job-local input tables.
  // For a Reference table, we only store the ChunkOffsets since we have to resolve the PosList anyway later.
  // TODO(anyone): Consider using pmr_vector instead of std::vector.
  using ReferenceList =
      std::conditional_t<std::is_same_v<IsReferenceTable, std::true_type>, std::vector<ChunkOffset>, RowIDPosList>;
  using PosLists = std::vector<std::shared_ptr<ReferenceList>>;

  auto pos_lists_per_job = std::array<std::shared_ptr<PosLists>, RADIX_SPLIT_MAX_BUCKETS>();

  const auto chunk_count = input_table->chunk_count();
  for (auto bucket_id = size_t{0}; bucket_id < RADIX_SPLIT_MAX_BUCKETS; ++bucket_id) {
    auto pos_lists = std::make_shared<PosLists>(chunk_count);
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      (*pos_lists)[chunk_id] = std::make_shared<ReferenceList>();
    }
    pos_lists_per_job[bucket_id] = pos_lists;
  }

  auto hashing_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  hashing_jobs.reserve(chunk_count);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    const auto chunk_size = chunk->size();

    if (chunk_size == 0) {
      continue;
    }

    hashing_jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id, chunk_size, chunk]() {
      auto hashes = std::vector<size_t>(chunk_size, 0);
      for (const auto& column_id : _groupby_column_ids) {
        const auto data_type = input_table->column_data_type(column_id);
        resolve_data_type(data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          const auto& abstract_segment = chunk->get_segment(column_id);
          const auto hash_f = std::hash<ColumnDataType>{};  // TODO(anyone): Use a better hash function

          segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& position) {
            auto value = position.is_null() ? 0 : hash_f(position.value());

            boost::hash_combine(hashes[position.chunk_offset()], hash_mix(value));
          });
        });
      }

      // Cache pointers in a local array to avoid repeated lookups from the array
      // Here we can also reserve some space before pushing back the RowIDs/chunk_offsets,
      // approximately chunk_size / RADIX_SPLIT_MAX_BUCKETS
      // This is probably not accurate, but better than reserving nothing.
      std::array<std::shared_ptr<ReferenceList>, RADIX_SPLIT_MAX_BUCKETS> local_pos_lists;
      for (auto i = 0; i < RADIX_SPLIT_MAX_BUCKETS; ++i) {
        local_pos_lists[i] = pos_lists_per_job[i]->at(chunk_id);
        local_pos_lists[i]->reserve(chunk_size / RADIX_SPLIT_MAX_BUCKETS);
      }

      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        // see definition of pos_lists_per_job
        const auto value = hashes[chunk_offset];
        const auto key = value & RADIX_MASK;
        if constexpr (std::is_same_v<IsReferenceTable, std::true_type>) {
          local_pos_lists[key]->push_back(chunk_offset);
        } else {
          local_pos_lists[key]->push_back(RowID{chunk_id, chunk_offset});
        }
      }
    }));
  }

  // If there are no jobs, return an empty table. Otherwise, schedule the jobs.
  // TODO(anyone): Skip scheduler for a single job
  if (hashing_jobs.empty()) {
    return std::make_shared<Table>(input_table->column_definitions(), TableType::References);
  } else {
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(hashing_jobs);
  }

  // End First Split
  // After splitting into radix buckets, we can aggregate each bucket in parallel.
  // For each bucket we create a local input table that contains references to the corresponding rows of the original
  // input table. For ReferenceSegments we re-use the code from TableScan.

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(RADIX_SPLIT_MAX_BUCKETS);

  for (auto job_id = size_t{0}; job_id < RADIX_SPLIT_MAX_BUCKETS; ++job_id) {
    const auto& pos_lists = *pos_lists_per_job[job_id];
    const auto bucket_is_empty = std::all_of(pos_lists.begin(), pos_lists.end(), [](const auto& pos_list) {
      return pos_list->empty();
    });

    if (bucket_is_empty) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&]() {
      const auto local_input_table = std::make_shared<Table>(input_table->column_definitions(), TableType::References);
      // Scan input table for radix bucket and populate the local input table with references to the original input
      // table.
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        auto pos_list = pos_lists[chunk_id];
        if (pos_list->empty()) {
          continue;
        }
        auto out_segments = Segments{};
        if constexpr (std::is_same_v<IsReferenceTable, std::false_type>) {
          pos_list->guarantee_single_chunk();
          out_segments.resize(column_count);
          for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
            out_segments[column_id] = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);
          }
        } else {
          // Re-used from TableScan: Resolve the ReferenceSegments of the input reference table into our local table.
          // TODO(anyone): Find a way to avoid the code duplication
          const auto& chunk_in = input_table->get_chunk(chunk_id);
          if (pos_list->size() == chunk_in->size()) {
            // Shortcut - the entire input reference segment matches, so we can simply forward that chunk.
            local_input_table->append_chunk(chunk_in->segments());
            continue;
          } else {
            auto filtered_pos_lists = std::map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>>{};

            out_segments.resize(column_count);
            for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
              const auto segment_in = chunk_in->get_segment(column_id);

              const auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
              DebugAssert(ref_segment_in, "All segments should be of type ReferenceSegment.");

              const auto pos_list_in = ref_segment_in->pos_list();

              auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

              // We only create a new RowIdPosList if we have not yet created one for the pos_list_in.
              // This accounts for the same PosList being used for multiple columns.
              if (!filtered_pos_list) {
                filtered_pos_list = std::make_shared<RowIDPosList>(pos_list->size());

                if (pos_list_in->references_single_chunk()) {
                  filtered_pos_list->guarantee_single_chunk();
                }

                auto offset = size_t{0};

                for (const auto& match : *pos_list) {
                  const auto row_id = (*pos_list_in)[match];
                  (*filtered_pos_list)[offset] = row_id;
                  ++offset;
                }
              }

              const auto table_out = ref_segment_in->referenced_table();
              const auto column_id_out = ref_segment_in->referenced_column_id();
              out_segments[column_id] = std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
            }
          }
        }
        local_input_table->append_chunk(out_segments);
      }

      // Aggregate the local input table in this job, write its output into the output table
      // The output table is shared between all jobs, but no two jobs write to the same chunk. For avoiding append_chunk
      // collisions we are using a mutex in _write_output.
      const auto local_row_count = local_input_table->row_count();

      auto contexts_per_column = ContextsPerColumn(aggregates_count);
      _aggregate<AggregateKey>(contexts_per_column, local_input_table, local_row_count > _max_job_size);

      _write_output(contexts_per_column, local_input_table, output_table, aggregate_result_table);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  return output_table;
}

const auto JOB_COUNT_ESTIMATE = ChunkID{16};

/**
 * This is the unpartitioned variant. It will handle the table partitioning for low cardinalities and call the partitioned
 * variant below for the actual aggregation.
 */
template <typename AggregateKey>
void AggregateDYOD::_aggregate(ContextsPerColumn& contexts_per_column, const std::shared_ptr<const Table>& input_table,
                               bool check_for_single_keys) {
  std::atomic_size_t expected_result_size;
  bool use_immediate_key_shortcut = false;
  bool guarantee_single_key = false;
  auto keys_per_chunk = check_for_single_keys
                            ? _partition_by_groupby_keys<std::true_type, AggregateKey>(
                                  input_table, expected_result_size, use_immediate_key_shortcut, guarantee_single_key)
                            : _partition_by_groupby_keys<std::false_type, AggregateKey>(
                                  input_table, expected_result_size, use_immediate_key_shortcut, guarantee_single_key);

  // TODO(anyone): Estimate ideal number of jobs/threads for this bucket.
  // If we only have one group, we can easily split this job, since we have only one result per context.
  const auto chunk_count = input_table->chunk_count();
  auto bucket_job_count = (guarantee_single_key && input_table->row_count() > _max_job_size)
                              ? std::min(JOB_COUNT_ESTIMATE, chunk_count)
                              : ChunkID{1};

  // We have an empty table or cannot enable single key optimization, so just skip the rest.
  if (bucket_job_count < 2) {
    _aggregate<AggregateKey>(contexts_per_column, input_table, expected_result_size, use_immediate_key_shortcut,
                             keys_per_chunk, ChunkID{0}, chunk_count);
    return;
  }

  const auto job_size = chunk_count / bucket_job_count;

  auto contexts_per_column_per_job = std::vector<ContextsPerColumn>{};
  contexts_per_column_per_job.reserve(bucket_job_count);

  // If we have only one group in the table, further split the table into subsets of chunks, then call _aggregate
  // separately.
  auto aggregate_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  aggregate_jobs.reserve(bucket_job_count);

  const auto aggregates_count = _aggregates.size();
  for (auto job_id = ChunkID{0}; job_id < bucket_job_count; ++job_id) {
    contexts_per_column_per_job.emplace_back(aggregates_count);
    const auto aggregate_bucket = [&, job_id]() {
      const auto job_start = static_cast<ChunkID>(job_id * job_size);
      const auto job_end =
          static_cast<ChunkID>((job_id + 1 == bucket_job_count) ? (chunk_count) : ((job_id + 1) * job_size));

      _aggregate<AggregateKey>(contexts_per_column_per_job[job_id], input_table, expected_result_size,
                               use_immediate_key_shortcut, keys_per_chunk, job_start, job_end);
    };

    if (bucket_job_count == 1) {
      aggregate_bucket();
    } else {
      aggregate_jobs.emplace_back(std::make_shared<JobTask>(aggregate_bucket));
    }
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(aggregate_jobs);
  if (input_table->empty()) {
    return;
  }

  // TODO(anyone): Make more pretty.
  auto merge_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  merge_jobs.reserve(aggregates_count);

  for (auto aggregate_index = ColumnID{0}; aggregate_index < aggregates_count; ++aggregate_index) {
    const auto merge_one_aggregate = [&, aggregate_index]() {
      const auto& aggregate = _aggregates[aggregate_index];
      const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
      const auto input_column_id = pqp_column.column_id;

      // Output column for COUNT(*).
      const auto data_type =
          input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);

      auto& context = contexts_per_column_per_job[0][aggregate_index];
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        resolve_window_function(aggregate->window_function, [&]<WindowFunction aggregate_func>() {
          for (auto job_id = ChunkID{1}; job_id < bucket_job_count; ++job_id) {
            auto& other = contexts_per_column_per_job[job_id][aggregate_index];
            _merge_contexts<ColumnDataType, aggregate_func, AggregateKey>(context, other);
          }
        });
      });
    };

    if (aggregates_count == 1) {
      merge_one_aggregate();
    } else {
      merge_jobs.emplace_back(std::make_shared<JobTask>(merge_one_aggregate));
    }
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(merge_jobs);

  contexts_per_column = std::move(contexts_per_column_per_job[0]);
}

/**
 * This is the partitioned variant. It will only aggregate on a subset of chunks (which can be all chunks, if no partitioning
 * was performed previously). It performs the actual aggregation, writing the result to contexts_per_column.
 */
template <typename AggregateKey>
void AggregateDYOD::_aggregate(ContextsPerColumn& contexts_per_column, const std::shared_ptr<const Table>& input_table,
                               std::atomic_size_t& expected_result_size, bool& use_immediate_key_shortcut,
                               KeysPerChunk<AggregateKey>& keys_per_chunk, ChunkID start, ChunkID end) {
  if (!_has_aggregate_functions) {
    /*
    Insert a dummy context for the DISTINCT implementation. That way, `contexts_per_column` will always have at least
    one context with results. This is important later on when we write the group keys into the table. The template
    parameters (int32_t, WindowFunction::Min) do not matter, as we do not calculate an aggregate anyway.
    */
    auto context =
        std::make_shared<DYODAggregateContext<int32_t, WindowFunction::Min, AggregateKey>>(expected_result_size);

    contexts_per_column.push_back(context);
  }

  /**
   * Create an DYODAggregateContext for each column in the input table that a normal (i.e. non-DISTINCT) aggregate is
   * created on. We do this here, and not in the per-chunk-loop below, because there might be no Chunks in the input
   * and _write_aggregate_output() needs these contexts anyway.
   */
  const auto aggregates_count = _aggregates.size();
  for (auto aggregate_index = ColumnID{0}; aggregate_index < aggregates_count; ++aggregate_index) {
    const auto& aggregate = _aggregates[aggregate_index];

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    if (input_column_id == INVALID_COLUMN_ID) {
      DebugAssert(aggregate->window_function == WindowFunction::Count, "Only COUNT may have an invalid ColumnID.");
      // SELECT COUNT(*) - we know the template arguments, so we do not need a visitor.
      auto context = std::make_shared<DYODAggregateContext<CountColumnType, WindowFunction::Count, AggregateKey>>(
          expected_result_size);

      contexts_per_column[aggregate_index] = context;
      continue;
    }
    const auto data_type = input_table->column_data_type(input_column_id);
    contexts_per_column[aggregate_index] =
        _create_aggregate_context<AggregateKey>(data_type, aggregate->window_function, expected_result_size);
  }

  // Process chunks and perform aggregations.
  for (auto chunk_id = start; chunk_id < end; ++chunk_id) {
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

      auto context =
          std::static_pointer_cast<DYODAggregateContext<DistinctColumnType, WindowFunction::Min, AggregateKey>>(
              contexts_per_column[0]);

      auto& result_ids = *context->result_ids;
      auto& results = context->results;

      // Add value or combination of values is added to the list of distinct value(s). This is done by calling
      // visit_and_get_result, which adds the corresponding entry in the list of GROUP BY values.
      if (use_immediate_key_shortcut) {
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk_size; ++chunk_offset) {
          // We are able to use immediate keys, so pass true_type so that the combined caching/immediate key code path
          // is enabled in visit_and_get_result.
          visit_and_get_result(std::true_type{}, result_ids, results,
                               dyod_get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                               RowID{chunk_id, chunk_offset});
        }
      } else {
        // Same as above, but we do not have immediate keys, so we disable that code path to reduce the complexity of
        // dyod_get_aggregate_key.
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk_size; ++chunk_offset) {
          visit_and_get_result(std::false_type{}, result_ids, results,
                               dyod_get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                               RowID{chunk_id, chunk_offset});
        }
      }
    } else {
      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(aggregates_count);

      for (auto aggregate_index = ColumnID{0}; aggregate_index < aggregates_count; ++aggregate_index) {
        const auto perform_aggregation = [&, aggregate_index]() {
          const auto aggregate = _aggregates[aggregate_index];
          /**
           * Special COUNT(*) implementation.
           * Because COUNT(*) does not have a specific target column, we use the maximum ColumnID. We then go through the
           * `keys_per_chunk` map and count the occurrences of each group key. The results are saved in the regular
           * `accumulator` variable so that we do not need a specific output logic for COUNT(*).
           */

          const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
          const auto input_column_id = pqp_column.column_id;

          if (input_column_id == INVALID_COLUMN_ID) {
            Assert(aggregate->window_function == WindowFunction::Count, "Only COUNT may have an invalid ColumnID.");
            auto context =
                std::static_pointer_cast<DYODAggregateContext<CountColumnType, WindowFunction::Count, AggregateKey>>(
                    contexts_per_column[aggregate_index]);

            auto& result_ids = *context->result_ids;
            auto& results = context->results;

            if constexpr (std::is_same_v<AggregateKey, DYODEmptyAggregateKey>) {
              // Not grouped by anything, simply count the number of rows.
              results.resize(1);
              results[0].accumulator += input_chunk_size;
              results[0].has_aggregates = input_chunk_size > 0;

              // We need to set any RowID because the default value (NULL_ROW_ID) would later be skipped. As we are not
              // reconstructing the GROUP BY values later, the exact value of this row_id does not matter, as long as it
              // not NULL_ROW_ID.
              results[0].row_id = RowID{ChunkID{0}, ChunkOffset{0}};
            } else {
              // Count occurrences for each group key -  If we have more than one aggregate function (and thus more than
              // one context), it makes sense to cache the results indexes, see visit_and_get_result for details.
              if (contexts_per_column.size() > 1 || use_immediate_key_shortcut) {
                for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk_size; ++chunk_offset) {
                  // Use CacheResultIds==true_type if we have more than one group by column or if the cached result ids
                  // have been written by the immediate key shortcut
                  auto& result =
                      visit_and_get_result(std::true_type{}, result_ids, results,
                                           dyod_get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                                           RowID{chunk_id, chunk_offset});

                  ++result.accumulator;
                  result.has_aggregates = true;
                }
              } else {
                for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk_size; ++chunk_offset) {
                  auto& result =
                      visit_and_get_result(std::false_type{}, result_ids, results,
                                           dyod_get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                                           RowID{chunk_id, chunk_offset});
                  ++result.accumulator;
                  result.has_aggregates = true;
                }
              }
            }

            return;
          }

          const auto abstract_segment = chunk_in->get_segment(input_column_id);
          const auto data_type = input_table->column_data_type(input_column_id);

          /*
          Invoke correct aggregator for each segment
          */

          resolve_data_type(data_type, [&, aggregate](auto type) {
            using ColumnDataType = typename decltype(type)::type;

            // ANY is a pseudo-function and is handled by `dyod_get_aggregate_key`.
            resolve_window_function_without_any(aggregate->window_function, [&]<WindowFunction aggregate_func>() {
              _aggregate_segment<ColumnDataType, aggregate_func, AggregateKey>(
                  chunk_id, aggregate_index, *abstract_segment, keys_per_chunk, contexts_per_column,
                  use_immediate_key_shortcut);
            });
          });
        };
        // If we cache the lookups, we do one run single threaded to write the cached results.
        // If there are at most two aggregates, we don't create an extra job.
        // If we use immediate key shortcuts, key_per_chunk should remain const and thus thread safe.
        if ((aggregate_index == size_t{0} && contexts_per_column.size() > 1) || aggregates_count <= 2) {
          perform_aggregation();
        } else {
          jobs.emplace_back(std::make_shared<JobTask>(perform_aggregation));
        }
      }

      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
    }
  }
}  // NOLINT(readability/fn_size)

void AggregateDYOD::_write_output(ContextsPerColumn& contexts_per_column,
                                  const std::shared_ptr<const Table>& input_table, std::shared_ptr<Table>& output_table,
                                  std::shared_ptr<Table>& aggregate_result_table) {
  const auto num_output_columns = _groupby_column_ids.size() + _aggregates.size();
  auto output_column_definitions = TableColumnDefinitions{};
  output_column_definitions.resize(num_output_columns);

  auto intermediate_result = std::vector<Segments>();

  /**
   * If only GROUP BY columns (including ANY pseudo-aggregates) are written, we need to call `dyod_get_aggregate_key`.
   *   Example: SELECT c_custkey, c_name FROM customer GROUP BY c_custkey, c_name (same as SELECT DISTINCT), which
   *            is rewritten to group only on c_custkey and collect c_name as an ANY pseudo-aggregate.
   * Otherwise, it is called by the first call to `_write_aggregate_output`.
   **/
  if (!_has_aggregate_functions) {
    auto context = std::static_pointer_cast<DYODAggregateResultContext<DistinctColumnType, WindowFunction::Min>>(
        contexts_per_column[0]);
    // auto groupby_columns_writing_timer = Timer{};
    dyod_write_output_group_columns(input_table, _aggregates, _groupby_column_ids, context->results,
                                    output_column_definitions, intermediate_result);
  }

  /*
  Write the aggregated columns to the output.
  */
  auto aggregate_index = ColumnID{0};
  for (const auto& aggregate : _aggregates) {
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    // Output column for COUNT(*).
    const auto data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      // Pseudo-aggregates are written by dyod_write_output_group_columns.
      resolve_window_function_without_any(aggregate->window_function, [&]<WindowFunction aggregate_func>() {
        _write_aggregate_output<ColumnDataType, aggregate_func>(
            aggregate_index, contexts_per_column, intermediate_result, input_table, output_column_definitions);
      });
    });

    ++aggregate_index;
  }

  /**
   * Write the output.
   *
   * At this point, we collected the GROUP BY columns as reference segments, which are split using the default chunk
   * size (minus gap rows, see comments on NULL_ID). Similarly, the aggregate values are split into chunks. Both are currently stored in
   * intermediate_result. We write the materialized aggregate columns to the (global) aggregate_result_table, then store
   * reference segments to those columns as well as the groupby keys to the output table.
  */

  auto reference_segment_indexes = std::vector<ColumnID>(_groupby_column_ids.size());
  auto entireposlist_indexes = std::vector<ColumnID>{};
  entireposlist_indexes.reserve(_aggregates.size());

  // NOLINTNEXTLINE(modernize-use-ranges): We need LLVM 21's libc++ for std::ranges::iota.
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

  // Write the materialized columns to the aggregate_result_table. The operator output references this table's
  // columns via `EntireChunkPosList` reference segments. Note that we need the aggregate_result_table to be global
  // to avoid creating one table per thread, which will outlive the thread as it is referenced by the output.

  auto first_materialized_chunk_id = ChunkID{0};

  if (!entireposlist_indexes.empty()) {
    const auto materialized_column_count = entireposlist_indexes.size();

    auto aggregate_chunks = std::vector<std::shared_ptr<Segments>>();
    for (const auto& materialized_result_chunk : intermediate_result) {
      auto aggregate_segments = std::make_shared<Segments>();
      aggregate_segments->reserve(materialized_column_count);

      for (const auto entireposlist_index : entireposlist_indexes) {
        aggregate_segments->emplace_back(materialized_result_chunk[entireposlist_index]);
      }
      aggregate_chunks.emplace_back(aggregate_segments);
    }

    // Locking the aggregate_result_table is delayed as much as possible to reduce waiting time for threads.
    // We store the ChunkID of the first chunk we append for later use in the ReferenceSegments of our
    // operator output.
    const auto lock = std::lock_guard<std::mutex>{_aggregate_mutex};

    if (!_aggregate_writing_started) {
      auto aggregate_column_definitions = std::vector<TableColumnDefinition>{};
      aggregate_column_definitions.reserve(materialized_column_count);

      for (const auto entireposlist_index : entireposlist_indexes) {
        aggregate_column_definitions.emplace_back(output_column_definitions[entireposlist_index]);
      }

      aggregate_result_table = std::make_shared<Table>(aggregate_column_definitions, TableType::Data);
      _aggregate_writing_started = true;
    }
    first_materialized_chunk_id = aggregate_result_table->chunk_count();

    for (auto& aggregate_segments : aggregate_chunks) {
      aggregate_result_table->append_chunk(*aggregate_segments);
    }
  }

  // Write the final output to the output_table. We now combine actual reference segments (e.g., of GROUP BY columns)
  // with segments that reference the temporary materialized table created above. All chunks are first created before
  // writing to the table starts to reduce the amount of time the table is locked.
  auto output_chunks = std::vector<std::shared_ptr<Segments>>();

  if (!intermediate_result.empty() && intermediate_result.front()[0]->size() > 0) {
    const auto output_table_chunk_count = intermediate_result.size();
    for (auto chunk_id = ChunkID{0}; chunk_id < output_table_chunk_count; ++chunk_id) {
      if (!intermediate_result[chunk_id][0]) {
        // When vectors have been oversized (see visit_and_get_result()), intermediate chunks might be completely empty.
        continue;
      }

      auto reference_segments = std::make_shared<Segments>(num_output_columns);
      auto& reference_segments_reference = *reference_segments;

      for (const auto column_id : reference_segment_indexes) {
        DebugAssert(std::dynamic_pointer_cast<const ReferenceSegment>(intermediate_result[chunk_id][column_id]),
                    "Expected a ReferenceSegment at this position.");
        reference_segments_reference[column_id] = intermediate_result[chunk_id][column_id];
      }

      const auto materialized_table_column_count = entireposlist_indexes.size();
      const auto chunk_size = intermediate_result[chunk_id][0]->size();
      Assert(!_groupby_column_ids.empty() || materialized_table_column_count > 0,
             "Output does not contain any columns.");

      for (auto materialized_table_column_id = ColumnID{0};
           materialized_table_column_id < materialized_table_column_count; ++materialized_table_column_id) {
        DebugAssert(!std::dynamic_pointer_cast<const ReferenceSegment>(
                        aggregate_result_table->get_chunk(ChunkID{first_materialized_chunk_id + chunk_id})
                            ->get_segment(ColumnID{materialized_table_column_id})),
                    "Unexpected reference segment at this position.");
        const auto entire_chunk_pos_list =
            std::make_shared<EntireChunkPosList>(ChunkID{first_materialized_chunk_id + chunk_id}, chunk_size);
        reference_segments_reference[entireposlist_indexes[materialized_table_column_id]] =
            std::make_shared<ReferenceSegment>(aggregate_result_table, materialized_table_column_id,
                                               entire_chunk_pos_list);
      }
      output_chunks.emplace_back(reference_segments);
    }
  }

  // Lock the output table to append all chunks created above.
  const auto lock = std::lock_guard<std::mutex>{_output_mutex};

  if (!_output_writing_started) {
    _output_writing_started = true;
    output_table = std::make_shared<Table>(output_column_definitions, TableType::References);
  }
  if (!intermediate_result.empty() && intermediate_result.front()[0]->size() > 0) {
    for (const auto& output_segments : output_chunks) {
      output_table->append_chunk(*output_segments);
    }
  }
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  // We do not want the overhead of a vector with heap storage when we have a limited number of aggregate columns.
  // However, more specializations mean more compile time. We now have specializations for 0, 1, 2, and >2 GROUP BY
  // columns.

  switch (_groupby_column_ids.size()) {
    case 0:
      return _partition_and_aggregate<DYODEmptyAggregateKey>();
    case 1:
      // No need for a complex data structure if we only have one entry.
      return _partition_and_aggregate<DYODAggregateKeyEntry>();
    case 2:
      return _partition_and_aggregate<std::array<DYODAggregateKeyEntry, 2>>();
    default:
      return _partition_and_aggregate<DYODAggregateKeySmallVector>();
  }
}

template <typename ColumnDataType, WindowFunction aggregate_function>
void AggregateDYOD::_write_aggregate_output(ColumnID aggregate_index, ContextsPerColumn& contexts_per_column,
                                            std::vector<Segments>& intermediate_result,
                                            const std::shared_ptr<const Table>& input_table,
                                            TableColumnDefinitions& output_column_definitions) {
  // Retrieve type information from the aggregation traits.
  using aggregate_type = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  auto result_type = WindowFunctionTraits<ColumnDataType, aggregate_function>::RESULT_TYPE;

  const auto& aggregate = _aggregates[aggregate_index];

  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  const auto input_column_id = pqp_column.column_id;

  if (result_type == DataType::Null) {
    // If not specified, it is the input column’s type.
    result_type = input_table->column_data_type(input_column_id);
  }

  auto context = std::static_pointer_cast<DYODAggregateResultContext<ColumnDataType, aggregate_function>>(
      contexts_per_column[aggregate_index]);

  const auto& results = context->results;

  // Before writing the first aggregate column, write all group keys into the respective columns.
  if (aggregate_index == 0) {
    dyod_write_output_group_columns(input_table, _aggregates, _groupby_column_ids, results, output_column_definitions,
                                    intermediate_result);
  }

  constexpr auto NEEDS_NULL =
      (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct);
  const auto output_column_id = _groupby_column_ids.size() + aggregate_index;

  auto value_vectors = std::vector<pmr_vector<aggregate_type>>{};
  auto null_vectors = std::vector<pmr_vector<bool>>{};
  auto aggregate_result_contains_nulls =
      dyod_write_aggregate_values<ColumnDataType, aggregate_type, aggregate_function>(results, value_vectors,
                                                                                      null_vectors);

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

  DebugAssert(NEEDS_NULL || null_vectors.empty(), "dyod_write_aggregate_values unexpectedly wrote NULL values.");

  dyod_prepare_output(intermediate_result, value_vectors.size(), output_column_definitions.size());

  output_column_definitions[output_column_id] =
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
    intermediate_result[segment_id][output_column_id] = output_segment;
  }
}

template <typename AggregateKey>
std::shared_ptr<DYODSegmentVisitorContext> AggregateDYOD::_create_aggregate_context(
    const DataType data_type, const WindowFunction aggregate_function, std::atomic_size_t& expected_result_size) const {
  std::shared_ptr<DYODSegmentVisitorContext> context;
  const auto size = expected_result_size.load();
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    resolve_window_function(aggregate_function, [&]<WindowFunction aggregate_func>() {
      context = std::make_shared<DYODAggregateContext<ColumnDataType, aggregate_func, AggregateKey>>(size);
    });
  });

  return context;
}

}  // namespace hyrise
