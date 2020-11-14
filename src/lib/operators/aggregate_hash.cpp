#include "aggregate_hash.hpp"

#include <cmath>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <magic_enum.hpp>

#include "aggregate/aggregate_traits.hpp"
#include "constant_mappings.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/aligned_size.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/timer.hpp"

namespace {
using namespace opossum;  // NOLINT

// Given an AggregateKey key, and a RowId row_id where this AggregateKey was encountered, this first checks if the
// AggregateKey was seen before. If not, a new aggregate result is inserted into results and connected to the row id.
// This is important so that we can reconstruct the original values later. In any case, a reference to the result is
// returned so that result information, such as the aggregate's count or sum, can be modified by the caller.
template <typename ResultIds, typename Results, typename AggregateKey>
typename Results::reference get_or_add_result(ResultIds& result_ids, Results& results, const AggregateKey& key,
                                              const RowID& row_id) {
  // Get the result id for the current key or add it to the id map
  if constexpr (std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    if (results.empty()) {
      results.emplace_back();
      results[0].row_id = row_id;
    }
    return results[0];
  } else {
    auto it = result_ids.find(key);
    if (it != result_ids.end()) return results[it->second];

    auto result_id = results.size();

    result_ids.emplace_hint(it, key, result_id);

    // If it was added to the id map, add the current row id to the result list so that we can revert the
    // value(s) -> key mapping
    results.emplace_back();
    results[result_id].row_id = row_id;

    return results[result_id];
  }
}

template <typename AggregateKey>
const AggregateKey& get_aggregate_key([[maybe_unused]] const KeysPerChunk<AggregateKey>& keys_per_chunk,
                                      [[maybe_unused]] const ChunkID chunk_id,
                                      [[maybe_unused]] const ChunkOffset chunk_offset) {
  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    const auto& hash_keys = keys_per_chunk[chunk_id];

    return hash_keys[chunk_offset];
  } else {
    // We have to return a reference to something, so we create a static EmptyAggregateKey here which is used by
    // every call.
    static EmptyAggregateKey empty_aggregate_key;
    return empty_aggregate_key;
  }
}

}  // namespace

namespace opossum {

AggregateHash::AggregateHash(const std::shared_ptr<AbstractOperator>& in,
                             const std::vector<std::shared_ptr<AggregateExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(in, aggregates, groupby_column_ids,
                                std::make_unique<OperatorPerformanceData<OperatorSteps>>()) {
  _has_aggregate_functions =
      !_aggregates.empty() && !std::all_of(_aggregates.begin(), _aggregates.end(), [](const auto aggregate_expression) {
        return aggregate_expression->aggregate_function == AggregateFunction::Any;
      });
}

const std::string& AggregateHash::name() const {
  static const auto name = std::string{"AggregateHash"};
  return name;
}

std::shared_ptr<AbstractOperator> AggregateHash::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<AggregateHash>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateHash::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateHash::_on_cleanup() { _contexts_per_column.clear(); }

/*
Visitor context for the AggregateVisitor. The AggregateResultContext can be used without knowing the
AggregateKey, the AggregateContext is the "full" version.
*/
template <typename ColumnDataType, AggregateFunction aggregate_function>
struct AggregateResultContext : SegmentVisitorContext {
  using AggregateResultAllocator = PolymorphicAllocator<AggregateResults<ColumnDataType, aggregate_function>>;

  AggregateResultContext() : results(AggregateResultAllocator{&buffer}) {}

  boost::container::pmr::monotonic_buffer_resource buffer;
  AggregateResults<ColumnDataType, aggregate_function> results;
};

template <typename ColumnDataType, AggregateFunction aggregate_function, typename AggregateKey>
struct AggregateContext : public AggregateResultContext<ColumnDataType, aggregate_function> {
  AggregateContext() {
    auto allocator = AggregateResultIdMapAllocator<AggregateKey>{&this->buffer};

    // Unused if AggregateKey == EmptyAggregateKey, but we initialize it anyway to reduce the number of diverging code
    // paths.
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage) - false warning: called C++ object (result_ids) is null
    result_ids = std::make_unique<AggregateResultIdMap<AggregateKey>>(allocator);
  }

  std::unique_ptr<AggregateResultIdMap<AggregateKey>> result_ids;
};

template <typename ColumnDataType, AggregateFunction aggregate_function, typename AggregateKey>
__attribute__((hot)) void AggregateHash::_aggregate_segment(ChunkID chunk_id, ColumnID column_index,
                                                            const AbstractSegment& abstract_segment,
                                                            const KeysPerChunk<AggregateKey>& keys_per_chunk) {
  using AggregateType = typename AggregateTraits<ColumnDataType, aggregate_function>::AggregateType;

  auto aggregator =
      AggregateFunctionBuilder<ColumnDataType, AggregateType, aggregate_function>().get_aggregate_function();

  auto& context = *std::static_pointer_cast<AggregateContext<ColumnDataType, aggregate_function, AggregateKey>>(
      _contexts_per_column[column_index]);

  auto& result_ids = *context.result_ids;
  auto& results = context.results;

  ChunkOffset chunk_offset{0};

  segment_iterate<ColumnDataType>(abstract_segment, [&](const auto& position) {
    auto& result =
        get_or_add_result(result_ids, results, get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                          RowID{chunk_id, chunk_offset});

    /**
    * If the value is NULL, the current aggregate value does not change.
    */
    if (!position.is_null()) {
      if constexpr (aggregate_function == AggregateFunction::CountDistinct) {
        // For the case of CountDistinct, insert the current value into the set to keep track of distinct values
        result.accumulator.emplace(position.value());
      } else {
        aggregator(position.value(), result.aggregate_count, result.accumulator);
      }

      ++result.aggregate_count;
    }

    ++chunk_offset;
  });
}

/**
 * Partition the input chunks by the given group key(s). This is done by creating a vector that contains the
 * AggregateKey for each row. It is gradually built by visitors, one for each group segment.
 */
template <typename AggregateKey>
KeysPerChunk<AggregateKey> AggregateHash::_partition_by_groupby_keys() const {
  KeysPerChunk<AggregateKey> keys_per_chunk;

  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    const auto& input_table = left_input_table();
    const auto chunk_count = input_table->chunk_count();

    // Create the actual data structure
    keys_per_chunk = KeysPerChunk<AggregateKey>{};
    keys_per_chunk.reserve(chunk_count);
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table->get_chunk(chunk_id);
      if (!chunk) continue;

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
    // 0 GROUP BY columns:   No partitioning needed; we don't reach this point because of the check for
    //                       EmptyAggregateKey above
    // 1 GROUP BY column:    The AggregateKey is one dimensional, i.e., the same as AggregateKeyEntry
    // > 1 GROUP BY columns: The AggregateKey is multi-dimensional. The value in
    //                       keys_per_chunk[chunk_id][chunk_offset] is subscripted with the index of the GROUP BY
    //                       columns (not the same as the GROUP BY column_id)
    //
    // To generate a unique identifier, we create a map from the value found in the respective GROUP BY column to
    // a unique uint64_t. The value 0 is reserved for NULL.
    //
    // This has the cost of a hashmap lookup and potential insert for each row and each GROUP BY column. There are
    // some cases in which we can avoid this. These make use of the fact that we can only have 2^64 - 2*2^32 values
    // in a table (due to INVALID_VALUE_ID and INVALID_CHUNK_OFFSET limiting the range of RowIDs).
    //
    // (1) For types smaller than AggregateKeyEntry, such as int32_t, their value range can be immediately mapped into
    //     uint64_t. We cannot do the same for int64_t because we need to account for NULL values.
    // (2) For strings not longer than five characters, there are 1+2^(1*8)+2^(2*8)+2^(3*8)+2^(4*8) potential values.
    //     We can immediately map these into a numerical representation by reinterpreting their byte storage as an
    //     integer. The calculation is described below. Note that this is done on a per-string basis and does not
    //     require all strings in the given column to be that short.
    std::vector<std::shared_ptr<AbstractTask>> jobs;
    jobs.reserve(_groupby_column_ids.size());

    for (size_t group_column_index = 0; group_column_index < _groupby_column_ids.size(); ++group_column_index) {
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
                  if (position.is_null()) {
                    keys[chunk_offset] = 0;
                  } else {
                    keys[chunk_offset] = int_to_uint(position.value()) + 1;
                  }
                } else {
                  if (position.is_null()) {
                    keys[chunk_offset][group_column_index] = 0;
                  } else {
                    keys[chunk_offset][group_column_index] = int_to_uint(position.value()) + 1;
                  }
                }
                ++chunk_offset;
              });
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

            auto id_map = tsl::robin_map<ColumnDataType, AggregateKeyEntry, std::hash<ColumnDataType>, std::equal_to<>,
                                         decltype(allocator)>(allocator);
            AggregateKeyEntry id_counter = 1u;

            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              // We store strings shorter than five characters without using the id_map. For that, we need to reserve
              // the IDs used for short strings (see below).
              id_counter = 5'000'000'000;
            }

            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto chunk_in = input_table->get_chunk(chunk_id);
              if (!chunk_in) continue;

              auto& keys = keys_per_chunk[chunk_id];

              const auto abstract_segment = chunk_in->get_segment(groupby_column_id);
              ChunkOffset chunk_offset{0};
              segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& position) {
                if (position.is_null()) {
                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys[chunk_offset] = 0u;
                  } else {
                    keys[chunk_offset][group_column_index] = 0u;
                  }
                } else {
                  // We need to generate an ID that is unique for the value. In some cases, we can use an optimization,
                  // in others, we can't. We need to somehow track whether we have found an ID or not. For this, we
                  // first set `id` to its maximum value. If after all branches it is still that max value, no optimized
                  // ID generation was applied and we need to generate the ID using the value->ID map.
                  auto id = std::numeric_limits<AggregateKeyEntry>::max();

                  if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
                    const auto& string = position.value();
                    if (string.size() < 5) {
                      static_assert(std::is_same_v<AggregateKeyEntry, uint64_t>, "Calculation only valid for uint64_t");

                      const auto char_to_uint = [](const char in, const uint bits) {
                        // chars may be signed or unsigned. For the calculation as described below, we need signed
                        // chars.
                        return static_cast<uint64_t>(*reinterpret_cast<const uint8_t*>(&in)) << bits;
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
                          id = uint64_t{1};
                        } break;

                        case 1: {
                          id = uint64_t{2} + char_to_uint(string[0], 0);
                        } break;

                        case 2: {
                          id = uint64_t{258} + char_to_uint(string[1], 8) + char_to_uint(string[0], 0);
                        } break;

                        case 3: {
                          id = uint64_t{65'794} + char_to_uint(string[2], 16) + char_to_uint(string[1], 8) +
                               char_to_uint(string[0], 0);
                        } break;

                        case 4: {
                          id = uint64_t{16'843'010} + char_to_uint(string[3], 24) + char_to_uint(string[2], 16) +
                               char_to_uint(string[1], 8) + char_to_uint(string[0], 0);
                        } break;
                      }
                    }
                  }

                  if (id == std::numeric_limits<AggregateKeyEntry>::max()) {
                    // Could not take the shortcut above, either because we don't have a string or because it is too
                    // long
                    auto inserted = id_map.try_emplace(position.value(), id_counter);

                    id = inserted.first->second;

                    // if the id_map didn't have the value as a key and a new element was inserted
                    if (inserted.second) ++id_counter;
                  }

                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys[chunk_offset] = id;
                  } else {
                    keys[chunk_offset][group_column_index] = id;
                  }
                }

                ++chunk_offset;
              });
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
      Assert(groupby_column_id < input_table->column_count(), "GroupBy column index out of bounds");
    }
  }

  // Check for invalid aggregates
  _validate_aggregates();

  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  Timer timer;

  /**
   * PARTITIONING STEP
   */
  const auto keys_per_chunk = _partition_by_groupby_keys<AggregateKey>();
  step_performance_data.set_step_runtime(OperatorSteps::GroupByKeyPartitioning, timer.lap());

  /**
   * AGGREGATION STEP
   */
  _contexts_per_column = std::vector<std::shared_ptr<SegmentVisitorContext>>(_aggregates.size());

  if (!_has_aggregate_functions) {
    /*
    Insert a dummy context for the DISTINCT implementation.
    That way, _contexts_per_column will always have at least one context with results.
    This is important later on when we write the group keys into the table.
    The template parameters (int32_t, AggregateFunction::Min) do not matter, as we do not calculate an aggregate anyway.
    */
    auto context = std::make_shared<AggregateContext<int32_t, AggregateFunction::Min, AggregateKey>>();
    _contexts_per_column.push_back(context);
  }

  /**
   * Create an AggregateContext for each column in the input table that a normal (i.e. non-DISTINCT) aggregate is
   * created on. We do this here, and not in the per-chunk-loop below, because there might be no Chunks in the input
   * and _write_aggregate_output() needs these contexts anyway.
   */
  for (ColumnID aggregate_idx{0}; aggregate_idx < _aggregates.size(); ++aggregate_idx) {
    const auto& aggregate = _aggregates[aggregate_idx];

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    if (input_column_id == INVALID_COLUMN_ID) {
      Assert(aggregate->aggregate_function == AggregateFunction::Count, "Only COUNT may have an invalid ColumnID");
      // SELECT COUNT(*) - we know the template arguments, so we don't need a visitor
      auto context = std::make_shared<AggregateContext<CountColumnType, AggregateFunction::Count, AggregateKey>>();
      _contexts_per_column[aggregate_idx] = context;
      continue;
    }
    const auto data_type = input_table->column_data_type(input_column_id);
    _contexts_per_column[aggregate_idx] =
        _create_aggregate_context<AggregateKey>(data_type, aggregate->aggregate_function);
  }

  // Process Chunks and perform aggregations
  const auto chunk_count = input_table->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk_in = input_table->get_chunk(chunk_id);
    if (!chunk_in) continue;

    // Sometimes, gcc is really bad at accessing loop conditions only once, so we cache that here.
    const auto input_chunk_size = chunk_in->size();

    if (!_has_aggregate_functions) {
      /**
       * DISTINCT implementation
       *
       * In Opossum we handle the SQL keyword DISTINCT by using an aggregate operator with grouping but without 
       * aggregate functions. All input columns (either explicitly specified as `SELECT DISTINCT a, b, c` OR implicitly
       * as `SELECT DISTINCT *` are passed as `groupby_column_ids`).
       *
       * As the grouping happens as part of the aggregation but no aggregate function exists , we use
       * `AggregateFunction::Min` as a fake aggregate function whose result will be discarded. From here on, the steps
       * are the same as they are for a regular grouped aggregate.
       */

      auto context =
          std::static_pointer_cast<AggregateContext<DistinctColumnType, AggregateFunction::Min, AggregateKey>>(
              _contexts_per_column[0]);

      auto& result_ids = *context->result_ids;
      auto& results = context->results;

      for (ChunkOffset chunk_offset{0}; chunk_offset < input_chunk_size; chunk_offset++) {
        // Make sure the value or combination of values is added to the list of distinct value(s)
        get_or_add_result(result_ids, results, get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                          RowID{chunk_id, chunk_offset});
      }
    } else {
      ColumnID aggregate_idx{0};
      for (const auto& aggregate : _aggregates) {
        /**
         * Special COUNT(*) implementation.
         * Because COUNT(*) does not have a specific target column, we use the maximum ColumnID.
         * We then go through the keys_per_chunk map and count the occurrences of each group key.
         * The results are saved in the regular aggregate_count variable so that we don't need a
         * specific output logic for COUNT(*).
         */

        const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
        const auto input_column_id = pqp_column.column_id;

        if (input_column_id == INVALID_COLUMN_ID) {
          Assert(aggregate->aggregate_function == AggregateFunction::Count, "Only COUNT may have an invalid ColumnID");
          auto context =
              std::static_pointer_cast<AggregateContext<CountColumnType, AggregateFunction::Count, AggregateKey>>(
                  _contexts_per_column[aggregate_idx]);

          auto& result_ids = *context->result_ids;
          auto& results = context->results;

          if constexpr (std::is_same_v<AggregateKey, EmptyAggregateKey>) {
            // Not grouped by anything, simply count the number of rows
            results.resize(1);
            results[0].aggregate_count += input_chunk_size;
          } else {
            // count occurrences for each group key
            for (ChunkOffset chunk_offset{0}; chunk_offset < input_chunk_size; chunk_offset++) {
              auto& result = get_or_add_result(result_ids, results,
                                               get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                                               RowID{chunk_id, chunk_offset});
              ++result.aggregate_count;
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

          switch (aggregate->aggregate_function) {
            case AggregateFunction::Min:
              _aggregate_segment<ColumnDataType, AggregateFunction::Min, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::Max:
              _aggregate_segment<ColumnDataType, AggregateFunction::Max, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::Sum:
              _aggregate_segment<ColumnDataType, AggregateFunction::Sum, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::Avg:
              _aggregate_segment<ColumnDataType, AggregateFunction::Avg, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::Count:
              _aggregate_segment<ColumnDataType, AggregateFunction::Count, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::CountDistinct:
              _aggregate_segment<ColumnDataType, AggregateFunction::CountDistinct, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::StandardDeviationSample:
              _aggregate_segment<ColumnDataType, AggregateFunction::StandardDeviationSample, AggregateKey>(
                  chunk_id, aggregate_idx, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::Any:
              // ANY is a pseudo-function and is handled by _write_groupby_output
              break;
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
      // No need for a complex data structure if we only have one entry
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
  _output_segments.resize(num_output_columns);

  /**
   * If only GROUP BY columns (including ANY pseudo-aggregates) are written, we need to call _write_groupby_output.
   *   Example: SELECT c_custkey, c_name FROM customer GROUP BY c_custkey, c_name (same as SELECT DISTINCT), which
   *            is rewritten to group only on c_custkey and collect c_name as an ANY pseudo-aggregate.
   * Otherwise, it is called by the first call to _write_aggregate_output.
   **/
  if (!_has_aggregate_functions) {
    auto context = std::static_pointer_cast<AggregateResultContext<DistinctColumnType, AggregateFunction::Min>>(
        _contexts_per_column[0]);
    auto pos_list = RowIDPosList();
    pos_list.reserve(context->results.size());
    for (const auto& result : context->results) {
      pos_list.push_back(result.row_id);
    }
    _write_groupby_output(pos_list);
  }

  // _aggregate and _write_groupby_output have their own, internal timer. Start measuring once they are done.
  auto& step_performance_data = static_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  Timer timer;

  /*
  Write the aggregated columns to the output
  */
  const auto& input_table = left_input_table();
  ColumnID aggregate_idx{0};
  for (const auto& aggregate : _aggregates) {
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    // Output column for COUNT(*).
    const auto data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);

    resolve_data_type(data_type, [&, aggregate_idx](auto type) {
      _write_aggregate_output(type, aggregate_idx, aggregate->aggregate_function);
    });

    ++aggregate_idx;
  }
  step_performance_data.set_step_runtime(OperatorSteps::AggregateColumnsWriting, timer.lap());

  // Write the output
  auto output = std::make_shared<Table>(_output_column_definitions, TableType::Data);
  if (_output_segments.at(0)->size() > 0) {
    output->append_chunk(_output_segments);
  }

  step_performance_data.set_step_runtime(OperatorSteps::OutputWriting, timer.lap());

  return output;
}

/*
The following template functions write the aggregated values for the different aggregate functions.
They are separate and templated to avoid compiler errors for invalid type/function combinations.
*/
// MIN, MAX, SUM, ANY write the current aggregated value
template <typename ColumnDataType, typename AggregateType, AggregateFunction aggregate_func>
std::enable_if_t<aggregate_func == AggregateFunction::Min || aggregate_func == AggregateFunction::Max ||
                     aggregate_func == AggregateFunction::Sum || aggregate_func == AggregateFunction::Any,
                 void>
write_aggregate_values(pmr_vector<AggregateType>& values, pmr_vector<bool>& null_values,
                       const AggregateResults<ColumnDataType, aggregate_func>& results) {
  values.resize(results.size());
  null_values.resize(results.size());

  size_t output_offset = 0;
  for (const auto& result : results) {
    if (result.aggregate_count > 0) {
      values[output_offset] = result.accumulator;
    } else {
      null_values[output_offset] = true;
    }

    ++output_offset;
  }
}

// COUNT writes the aggregate counter
template <typename ColumnDataType, typename AggregateType, AggregateFunction aggregate_func>
std::enable_if_t<aggregate_func == AggregateFunction::Count, void> write_aggregate_values(
    pmr_vector<AggregateType>& values, pmr_vector<bool>& null_values,
    const AggregateResults<ColumnDataType, aggregate_func>& results) {
  values.resize(results.size());

  size_t output_offset = 0;
  for (const auto& result : results) {
    values[output_offset] = result.aggregate_count;
    ++output_offset;
  }
}

// COUNT(DISTINCT) writes the number of distinct values
template <typename ColumnDataType, typename AggregateType, AggregateFunction aggregate_func>
std::enable_if_t<aggregate_func == AggregateFunction::CountDistinct, void> write_aggregate_values(
    pmr_vector<AggregateType>& values, pmr_vector<bool>& null_values,
    const AggregateResults<ColumnDataType, aggregate_func>& results) {
  values.resize(results.size());

  size_t output_offset = 0;
  for (const auto& result : results) {
    values[output_offset] = result.accumulator.size();
    ++output_offset;
  }
}

// AVG writes the calculated average from current aggregate and the aggregate counter
template <typename ColumnDataType, typename AggregateType, AggregateFunction aggregate_func>
std::enable_if_t<aggregate_func == AggregateFunction::Avg && std::is_arithmetic_v<AggregateType>, void>
write_aggregate_values(pmr_vector<AggregateType>& values, pmr_vector<bool>& null_values,
                       const AggregateResults<ColumnDataType, aggregate_func>& results) {
  values.resize(results.size());
  null_values.resize(results.size());

  auto output_offset = ChunkOffset{0};
  for (const auto& result : results) {
    if (result.aggregate_count > 0) {
      values[output_offset] = result.accumulator / static_cast<AggregateType>(result.aggregate_count);
    } else {
      null_values[output_offset] = true;
    }
    ++output_offset;
  }
}

// AVG is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnDataType, typename AggregateType, AggregateFunction aggregate_func>
std::enable_if_t<aggregate_func == AggregateFunction::Avg && !std::is_arithmetic_v<AggregateType>, void>
write_aggregate_values(pmr_vector<AggregateType>& values, pmr_vector<bool>& null_values,
                       const AggregateResults<ColumnDataType, aggregate_func>& results) {
  Fail("Invalid aggregate");
}

// STDDEV_SAMP writes the calculated standard deviation from current aggregate and the aggregate counter
template <typename ColumnDataType, typename AggregateType, AggregateFunction aggregate_func>
std::enable_if_t<aggregate_func == AggregateFunction::StandardDeviationSample && std::is_arithmetic_v<AggregateType>,
                 void>
write_aggregate_values(pmr_vector<AggregateType>& values, pmr_vector<bool>& null_values,
                       const AggregateResults<ColumnDataType, aggregate_func>& results) {
  values.resize(results.size());
  null_values.resize(results.size());

  auto output_offset = ChunkOffset{0};
  for (const auto& result : results) {
    if (result.aggregate_count > 1) {
      values[output_offset] = result.accumulator[3];
    } else {
      // STDDEV_SAMP is undefined for lists with less than two elements
      null_values[output_offset] = true;
    }
    ++output_offset;
  }
}

// STDDEV_SAMP is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnDataType, typename AggregateType, AggregateFunction aggregate_func>
std::enable_if_t<aggregate_func == AggregateFunction::StandardDeviationSample && !std::is_arithmetic_v<AggregateType>,
                 void>
write_aggregate_values(pmr_vector<AggregateType>& values, pmr_vector<bool>& null_values,
                       const AggregateResults<ColumnDataType, aggregate_func>& results) {
  Fail("Invalid aggregate");
}

void AggregateHash::_write_groupby_output(RowIDPosList& pos_list) {
  auto& step_performance_data = static_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  Timer timer;  // _aggregate above has its own, internal timer. Start measuring once _aggregate is done.

  auto input_table = left_input_table();

  auto unaggregated_columns = std::vector<std::pair<ColumnID, ColumnID>>{};
  {
    auto output_column_id = ColumnID{0};
    for (const auto& input_column_id : _groupby_column_ids) {
      unaggregated_columns.emplace_back(input_column_id, output_column_id);
      ++output_column_id;
    }
    for (const auto& aggregate : _aggregates) {
      if (aggregate->aggregate_function == AggregateFunction::Any) {
        const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
        const auto input_column_id = pqp_column.column_id;
        unaggregated_columns.emplace_back(input_column_id, output_column_id);
      }
      ++output_column_id;
    }
  }

  // For each GROUP BY column, resolve its type, iterate over its values, and add them to a new output ValueSegment
  for (const auto& unaggregated_column : unaggregated_columns) {
    // Structured bindings do not work with the capture below :/
    const auto input_column_id = unaggregated_column.first;
    const auto output_column_id = unaggregated_column.second;

    _output_column_definitions[output_column_id] =
        TableColumnDefinition{input_table->column_name(input_column_id), input_table->column_data_type(input_column_id),
                              input_table->column_is_nullable(input_column_id)};

    resolve_data_type(input_table->column_data_type(input_column_id), [&](const auto typed_value) {
      using ColumnDataType = typename decltype(typed_value)::type;

      const auto column_is_nullable = input_table->column_is_nullable(input_column_id);

      auto values = pmr_vector<ColumnDataType>(pos_list.size());
      auto null_values = pmr_vector<bool>(column_is_nullable ? pos_list.size() : 0);
      std::vector<std::unique_ptr<AbstractSegmentAccessor<ColumnDataType>>> accessors(input_table->chunk_count());

      auto output_offset = ChunkOffset{0};

      for (const auto& row_id : pos_list) {
        // pos_list was generated by grouping the input data. While it might point to rows that contain NULL
        // values, no new NULL values should have been added.
        DebugAssert(!row_id.is_null(), "Did not expect NULL value here");

        auto& accessor = accessors[row_id.chunk_id];
        if (!accessor) {
          accessor = create_segment_accessor<ColumnDataType>(
              input_table->get_chunk(row_id.chunk_id)->get_segment(input_column_id));
        }

        const auto& optional_value = accessor->access(row_id.chunk_offset);
        DebugAssert(optional_value || column_is_nullable, "Only nullable columns should contain optional values");
        if (!optional_value) {
          null_values[output_offset] = true;
        } else {
          values[output_offset] = *optional_value;
        }
        ++output_offset;
      }

      auto value_segment = std::shared_ptr<ValueSegment<ColumnDataType>>{};
      if (column_is_nullable) {
        value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
      } else {
        value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
      }

      _output_segments[output_column_id] = value_segment;
    });
  }

  step_performance_data.set_step_runtime(OperatorSteps::GroupByColumnsWriting, timer.lap());
}

template <typename ColumnDataType>
void AggregateHash::_write_aggregate_output(boost::hana::basic_type<ColumnDataType> type, ColumnID column_index,
                                            AggregateFunction aggregate_function) {
  switch (aggregate_function) {
    case AggregateFunction::Min:
      write_aggregate_output<ColumnDataType, AggregateFunction::Min>(column_index);
      break;
    case AggregateFunction::Max:
      write_aggregate_output<ColumnDataType, AggregateFunction::Max>(column_index);
      break;
    case AggregateFunction::Sum:
      write_aggregate_output<ColumnDataType, AggregateFunction::Sum>(column_index);
      break;
    case AggregateFunction::Avg:
      write_aggregate_output<ColumnDataType, AggregateFunction::Avg>(column_index);
      break;
    case AggregateFunction::Count:
      write_aggregate_output<ColumnDataType, AggregateFunction::Count>(column_index);
      break;
    case AggregateFunction::CountDistinct:
      write_aggregate_output<ColumnDataType, AggregateFunction::CountDistinct>(column_index);
      break;
    case AggregateFunction::StandardDeviationSample:
      write_aggregate_output<ColumnDataType, AggregateFunction::StandardDeviationSample>(column_index);
      break;
    case AggregateFunction::Any:
      // written by _write_groupby_output
      break;
  }
}

template <typename ColumnDataType, AggregateFunction aggregate_function>
void AggregateHash::write_aggregate_output(ColumnID aggregate_index) {
  // retrieve type information from the aggregation traits
  typename AggregateTraits<ColumnDataType, aggregate_function>::AggregateType aggregate_type;
  auto aggregate_data_type = AggregateTraits<ColumnDataType, aggregate_function>::AGGREGATE_DATA_TYPE;

  const auto& aggregate = _aggregates[aggregate_index];

  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  const auto input_column_id = pqp_column.column_id;

  if (aggregate_data_type == DataType::Null) {
    // if not specified, it’s the input column’s type
    aggregate_data_type = left_input_table()->column_data_type(input_column_id);
  }

  auto context = std::static_pointer_cast<AggregateResultContext<ColumnDataType, aggregate_function>>(
      _contexts_per_column[aggregate_index]);

  const auto& results = context->results;

  // Before writing the first aggregate column, write all group keys into the respective columns
  if (aggregate_index == 0) {
    auto pos_list = RowIDPosList(context->results.size());
    auto chunk_offset = ChunkOffset{0};
    for (auto& result : context->results) {
      pos_list[chunk_offset] = (result.row_id);
      ++chunk_offset;
    }
    _write_groupby_output(pos_list);
  }

  // Write aggregated values into the segment. While write_aggregate_values could track if an actual NULL value was
  // written or not, we rather make the output types consistent independent of the input types. Not sure what the
  // standard says about this.
  auto values = pmr_vector<decltype(aggregate_type)>{};
  auto null_values = pmr_vector<bool>{};

  constexpr bool NEEDS_NULL =
      (aggregate_function != AggregateFunction::Count && aggregate_function != AggregateFunction::CountDistinct);

  if (!results.empty()) {
    write_aggregate_values<ColumnDataType, decltype(aggregate_type), aggregate_function>(values, null_values, results);
  } else if (_groupby_column_ids.empty()) {
    // If we did not GROUP BY anything and we have no results, we need to add NULL for most aggregates and 0 for count
    values.push_back(decltype(aggregate_type){});
    if (NEEDS_NULL) {
      null_values.push_back(true);
    }
  }

  DebugAssert(NEEDS_NULL || null_values.empty(), "write_aggregate_values unexpectedly wrote NULL values");
  const auto output_column_id = _groupby_column_ids.size() + aggregate_index;
  _output_column_definitions[output_column_id] =
      TableColumnDefinition{aggregate->as_column_name(), aggregate_data_type, NEEDS_NULL};

  auto output_segment = std::shared_ptr<ValueSegment<decltype(aggregate_type)>>{};
  if (!NEEDS_NULL) {
    output_segment = std::make_shared<ValueSegment<decltype(aggregate_type)>>(std::move(values));
  } else {
    output_segment =
        std::make_shared<ValueSegment<decltype(aggregate_type)>>(std::move(values), std::move(null_values));
  }
  _output_segments[output_column_id] = output_segment;
}

template <typename AggregateKey>
std::shared_ptr<SegmentVisitorContext> AggregateHash::_create_aggregate_context(
    const DataType data_type, const AggregateFunction aggregate_function) const {
  std::shared_ptr<SegmentVisitorContext> context;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    switch (aggregate_function) {
      case AggregateFunction::Min:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::Min, AggregateKey>>();
        break;
      case AggregateFunction::Max:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::Max, AggregateKey>>();
        break;
      case AggregateFunction::Sum:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::Sum, AggregateKey>>();
        break;
      case AggregateFunction::Avg:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::Avg, AggregateKey>>();
        break;
      case AggregateFunction::Count:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::Count, AggregateKey>>();
        break;
      case AggregateFunction::CountDistinct:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::CountDistinct, AggregateKey>>();
        break;
      case AggregateFunction::StandardDeviationSample:
        context = std::make_shared<
            AggregateContext<ColumnDataType, AggregateFunction::StandardDeviationSample, AggregateKey>>();
        break;
      case AggregateFunction::Any:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::Any, AggregateKey>>();
        break;
    }
  });
  return context;
}

}  // namespace opossum
