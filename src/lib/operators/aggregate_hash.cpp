#include "aggregate_hash.hpp"

#include <cmath>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container/pmr/monotonic_buffer_resource.hpp>

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
    : AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

const std::string& AggregateHash::name() const {
  static const auto name = std::string{"AggregateHash"};
  return name;
}

std::shared_ptr<AbstractOperator> AggregateHash::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<AggregateHash>(copied_input_left, _aggregates, _groupby_column_ids);
}

void AggregateHash::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateHash::_on_cleanup() { _contexts_per_column.clear(); }

/*
Visitor context for the AggregateVisitor. The AggregateResultContext can be used without knowing the
AggregateKey, the AggregateContext is the "full" version.
*/
template <typename ColumnDataType, typename AggregateType>
struct AggregateResultContext : SegmentVisitorContext {
  using AggregateResultAllocator = PolymorphicAllocator<AggregateResults<ColumnDataType, AggregateType>>;

  AggregateResultContext() : results(AggregateResultAllocator{&buffer}) {}

  boost::container::pmr::monotonic_buffer_resource buffer;
  AggregateResults<ColumnDataType, AggregateType> results;
};

template <typename ColumnDataType, typename AggregateType, typename AggregateKey>
struct AggregateContext : public AggregateResultContext<ColumnDataType, AggregateType> {
  AggregateContext() {
    auto allocator = AggregateResultIdMapAllocator<AggregateKey>{&this->buffer};

    // Unused if AggregateKey == EmptyAggregateKey, but we initialize it anyway to reduce the number of diverging code
    // paths.
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage) - false warning: called C++ object (result_ids) is null
    result_ids = std::make_unique<AggregateResultIdMap<AggregateKey>>(allocator);
  }

  std::unique_ptr<AggregateResultIdMap<AggregateKey>> result_ids;
};

template <typename ColumnDataType, AggregateFunction function, typename AggregateKey>
void AggregateHash::_aggregate_segment(ChunkID chunk_id, ColumnID column_index, const BaseSegment& base_segment,
                                       const KeysPerChunk<AggregateKey>& keys_per_chunk) {
  using AggregateType = typename AggregateTraits<ColumnDataType, function>::AggregateType;

  auto aggregator = AggregateFunctionBuilder<ColumnDataType, AggregateType, function>().get_aggregate_function();

  auto& context = *std::static_pointer_cast<AggregateContext<ColumnDataType, AggregateType, AggregateKey>>(
      _contexts_per_column[column_index]);

  auto& result_ids = *context.result_ids;
  auto& results = context.results;

  ChunkOffset chunk_offset{0};

  segment_iterate<ColumnDataType>(base_segment, [&](const auto& position) {
    auto& result =
        get_or_add_result(result_ids, results, get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                          RowID{chunk_id, chunk_offset});

    /**
    * If the value is NULL, the current aggregate value does not change.
    */
    if (!position.is_null()) {
      // If we have a value, use the aggregator lambda to update the current aggregate value for this group
      aggregator(position.value(), result.current_primary_aggregate, result.current_secondary_aggregates);

      if constexpr (function == AggregateFunction::Avg || function == AggregateFunction::Count ||
                    function == AggregateFunction::StandardDeviationSample) {  // NOLINT
        // Increase the counter of non-NULL values only for aggregation functions that use it.
        ++result.aggregate_count;
      }

      if constexpr (function == AggregateFunction::CountDistinct) {  // NOLINT
        // clang-tidy error: https://bugs.llvm.org/show_bug.cgi?id=35824
        // for the case of CountDistinct, insert this value into the set to keep track of distinct values
        result.distinct_values.insert(position.value());
      }
    }

    ++chunk_offset;
  });
}

template <typename AggregateKey>
void AggregateHash::_aggregate() {
  // We use monotonic_buffer_resource for the vector of vectors that hold the aggregate keys. That is so that we can
  // save time when allocating and we can throw away everything in this temporary structure at once (once the resource
  // gets deleted). Also, we use the scoped_allocator_adaptor to propagate the allocator to all inner vectors.
  // This is suitable here because the amount of memory needed is known from the start. In other places with frequent
  // reallocations, this might make less sense.
  // We use boost over std because libc++ does not yet (July 2018) support monotonic_buffer_resource:
  // https://libcxx.llvm.org/ts1z_status.html
  using AggregateKeysAllocator =
      boost::container::scoped_allocator_adaptor<PolymorphicAllocator<AggregateKeys<AggregateKey>>>;

  auto input_table = input_table_left();

  for ([[maybe_unused]] const auto& groupby_column_id : _groupby_column_ids) {
    DebugAssert(groupby_column_id < input_table->column_count(), "GroupBy column index out of bounds");
  }

  // Check for invalid aggregates
  _validate_aggregates();

  KeysPerChunk<AggregateKey> keys_per_chunk;

  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {  // NOLINT
    /*
    PARTITIONING PHASE
    First we partition the input chunks by the given group key(s).
    This is done by creating a vector that contains the AggregateKey for each row.
    It is gradually built by visitors, one for each group segment.
    */

    const auto chunk_count = input_table->chunk_count();

    {
      // Allocate a temporary memory buffer, for more details see aggregate_hash.hpp
      // This calculation assumes that we use std::vector<AggregateKeyEntry> - other data structures use less space, but
      // that is fine
      size_t needed_size_per_aggregate_key =
          aligned_size<AggregateKey>() + _groupby_column_ids.size() * aligned_size<AggregateKeyEntry>();
      size_t needed_size = aligned_size<KeysPerChunk<AggregateKey>>() +
                           chunk_count * aligned_size<AggregateKeys<AggregateKey>>() +
                           input_table->row_count() * needed_size_per_aggregate_key;
      needed_size = static_cast<size_t>(needed_size * 1.1);  // Give it a little bit more, just in case

      auto temp_buffer = boost::container::pmr::monotonic_buffer_resource(needed_size);
      auto allocator = AggregateKeysAllocator{PolymorphicAllocator<AggregateKeys<AggregateKey>>{&temp_buffer}};
      allocator.allocate(1);  // Make sure that the buffer is initialized
      const auto start_next_buffer_size = temp_buffer.next_buffer_size();

      // Create the actual data structure
      keys_per_chunk = KeysPerChunk<AggregateKey>{allocator};
      keys_per_chunk.reserve(chunk_count);
      for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = input_table->get_chunk(chunk_id);
        if (!chunk) continue;

        if constexpr (std::is_same_v<AggregateKey, std::vector<AggregateKeyEntry>>) {
          keys_per_chunk.emplace_back(chunk->size(), AggregateKey(_groupby_column_ids.size()));
        } else {
          keys_per_chunk.emplace_back(chunk->size(), AggregateKey{});
        }
      }

      // Make sure that we did not have to allocate more memory than originally computed
      if (temp_buffer.next_buffer_size() != start_next_buffer_size) {
        // The buffer sizes are increasing when the current buffer is full. We can use this to make sure that we
        // allocated enough space from the beginning on. It would be more intuitive to compare current_buffer(), but
        // this seems to be broken in boost: https://svn.boost.org/trac10/ticket/13639#comment:1
        PerformanceWarning(std::string("needed_size ") + std::to_string(needed_size) +
                           " was not enough and a second buffer was needed");
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
              const auto base_segment = chunk_in->get_segment(groupby_column_id);
              ChunkOffset chunk_offset{0};
              segment_iterate<ColumnDataType>(*base_segment, [&](const auto& position) {
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
                    keys_per_chunk[chunk_id][chunk_offset] = 0;
                  } else {
                    keys_per_chunk[chunk_id][chunk_offset] = int_to_uint(position.value()) + 1;
                  }
                } else {
                  if (position.is_null()) {
                    keys_per_chunk[chunk_id][chunk_offset][group_column_index] = 0;
                  } else {
                    keys_per_chunk[chunk_id][chunk_offset][group_column_index] = int_to_uint(position.value()) + 1;
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

            auto id_map = std::unordered_map<ColumnDataType, AggregateKeyEntry, std::hash<ColumnDataType>,
                                             std::equal_to<>, decltype(allocator)>(allocator);
            AggregateKeyEntry id_counter = 1u;

            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {  // NOLINT
              // We store strings shorter than five characters without using the id_map. For that, we need to reserve
              // the IDs used for short strings (see below).
              id_counter = 5'000'000'000;
            }

            for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto chunk_in = input_table->get_chunk(chunk_id);
              if (!chunk_in) continue;

              const auto base_segment = chunk_in->get_segment(groupby_column_id);
              ChunkOffset chunk_offset{0};
              segment_iterate<ColumnDataType>(*base_segment, [&](const auto& position) {
                if (position.is_null()) {
                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys_per_chunk[chunk_id][chunk_offset] = 0u;
                  } else {
                    keys_per_chunk[chunk_id][chunk_offset][group_column_index] = 0u;
                  }
                } else {
                  // We need to generate an ID that is unique for the value. In some cases, we can use an optimization,
                  // in others, we can't. We need to somehow track whether we have found an ID or not. For this, we
                  // first set `id` to its maximum value. If after all branches it is still that max value, no optimized
                  // ID generation was applied and we need to generate the ID using the value->ID map.
                  auto id = std::numeric_limits<AggregateKeyEntry>::max();

                  if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {  // NOLINT
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
                    keys_per_chunk[chunk_id][chunk_offset] = id;
                  } else {
                    keys_per_chunk[chunk_id][chunk_offset][group_column_index] = id;
                  }
                }

                ++chunk_offset;
              });
            }
          }
        });
      }));
      jobs.back()->schedule();
    }

    Hyrise::get().scheduler()->wait_for_tasks(jobs);
  }

  /*
  AGGREGATION PHASE
  */
  _contexts_per_column = std::vector<std::shared_ptr<SegmentVisitorContext>>(_aggregates.size());

  if (_aggregates.empty()) {
    /*
    Insert a dummy context for the DISTINCT implementation.
    That way, _contexts_per_column will always have at least one context with results.
    This is important later on when we write the group keys into the table.

    We choose int8_t for column type and aggregate type because it's small.
    */
    auto context = std::make_shared<AggregateContext<DistinctColumnType, DistinctAggregateType, AggregateKey>>();
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
      auto context = std::make_shared<AggregateContext<CountColumnType, CountAggregateType, AggregateKey>>();
      _contexts_per_column[aggregate_idx] = context;
      continue;
    }
    auto data_type = input_table->column_data_type(input_column_id);
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

    if (_aggregates.empty()) {
      /**
       * DISTINCT implementation
       *
       * In Opossum we handle the SQL keyword DISTINCT by grouping without aggregation.
       *
       * For a query like "SELECT DISTINCT * FROM A;"
       * we would assume that all columns from A are part of 'groupby_columns',
       * respectively any columns that were specified in the projection.
       * The optimizer is responsible to take care of passing in the correct columns.
       *
       * How does this operation work?
       * Distinct rows are retrieved by grouping by vectors of values. Similar as for the usual aggregation
       * these vectors are used as keys in the 'column_results' map.
       *
       * At this point we've got all the different keys from the chunks and accumulate them in 'column_results'.
       * In order to reuse the aggregation implementation, we add a dummy AggregateResult.
       * One could optimize here in the future.
       *
       * Obviously this implementation is also used for plain GroupBy's.
       */

      auto context =
          std::static_pointer_cast<AggregateContext<DistinctColumnType, DistinctAggregateType, AggregateKey>>(
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
          auto context = std::static_pointer_cast<AggregateContext<CountColumnType, CountAggregateType, AggregateKey>>(
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

        auto base_segment = chunk_in->get_segment(input_column_id);
        auto data_type = input_table->column_data_type(input_column_id);

        /*
        Invoke correct aggregator for each segment
        */

        resolve_data_type(data_type, [&, aggregate](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          switch (aggregate->aggregate_function) {
            case AggregateFunction::Min:
              _aggregate_segment<ColumnDataType, AggregateFunction::Min, AggregateKey>(chunk_id, aggregate_idx,
                                                                                       *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Max:
              _aggregate_segment<ColumnDataType, AggregateFunction::Max, AggregateKey>(chunk_id, aggregate_idx,
                                                                                       *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Sum:
              _aggregate_segment<ColumnDataType, AggregateFunction::Sum, AggregateKey>(chunk_id, aggregate_idx,
                                                                                       *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Avg:
              _aggregate_segment<ColumnDataType, AggregateFunction::Avg, AggregateKey>(chunk_id, aggregate_idx,
                                                                                       *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Count:
              _aggregate_segment<ColumnDataType, AggregateFunction::Count, AggregateKey>(chunk_id, aggregate_idx,
                                                                                         *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::CountDistinct:
              _aggregate_segment<ColumnDataType, AggregateFunction::CountDistinct, AggregateKey>(
                  chunk_id, aggregate_idx, *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::StandardDeviationSample:
              _aggregate_segment<ColumnDataType, AggregateFunction::StandardDeviationSample, AggregateKey>(
                  chunk_id, aggregate_idx, *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Any:
              _aggregate_segment<ColumnDataType, AggregateFunction::Any, AggregateKey>(chunk_id, aggregate_idx,
                                                                                       *base_segment, keys_per_chunk);
          }
        });

        ++aggregate_idx;
      }
    }
  }
}

std::shared_ptr<const Table> AggregateHash::_on_execute() {
  // We do not want the overhead of a vector with heap storage when we have a limited number of aggregate columns.
  // The reason we only have specializations up to 2 is because every specialization increases the compile time.
  // Also, we need to make sure that there are tests for at least the first case, one array case, and the fallback.
  switch (_groupby_column_ids.size()) {
    case 0:
      _aggregate<EmptyAggregateKey>();
      break;
    case 1:
      // No need for a complex data structure if we only have one entry
      _aggregate<AggregateKeyEntry>();
      break;
    case 2:
      // We need to explicitly list all array sizes that we want to support
      _aggregate<std::array<AggregateKeyEntry, 2>>();
      break;
    default:
      PerformanceWarning("No std::array implementation initialized - falling back to vector");
      _aggregate<std::vector<AggregateKeyEntry>>();
      break;
  }

  const auto& input_table = input_table_left();

  /**
   * Write group-by columns.
   *
   * 'results_per_column' always contains at least one element, since there are either GroupBy or Aggregate columns.
   * However, we need to look only at the first element, because the keys for all columns are the same.
   *
   * The following loop is used for both, actual GroupBy columns and DISTINCT columns.
   **/
  if (_aggregates.empty()) {
    auto context = std::static_pointer_cast<AggregateResultContext<DistinctColumnType, DistinctAggregateType>>(
        _contexts_per_column[0]);
    auto pos_list = PosList();
    pos_list.reserve(context->results.size());
    for (const auto& result : context->results) {
      pos_list.push_back(result.row_id);
    }
    _write_groupby_output(pos_list);
  }

  /*
  Write the aggregated columns to the output
  */
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

  // Write the output
  auto output = std::make_shared<Table>(_output_column_definitions, TableType::Data);
  output->append_chunk(_output_segments);

  return output;
}

/*
The following template functions write the aggregated values for the different aggregate functions.
They are separate and templated to avoid compiler errors for invalid type/function combinations.
*/
// MIN, MAX, SUM, ANY write the current aggregated value
template <typename ColumnDataType, typename AggregateType, AggregateFunction func>
std::enable_if_t<func == AggregateFunction::Min || func == AggregateFunction::Max || func == AggregateFunction::Sum ||
                     func == AggregateFunction::Any,
                 void>
write_aggregate_values(std::shared_ptr<ValueSegment<AggregateType>> segment,
                       const AggregateResults<ColumnDataType, AggregateType>& results) {
  DebugAssert(segment->is_nullable(), "Aggregate: Output segment needs to be nullable");

  auto& values = segment->values();
  auto& null_values = segment->null_values();

  values.resize(results.size());
  null_values.resize(results.size());

  size_t i = 0;
  for (const auto& result : results) {
    null_values[i] = !result.current_primary_aggregate;

    if (result.current_primary_aggregate) {
      values[i] = *result.current_primary_aggregate;
    }
    ++i;
  }
}

// COUNT writes the aggregate counter
template <typename ColumnDataType, typename AggregateType, AggregateFunction func>
std::enable_if_t<func == AggregateFunction::Count, void> write_aggregate_values(
    std::shared_ptr<ValueSegment<AggregateType>> segment,
    const AggregateResults<ColumnDataType, AggregateType>& results) {
  DebugAssert(!segment->is_nullable(), "Aggregate: Output segment for COUNT shouldn't be nullable");

  auto& values = segment->values();
  values.resize(results.size());

  size_t i = 0;
  for (const auto& result : results) {
    values[i] = result.aggregate_count;
    ++i;
  }
}

// COUNT(DISTINCT) writes the number of distinct values
template <typename ColumnDataType, typename AggregateType, AggregateFunction func>
std::enable_if_t<func == AggregateFunction::CountDistinct, void> write_aggregate_values(
    std::shared_ptr<ValueSegment<AggregateType>> segment,
    const AggregateResults<ColumnDataType, AggregateType>& results) {
  DebugAssert(!segment->is_nullable(), "Aggregate: Output segment for COUNT shouldn't be nullable");

  auto& values = segment->values();
  values.resize(results.size());

  size_t i = 0;
  for (const auto& result : results) {
    values[i] = result.distinct_values.size();
    ++i;
  }
}

// AVG writes the calculated average from current aggregate and the aggregate counter
template <typename ColumnDataType, typename AggregateType, AggregateFunction func>
std::enable_if_t<func == AggregateFunction::Avg && std::is_arithmetic_v<AggregateType>, void> write_aggregate_values(
    std::shared_ptr<ValueSegment<AggregateType>> segment,
    const AggregateResults<ColumnDataType, AggregateType>& results) {
  DebugAssert(segment->is_nullable(), "Aggregate: Output segment needs to be nullable");

  auto& values = segment->values();
  auto& null_values = segment->null_values();

  values.resize(results.size());
  null_values.resize(results.size());

  size_t i = 0;
  for (const auto& result : results) {
    null_values[i] = !result.current_primary_aggregate;

    if (result.current_primary_aggregate) {
      values[i] = *result.current_primary_aggregate / static_cast<AggregateType>(result.aggregate_count);
    }
    ++i;
  }
}

// AVG is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnDataType, typename AggregateType, AggregateFunction func>
std::enable_if_t<func == AggregateFunction::Avg && !std::is_arithmetic_v<AggregateType>, void> write_aggregate_values(
    std::shared_ptr<ValueSegment<AggregateType>> segment,
    const AggregateResults<ColumnDataType, AggregateType>& results) {
  Fail("Invalid aggregate");
}

// STDDEV_SAMP writes the calculated standard deviation from current aggregate and the aggregate counter
template <typename ColumnDataType, typename AggregateType, AggregateFunction func>
std::enable_if_t<func == AggregateFunction::StandardDeviationSample && std::is_arithmetic_v<AggregateType>, void>
write_aggregate_values(std::shared_ptr<ValueSegment<AggregateType>> segment,
                       const AggregateResults<ColumnDataType, AggregateType>& results) {
  DebugAssert(segment->is_nullable(), "Aggregate: Output segment needs to be nullable");

  auto& values = segment->values();
  auto& null_values = segment->null_values();

  values.resize(results.size());
  null_values.resize(results.size());

  auto chunk_offset = ChunkOffset{0};
  for (const auto& result : results) {
    const auto count = static_cast<AggregateType>(result.aggregate_count);

    if (result.current_primary_aggregate && count > 1) {
      values[chunk_offset] = *result.current_primary_aggregate;
    } else {
      null_values[chunk_offset] = true;
    }
    ++chunk_offset;
  }
}

// STDDEV_SAMP is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnDataType, typename AggregateType, AggregateFunction func>
std::enable_if_t<func == AggregateFunction::StandardDeviationSample && !std::is_arithmetic_v<AggregateType>, void>
write_aggregate_values(std::shared_ptr<ValueSegment<AggregateType>> segment,
                       const AggregateResults<ColumnDataType, AggregateType>& results) {
  Fail("Invalid aggregate");
}

void AggregateHash::_write_groupby_output(PosList& pos_list) {
  auto input_table = input_table_left();

  // For each GROUP BY column, resolve its type, iterate over its values, and add them to a new output ValueSegment
  for (const auto& column_id : _groupby_column_ids) {
    _output_column_definitions.emplace_back(input_table->column_name(column_id),
                                            input_table->column_data_type(column_id),
                                            input_table->column_is_nullable(column_id));

    resolve_data_type(input_table->column_data_type(column_id), [&](const auto typed_value) {
      using ColumnDataType = typename decltype(typed_value)::type;

      auto values = pmr_vector<ColumnDataType>(pos_list.size());
      auto null_values = pmr_vector<bool>(pos_list.size());
      std::vector<std::unique_ptr<AbstractSegmentAccessor<ColumnDataType>>> accessors(input_table->chunk_count());

      auto output_offset = ChunkOffset{0};

      for (const auto& row_id : pos_list) {
        // pos_list was generated by grouping the input data. While it might point to rows that contain NULL
        // values, no new NULL values should have been added.
        DebugAssert(!row_id.is_null(), "Did not expect NULL value here");

        auto& accessor = accessors[row_id.chunk_id];
        if (!accessor) {
          accessor =
              create_segment_accessor<ColumnDataType>(input_table->get_chunk(row_id.chunk_id)->get_segment(column_id));
        }

        const auto& optional_value = accessor->access(row_id.chunk_offset);
        if (!optional_value) {
          null_values[output_offset] = true;
        } else {
          values[output_offset] = *optional_value;
        }
        ++output_offset;
      }

      auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
      _output_segments.push_back(value_segment);
    });
  }
}

template <typename ColumnDataType>
void AggregateHash::_write_aggregate_output(boost::hana::basic_type<ColumnDataType> type, ColumnID column_index,
                                            AggregateFunction function) {
  switch (function) {
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
      write_aggregate_output<ColumnDataType, AggregateFunction::Any>(column_index);
      break;
  }
}

template <typename ColumnDataType, AggregateFunction function>
void AggregateHash::write_aggregate_output(ColumnID column_index) {
  // retrieve type information from the aggregation traits
  typename AggregateTraits<ColumnDataType, function>::AggregateType aggregate_type;
  auto aggregate_data_type = AggregateTraits<ColumnDataType, function>::AGGREGATE_DATA_TYPE;

  const auto& aggregate = _aggregates[column_index];

  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  const auto input_column_id = pqp_column.column_id;

  if (aggregate_data_type == DataType::Null) {
    // if not specified, it’s the input column’s type
    aggregate_data_type = input_table_left()->column_data_type(input_column_id);
  }

  auto context = std::static_pointer_cast<AggregateResultContext<ColumnDataType, decltype(aggregate_type)>>(
      _contexts_per_column[column_index]);

  const auto& results = context->results;

  // Before writing the first aggregate column, write all group keys into the respective columns
  if (column_index == 0) {
    auto pos_list = PosList(context->results.size());
    auto chunk_offset = ChunkOffset{0};
    for (auto& result : context->results) {
      pos_list[chunk_offset] = (result.row_id);
      ++chunk_offset;
    }
    _write_groupby_output(pos_list);
  }

  // write aggregated values into the segment
  constexpr bool NEEDS_NULL = (function != AggregateFunction::Count && function != AggregateFunction::CountDistinct);
  _output_column_definitions.emplace_back(aggregate->as_column_name(), aggregate_data_type, NEEDS_NULL);

  auto output_segment = std::make_shared<ValueSegment<decltype(aggregate_type)>>(NEEDS_NULL);

  if (!results.empty()) {
    write_aggregate_values<ColumnDataType, decltype(aggregate_type), function>(output_segment, results);
  } else if (_groupby_column_ids.empty()) {
    // If we did not GROUP BY anything and we have no results, we need to add NULL for most aggregates and 0 for count
    output_segment->values().push_back(decltype(aggregate_type){});
    if (function != AggregateFunction::Count && function != AggregateFunction::CountDistinct) {
      output_segment->null_values().push_back(true);
    }
  }

  _output_segments.push_back(output_segment);
}

template <typename AggregateKey>
std::shared_ptr<SegmentVisitorContext> AggregateHash::_create_aggregate_context(
    const DataType data_type, const AggregateFunction function) const {
  std::shared_ptr<SegmentVisitorContext> context;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    switch (function) {
      case AggregateFunction::Min:
        context = std::make_shared<AggregateContext<
            ColumnDataType, typename AggregateTraits<ColumnDataType, AggregateFunction::Min>::AggregateType,
            AggregateKey>>();
        break;
      case AggregateFunction::Max:
        context = std::make_shared<AggregateContext<
            ColumnDataType, typename AggregateTraits<ColumnDataType, AggregateFunction::Max>::AggregateType,
            AggregateKey>>();
        break;
      case AggregateFunction::Sum:
        context = std::make_shared<AggregateContext<
            ColumnDataType, typename AggregateTraits<ColumnDataType, AggregateFunction::Sum>::AggregateType,
            AggregateKey>>();
        break;
      case AggregateFunction::Avg:
        context = std::make_shared<AggregateContext<
            ColumnDataType, typename AggregateTraits<ColumnDataType, AggregateFunction::Avg>::AggregateType,
            AggregateKey>>();
        break;
      case AggregateFunction::Count:
        context = std::make_shared<AggregateContext<
            ColumnDataType, typename AggregateTraits<ColumnDataType, AggregateFunction::Count>::AggregateType,
            AggregateKey>>();
        break;
      case AggregateFunction::CountDistinct:
        context = std::make_shared<AggregateContext<
            ColumnDataType, typename AggregateTraits<ColumnDataType, AggregateFunction::CountDistinct>::AggregateType,
            AggregateKey>>();
        break;
      case AggregateFunction::StandardDeviationSample:
        context = std::make_shared<AggregateContext<
            ColumnDataType,
            typename AggregateTraits<ColumnDataType, AggregateFunction::StandardDeviationSample>::AggregateType,
            AggregateKey>>();
        break;
      case AggregateFunction::Any:
        context = std::make_shared<AggregateContext<
            ColumnDataType, typename AggregateTraits<ColumnDataType, AggregateFunction::Any>::AggregateType,
            AggregateKey>>();
        break;
    }
  });
  return context;
}

}  // namespace opossum
