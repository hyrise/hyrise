#include "ucc_validation_rule.hpp"

#include <cmath>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "operators/get_table.hpp"
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

// `get_or_add_result` is called once per row when iterating over a column that is to be aggregated. The row's `key` has
// been calculated as part of `_partition_by_groupby_keys`. We also pass in the `row_id` of that row. This row id is
// stored in `Results` so that we can later use it to reconstruct the values in the group-by columns. If the operator
// calculates multiple aggregate functions, we only need to perform this lookup as part of the first aggregate function.
// By setting CacheResultIds to true_type, we can store the result of the lookup in the AggregateKey. Following
// aggregate functions can then retrieve the index from the AggregateKey.
constexpr auto CACHE_MASK = AggregateKeyEntry{1} << 63u;  // See explanation below

template <typename CacheResultIds, typename ResultIds, typename Results, typename AggregateKey>
bool get_or_add_result(CacheResultIds, ResultIds& result_ids, Results& results, AggregateKey& key,
                       const RowID& row_id) {
  // As described above, we may store the index into the results vector in the AggregateKey. If the AggregateKey
  // contains multiple entries, we use the first one. As such, we store a (non-owning, raw) pointer to either the only
  // or the first entry in first_key_entry. We need a raw pointer as a reference cannot be null or reset.
  AggregateKeyEntry* first_key_entry;
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
  // the cached value would double the size of the AggregateKey in the case of a single group-by column, thus halving
  // the utilization of the CPU cache. Same for a discriminating union, where the data structure alignment would also
  // result in another 8 bytes being used.
  static_assert(std::is_same_v<AggregateKeyEntry, uint64_t>, "Expected AggregateKeyEntry to be unsigned 64-bit value");

  // Check if the AggregateKey already contains a stored index.
  if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
    if (*first_key_entry & CACHE_MASK) {
      // The most significant bit is a 1, remove it by XORing the mask gives us the index into the results vector.
      const auto result_id = *first_key_entry ^ CACHE_MASK;

      // If we have not seen this index as part of the current aggregate function, the results vector may not yet have
      // the correct size. Resize it if necessary and write the current row_id so that we can recover the GroupBy
      // column(s) later. By default, the newly created values have a NULL_ROW_ID and are later ignored. We grow
      // the vector slightly more than necessary. Otherwise, monotonically increasing keys would lead to one resize
      // per row.
      if (result_id >= results.size()) {
        results.resize(static_cast<size_t>(static_cast<double>(result_id + 1) * 1.5));
      }
      auto& result = results[result_id];
      if (result.row_id != NULL_ROW_ID) {
        return false;
      }
      results[result_id].row_id = row_id;
      return true;
    }
  } else {
    Assert(!(*first_key_entry & CACHE_MASK),
           "CacheResultIds is set to false, but a cached or immediate key shortcut entry was found");
  }

  // Lookup the key in the result_ids map
  auto it = result_ids.find(key);
  if (it != result_ids.end()) {
    return false;
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
  return true;
}

template <typename AggregateKey>
AggregateKey& get_aggregate_key([[maybe_unused]] KeysPerChunk<AggregateKey>& keys_per_chunk,
                                [[maybe_unused]] const ChunkID chunk_id,
                                [[maybe_unused]] const ChunkOffset chunk_offset) {
  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    auto& hash_keys = keys_per_chunk[chunk_id];

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

UCCValidationRule::UCCValidationRule(
    tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes)
    : AbstractDependencyValidationRule(DependencyType::Unique, table_constraint_mutexes) {}

ValidationResult UCCValidationRule::_on_validate(const DependencyCandidate& candidate) const {
  std::vector<std::pair<std::string, std::shared_ptr<AbstractTableConstraint>>> constraints;
  Assert(candidate.dependents.empty(), "Invalid dependents for UCC");

  const auto table_name = candidate.determinants[0].table_name;
  const auto table = Hyrise::get().storage_manager.get_table(table_name);

  // Shortcut: if any dictionary is smaller than chunk, column cannot be unique
  if (candidate.determinants.size() == 1) {
    Assert(table->type() == TableType::Data, "Expected Data table");
    const auto column_id = candidate.determinants[0].column_id;
    const auto num_chunks = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }
      const auto segment = chunk->get_segment(column_id);
      if (const auto dictionary_segment = std::dynamic_pointer_cast<BaseDictionarySegment>(segment)) {
        if (dictionary_segment->unique_values_count() != dictionary_segment->size()) {
          return {DependencyValidationStatus::Invalid, {}};
        }
      }
    }
  }

  std::unordered_set<ColumnID> candidate_columns;
  std::vector<ColumnID> pruned_columns;
  for (const auto& determinant : candidate.determinants) {
    candidate_columns.emplace(determinant.column_id);
  }
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    if (!candidate_columns.count(column_id)) {
      pruned_columns.emplace_back(column_id);
    }
  }

  const auto get_table = std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, pruned_columns);
  get_table->execute();
  const auto pruned_table = get_table->get_output();
  const auto num_columns = pruned_table->column_count();
  std::vector<ColumnID> groupby_column_ids;
  groupby_column_ids.reserve(num_columns);
  for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
    groupby_column_ids.emplace_back(column_id);
  }

  Assert(groupby_column_ids.size() > 0, "Expected group columns");
  DependencyValidationStatus status;
  switch (groupby_column_ids.size()) {
    case 1:
      // No need for a complex data structure if we only have one entry
      status = _aggregate<AggregateKeyEntry>(pruned_table, groupby_column_ids);
      break;
    case 2:
      status = _aggregate<std::array<AggregateKeyEntry, 2>>(pruned_table, groupby_column_ids);
      break;
    default:
      status = _aggregate<AggregateKeySmallVector>(pruned_table, groupby_column_ids);
      break;
  }

  if (status == DependencyValidationStatus::Valid) {
    constraints.emplace_back(table_name,
                             std::make_shared<TableKeyConstraint>(candidate_columns, KeyConstraintType::UNIQUE));
  }
  return {status, constraints};
}

/*
Visitor context for the AggregateVisitor. The AggregateResultContext can be used without knowing the
AggregateKey, the AggregateContext is the "full" version.
*/
template <typename ColumnDataType, AggregateFunction aggregate_function>
struct AggregateResultContext : SegmentVisitorContext {
  using AggregateResultAllocator = PolymorphicAllocator<AggregateResults<ColumnDataType, aggregate_function>>;

  // In cases where we know how many values to expect, we can preallocate the context in order to avoid later
  // re-allocations.
  explicit AggregateResultContext(const size_t preallocated_size = 0)
      : results(preallocated_size, AggregateResultAllocator{&buffer}) {}

  boost::container::pmr::monotonic_buffer_resource buffer;
  AggregateResults<ColumnDataType, aggregate_function> results;
};

template <typename ColumnDataType, AggregateFunction aggregate_function, typename AggregateKey>
struct AggregateContext : public AggregateResultContext<ColumnDataType, aggregate_function> {
  explicit AggregateContext(const size_t preallocated_size = 0)
      : AggregateResultContext<ColumnDataType, aggregate_function>(preallocated_size) {
    auto allocator = AggregateResultIdMapAllocator<AggregateKey>{&this->buffer};

    // Unused if AggregateKey == EmptyAggregateKey, but we initialize it anyway to reduce the number of diverging code
    // paths.
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage) - false warning: called C++ object (result_ids) is null
    result_ids = std::make_unique<AggregateResultIdMap<AggregateKey>>(allocator);
  }

  std::unique_ptr<AggregateResultIdMap<AggregateKey>> result_ids;
};

/**
 * Partition the input chunks by the given group key(s). This is done by creating a vector that contains the
 * AggregateKey for each row. It is gradually built by visitors, one for each group segment.
 */
template <typename AggregateKey>
KeysPerChunk<AggregateKey> UCCValidationRule::_partition_by_groupby_keys(
    const std::shared_ptr<const Table>& input_table, const std::vector<ColumnID>& groupby_column_ids,
    std::atomic_size_t& expected_result_size, bool& use_immediate_key_shortcut) const {
  KeysPerChunk<AggregateKey> keys_per_chunk;

  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    const auto chunk_count = input_table->chunk_count();

    // Create the actual data structure
    keys_per_chunk.reserve(chunk_count);
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table->get_chunk(chunk_id);
      if (!chunk) continue;

      if constexpr (std::is_same_v<AggregateKey, AggregateKeySmallVector>) {
        keys_per_chunk.emplace_back(chunk->size(), AggregateKey(groupby_column_ids.size()));
      } else {
        keys_per_chunk.emplace_back(chunk->size(), AggregateKey{});
      }
    }

    // Now that we have the data structures in place, we can start the actual work. We want to fill
    // keys_per_chunk[chunk_id][chunk_offset] with something that uniquely identifies the group into which that
    // position belongs. There are a couple of options here (cf. UCCValidator::_on_execute):
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
    jobs.reserve(groupby_column_ids.size());

    for (size_t group_column_index = 0; group_column_index < groupby_column_ids.size(); ++group_column_index) {
      jobs.emplace_back(std::make_shared<JobTask>([&input_table, group_column_index, &keys_per_chunk, chunk_count,
                                                   &expected_result_size, &use_immediate_key_shortcut,
                                                   &groupby_column_ids]() {
        const auto groupby_column_id = groupby_column_ids.at(group_column_index);
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
              // In some cases (e.g., TPC-H Q18), we aggregate with consecutive int32_t values being used as a group by
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
                expected_result_size = static_cast<size_t>(max_key - min_key) + 2;
                use_immediate_key_shortcut = true;

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
            use_immediate_key_shortcut = false;

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

            // We will see at least `id_map.size()` different groups. We can use this knowledge to preallocate memory
            // for the results. Estimating the number of groups for multiple GROUP BY columns is somewhat hard, so we
            // simply take the number of groups created by the GROUP BY column with the highest number of distinct
            // values.
            auto previous_max = expected_result_size.load();
            while (previous_max < id_map.size()) {
              // _expected_result_size needs to be atomatically updated as the GROUP BY columns are processed in
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
  }

  return keys_per_chunk;
}

template <typename AggregateKey>
DependencyValidationStatus UCCValidationRule::_aggregate(const std::shared_ptr<const Table>& input_table,
                                                         const std::vector<ColumnID>& groupby_column_ids) const {
  std::atomic_size_t expected_result_size{};
  bool use_immediate_key_shortcut{};

  if constexpr (HYRISE_DEBUG) {
    for (const auto& groupby_column_id : groupby_column_ids) {
      Assert(groupby_column_id < input_table->column_count(), "GroupBy column index out of bounds");
    }
  }

  /**
   * PARTITIONING STEP
   */
  auto keys_per_chunk = _partition_by_groupby_keys<AggregateKey>(input_table, groupby_column_ids, expected_result_size,
                                                                 use_immediate_key_shortcut);

  /**
   * AGGREGATION STEP
   */
  /*
    Insert a dummy context for the DISTINCT implementation.
    That way, _contexts_per_column will always have at least one context with results.
    This is important later on when we write the group keys into the table.
    The template parameters (int32_t, AggregateFunction::Min) do not matter, as we do not calculate an aggregate anyway.
    */
  auto context =
      std::make_shared<AggregateContext<int32_t, AggregateFunction::Min, AggregateKey>>(expected_result_size);

  // Process Chunks and perform aggregations
  const auto chunk_count = input_table->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk_in = input_table->get_chunk(chunk_id);
    if (!chunk_in) continue;

    // Sometimes, gcc is really bad at accessing loop conditions only once, so we cache that here.
    const auto input_chunk_size = chunk_in->size();

    /**
       * DISTINCT implementation
       *
       * In Opossum we handle the SQL keyword DISTINCT by using an aggregate operator with grouping but without
       * aggregate functions. All input columns (either explicitly specified as `SELECT DISTINCT a, b, c` OR implicitly
       * as `SELECT DISTINCT *` are passed as `groupby_column_ids`).
       *
       * As the grouping happens as part of the aggregation but no aggregate function exists, we use
       * `AggregateFunction::Min` as a fake aggregate function whose result will be discarded. From here on, the steps
       * are the same as they are for a regular grouped aggregate.
       */
    auto& result_ids = *context->result_ids;
    auto& results = context->results;

    // Add value or combination of values is added to the list of distinct value(s). This is done by calling
    // get_or_add_result, which adds the corresponding entry in the list of GROUP BY values.
    if (use_immediate_key_shortcut) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < input_chunk_size; chunk_offset++) {
        // We are able to use immediate keys, so pass true_type so that the combined caching/immediate key code path
        // is enabled in get_or_add_result.
        if (!get_or_add_result(std::true_type{}, result_ids, results,
                               get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                               RowID{chunk_id, chunk_offset})) {
          return DependencyValidationStatus::Invalid;
        }
      }
    } else {
      // Same as above, but we do not have immediate keys, so we disable that code path to reduce the complexity of
      // get_aggregate_key.
      for (ChunkOffset chunk_offset{0}; chunk_offset < input_chunk_size; chunk_offset++) {
        if (!get_or_add_result(std::false_type{}, result_ids, results,
                               get_aggregate_key<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                               RowID{chunk_id, chunk_offset})) {
          return DependencyValidationStatus::Invalid;
        }
      }
    }
  }
  return DependencyValidationStatus::Valid;
}  // NOLINT(readability/fn_size)

}  // namespace opossum
