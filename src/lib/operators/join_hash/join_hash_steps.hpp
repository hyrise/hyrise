#pragma once

#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/unsynchronized_pool_resource.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/lexical_cast.hpp>
#include <uninitialized_vector.hpp>

#include "bytell_hash_map.hpp"
#include "hyrise.hpp"
#include "operators/multi_predicate_join/multi_predicate_join_evaluator.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "type_comparison.hpp"

/*
  This file includes the functions that cover the main steps of our hash join implementation
  (e.g., build() and probe()). These free functions are put into this header file to separate
  them from the process flow of the join hash and to make the better testable.
*/
namespace opossum {

// For most join types, we are interested in retrieving the positions (i.e., the RowIDs) on the left and the right side.
// For semi and anti joins, we only care whether a value exists or not, so there is no point in tracking the position
// in the input table of more than one occurrence of a value. However, if we have secondary predicates, we do need to
// track all occurrences of a value as that first position might be disqualified later.
enum class JoinHashBuildMode { AllPositions, ExistenceOnly };

using Hash = size_t;

/*
This is how elements of the input relations are saved after materialization.
The original value is used to detect hash collisions.
*/
template <typename T>
struct PartitionedElement {
  RowID row_id;
  T value;
};

// A partition is a part of a materialized join column, either on the build or the probe side. The partitions of one
// column are stored in a RadixContainer. Initially, the input data is partitioned by input chunks. After the
// (optional) radix partitioning step, they are partitioned according to the hash value. If no radix partitioning was
// performed, all partitions on the probe side have to be checked against all partitions on the build side (m:n). If
// radix partitioning is used, each partition on the probe side finds its matching rows in exactly one partition on
// the build side (1:1).
template <typename T>
struct Partition {
  // Initializing the partition vector takes some time. This is not necessary, because it will be overwritten anyway.
  // The uninitialized_vector behaves like a regular std::vector, but the entries are initially invalid.
  std::conditional_t<std::is_trivially_destructible_v<T>, uninitialized_vector<PartitionedElement<T>>,
                     std::vector<PartitionedElement<T>>>
      elements;

  // Bit vector to store NULL flags - not using uninitialized_vector because it is not specialized for bool.
  // It is stored independently of the elements as adding a single bit to PartitionedElement would cause memory waste
  // due to padding.
  std::vector<bool> null_values;
};

// This alias is used in two phases:
//   - the result of the materialization phase, at this time partitioned by chunks as we parallelize the
//     materialization phase via chunks
//   - the result of the radix clustering phase
// As the radix clustering might be skipped (when radix_bits == 0), both the materialization as well as the radix
// clustering methods yield RadixContainers.
template <typename T>
using RadixContainer = std::vector<Partition<T>>;

// Stores the mapping from HashedType to positions. Conceptually, this is similar to an (unordered_)multimap, but it has
// some optimizations for the performance-critical probe() method. Instead of storing the matches directly in the
// hashmap (think map<HashedType, PosList>), we store an offset - thus OffsetHashTable. This keeps the hashmap small and
// makes it easier to cache. The PosHashTable has a separate build and probe phase. In the build phase, the
// OffsetHashTable and the corresponding SmallPosLists are filled. After that, finalize() is called and compresses the
// SmallPosList into a single, contiguous RowIDPosList. This significantly reduces the memory footprint and thus
// improves the cache behavior of the following probe phase. In the probe phase, the find() method returns a pair of
// pointers to the range in the unified PosList. This is comparable to the interface of std::equal_range.
template <typename HashedType>
class PosHashTable {
  // If we end up with a partition that has more values than Offset can hold, the partitioning algorithm is at fault.
  using Offset = uint32_t;

  // In case we consider runtime to be more relevant, the flat hash map performs better (measured to be mostly on par
  // with bytell hash map and in some cases up to 5% faster) but is significantly larger than the bytell hash map.
  using OffsetHashTable = ska::bytell_hash_map<HashedType, Offset>;

  // The small_vector holds the first n values in local storage and only resorts to heap storage after that. 1 is chosen
  // as n because in many cases, we join on primary key attributes where by definition we have only one match on the
  // smaller side.
  using SmallPosList = boost::container::small_vector<RowID, 1, PolymorphicAllocator<RowID>>;

  // After finalize() is called, the UnifiedPosList holds the concatenation of all SmallPosLists. The SmallPosList at
  // position n can be found at the half-open range [ pos_list[offsets[n]], pos_list[offsets[n+1]] ).
  struct UnifiedPosList {
    RowIDPosList pos_list;
    std::vector<size_t> offsets;
  };

 public:
  explicit PosHashTable(const JoinHashBuildMode mode, const size_t max_size)
      : _mode(mode),
        _small_pos_lists(mode == JoinHashBuildMode::AllPositions ? max_size + 1 : 0,
                         SmallPosList{SmallPosList::allocator_type(_memory_pool.get())}) {
    // _small_pos_lists is initialized with an additional element to make the enforcement of the assertions easier. For
    // _JoinHashBuildMode::ExistenceOnly, we do not store positions and thus do not initialize _small_pos_lists.
    _offset_hash_table.reserve(max_size);
  }

  // For a value seen on the build side, add the value to the hash map.
  // The row id is only added in the AllPositions mode. For ExistenceOnly, as used e.g., by semi joins, the actual
  // row id is irrelevant and is not stored.
  template <typename InputType>
  void emplace(const InputType& value, RowID row_id) {
    const auto casted_value = static_cast<HashedType>(value);

    // If casted_value is already present in the hash table, this returns an iterator to the existing value. If not, it
    // inserts a mapping from casted_value to the index into _values, which is defined by the previously inserted
    // number of values.
    const auto it = _offset_hash_table.emplace(casted_value, _offset_hash_table.size());
    if (_mode == JoinHashBuildMode::AllPositions) {
      auto& pos_list = _small_pos_lists[it.first->second];
      pos_list.emplace_back(row_id);

      DebugAssert(_offset_hash_table.size() < _small_pos_lists.size(),
                  "Hash table too big for pre-allocated data structures");
      DebugAssert(_offset_hash_table.size() < std::numeric_limits<Offset>::max(), "Hash table too big for offset");
    }
  }

  // Rewrite the SmallPosLists into one giant UnifiedPosList (see above).
  void finalize() {
    _offset_hash_table.shrink_to_fit();

    if (_mode == JoinHashBuildMode::AllPositions) {
      _unified_pos_list = UnifiedPosList{};
      _unified_pos_list->offsets.resize(_offset_hash_table.size() + 1);
      auto total_size = size_t{};
      for (auto i = size_t{0}; i < _offset_hash_table.size(); ++i) {
        _unified_pos_list->offsets[i] = total_size;
        total_size += _small_pos_lists[i].size();
      }
      _unified_pos_list->offsets.back() = total_size;

      _unified_pos_list->pos_list.resize(total_size);
      auto offset = size_t{};
      for (auto i = size_t{0}; i < _offset_hash_table.size(); ++i) {
        std::copy(_small_pos_lists[i].begin(), _small_pos_lists[i].end(), _unified_pos_list->pos_list.begin() + offset);
        offset += _small_pos_lists[i].size();
      }

      _small_pos_lists = {};
      _memory_pool = {};
      _monotonic_buffer = {};
    }
  }

  // For a value seen on the probe side, return an iterator pair into the matching positions on the build side
  template <typename InputType>
  const std::pair<RowIDPosList::const_iterator, RowIDPosList::const_iterator> find(const InputType& value) const {
    DebugAssert(_mode == JoinHashBuildMode::AllPositions, "find is invalid for ExistenceOnly mode, use contains");
    DebugAssert(_unified_pos_list, "_unified_pos_list not set - was finalize called?");

    const auto casted_value = static_cast<HashedType>(value);
    const auto hash_table_iter = _offset_hash_table.find(casted_value);

    if (hash_table_iter == _offset_hash_table.end()) {
      // Not found, return an empty range
      return {_unified_pos_list->pos_list.end(), _unified_pos_list->pos_list.end()};
    }

    return {_unified_pos_list->pos_list.begin() + _unified_pos_list->offsets[hash_table_iter->second],
            _unified_pos_list->pos_list.begin() + _unified_pos_list->offsets[hash_table_iter->second + 1]};
  }

  // For a value seen on the probe side, return whether it has been seen on the build side
  template <typename InputType>
  bool contains(const InputType& value) const {
    const auto casted_value = static_cast<HashedType>(value);
    return _offset_hash_table.find(casted_value) != _offset_hash_table.end();
  }

  // const RowIDPosList::const_iterator begin() const { return _small_pos_lists.begin(); }

  // const RowIDPosList::const_iterator end() const { return _small_pos_lists.end(); }

 private:
  // During the build phase, the small_vectors cause many small allocations. Instead of going to malloc every time,
  // we create our own (non-thread-safe) pool, which is discarded once finalize() is called
  std::unique_ptr<boost::container::pmr::monotonic_buffer_resource> _monotonic_buffer =
      std::make_unique<boost::container::pmr::monotonic_buffer_resource>();  // TODO(md) increase in mem consumption? preallocate?
  std::unique_ptr<boost::container::pmr::unsynchronized_pool_resource> _memory_pool =
      std::make_unique<boost::container::pmr::unsynchronized_pool_resource>(_monotonic_buffer.get());

  JoinHashBuildMode _mode{};
  OffsetHashTable _offset_hash_table{};
  std::vector<SmallPosList> _small_pos_lists{};

  std::optional<UnifiedPosList> _unified_pos_list{};
};

// The bloom filter (with k=1) is used during the materialization and build phases. It contains `true` for each
// `hash_function(value) % BLOOM_FILTER_SIZE`. Much of how the bloom filter is used in the hash join could be improved:
// (1) Dynamically check whether bloom filters are worth the memory and computational costs This could be based on the
//     input table sizes, the expected cardinalities, the hardware characteristics, or other factors.
// (2) Choosing an appropriate filter size. 2^20 was experimentally found to be good for TPC-H SF 10, but is certainly
//     not optimal in every situation.
// (3) Check whether using multiple hash functions (k>1) brings any improvement.
// (4) Use the probe side bloom filter when partitioning the build side. By doing that, we reduce the size of the
//     intermediary results. When a bloom filter-supported partitioning has been done (i.e., partitioning has not
//     been skipped), we do not need to use a bloom filter in the build phase anymore.
// Some of these points could be addressed with relatively low effort and should bring additional, significant benefits.
// We did not yet work on this because the bloom filter was a byproduct of a research project and we have not had the
// resources to optimize it at the time.
static constexpr auto BLOOM_FILTER_SIZE = 1 << 20;
static constexpr auto BLOOM_FILTER_MASK = BLOOM_FILTER_SIZE - 1;

// Using dynamic_bitset because, different from vector<bool>, it has an efficient operator| implementation, which is
// needed for merging partial bloom filters created by different threads. Note that the dynamic_bitset(n, value)
// constructor does not do what you would expect it to, so try to avoid it.
using BloomFilter = boost::dynamic_bitset<>;

// ALL_TRUE_BLOOM_FILTER is initialized by creating a BloomFilter with every value being false and using bitwise
// negation (~x). As the negation is surprisingly expensive, we create a static empty bloom filter and reference
// it where needed. Having a bloom filter that always returns true avoids a branch in the hot loop.
static const auto ALL_TRUE_BLOOM_FILTER = ~BloomFilter(BLOOM_FILTER_SIZE);

// @param in_table             Table to materialize
// @param column_id            Column within that table to materialize
// @param histograms           Out: If radix_bits > 0, contains one histogram per chunk where each histogram contains
//                             1 << radix_bits slots
// @param radix_bits           Number of radix_bits, needed only for histogram calculation
// @param output_bloom_filter  Out: A filled BloomFilter where `value & BLOOM_FILTER_MASK == true` for each value
//                             encountered in the input column
// @param input_bloom_filter   Optional: Materialization is skipped for each value where the corresponding slot in the
//                             bloom filter is false
template <typename T, typename HashedType, bool keep_null_values>
RadixContainer<T> materialize_input(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                    std::vector<std::vector<size_t>>& histograms, const size_t radix_bits,
                                    BloomFilter& output_bloom_filter,
                                    const BloomFilter& input_bloom_filter = ALL_TRUE_BLOOM_FILTER) {
  // Retrieve input chunk_count as it might change during execution if we work on a non-reference table
  auto chunk_count = in_table->chunk_count();

  const std::hash<HashedType> hash_function;
  // List of all elements that will be partitioned
  auto radix_container = RadixContainer<T>{};
  radix_container.resize(chunk_count);

  // Fan-out
  const size_t num_radix_partitions = 1ull << radix_bits;

  // Currently, we just do one pass
  const auto pass = size_t{0};
  const auto radix_mask = static_cast<size_t>(pow(2, radix_bits * (pass + 1)) - 1);

  Assert(output_bloom_filter.empty(), "output_bloom_filter should be empty");
  output_bloom_filter.resize(BLOOM_FILTER_SIZE);
  std::mutex output_bloom_filter_mutex;

  Assert(input_bloom_filter.size() == BLOOM_FILTER_SIZE, "Invalid input_bloom_filter");

  // Create histograms per chunk
  histograms.resize(chunk_count);

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(chunk_count);
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    if (!in_table->get_chunk(chunk_id)) continue;

    jobs.emplace_back(std::make_shared<JobTask>([&, in_table, chunk_id]() {
      auto local_output_bloom_filter = BloomFilter{};
      std::reference_wrapper<BloomFilter> used_output_bloom_filter = output_bloom_filter;
      if (Hyrise::get().is_multi_threaded()) {
        // We cannot write to BloomFilter concurrently, so we build a local one first.
        local_output_bloom_filter = BloomFilter(BLOOM_FILTER_SIZE, false);
        used_output_bloom_filter = local_output_bloom_filter;
      }

      const auto chunk_in = in_table->get_chunk(chunk_id);

      // Skip chunks that were physically deleted
      if (!chunk_in) return;

      auto& elements = radix_container[chunk_id].elements;
      auto& null_values = radix_container[chunk_id].null_values;

      const auto num_rows = chunk_in->size();
      elements.resize(num_rows);
      if constexpr (keep_null_values) {
        null_values.resize(num_rows);
      }

      auto elements_iter = elements.begin();
      [[maybe_unused]] auto null_values_iter = null_values.begin();

      // prepare histogram
      auto histogram = std::vector<size_t>(num_radix_partitions);

      auto reference_chunk_offset = ChunkOffset{0};

      const auto segment = chunk_in->get_segment(column_id);
      segment_with_iterators<T>(*segment, [&](auto it, auto end) {
        using IterableType = typename decltype(it)::IterableType;

        if (dynamic_cast<ValueSegment<T>*>(&*segment)) {
          // The last chunk might have changed its size since we allocated elements. This would be due to concurrent
          // inserts into that chunk. In any case, those inserts will not be visible to our current transaction, so we
          // can ignore them.
          const auto inserted_rows = (end - it) - num_rows;
          end -= inserted_rows;
        } else {
          Assert(end - it == num_rows, "Non-ValueSegment changed size while being accessed");
        }

        while (it != end) {
          const auto& value = *it;

          if (!value.is_null() || keep_null_values) {
            // TODO(anyone): static_cast is almost always safe, since HashType is big enough. Only for double-vs-long
            // joins an information loss is possible when joining with longs that cannot be losslessly converted to
            // double. See #1550 for details.
            const Hash hashed_value = hash_function(static_cast<HashedType>(value.value()));

            auto skip = false;
            if (!value.is_null() && !input_bloom_filter[hashed_value & BLOOM_FILTER_MASK] && !keep_null_values) {
              // Value in not present in input bloom filter and can be skipped
              skip = true;
            }

            if (!skip) {
              // Fill the corresponding slot in the bloom filter
              used_output_bloom_filter.get()[hashed_value & BLOOM_FILTER_MASK] = true;

              /*
              For ReferenceSegments we do not use the RowIDs from the referenced tables.
              Instead, we use the index in the ReferenceSegment itself. This way we can later correctly dereference
              values from different inputs (important for Multi Joins).
              */
              if constexpr (is_reference_segment_iterable_v<IterableType>) {
                *elements_iter = PartitionedElement<T>{RowID{chunk_id, reference_chunk_offset}, value.value()};
              } else {
                *elements_iter = PartitionedElement<T>{RowID{chunk_id, value.chunk_offset()}, value.value()};
              }
              ++elements_iter;

              // In case we care about NULL values, store the NULL flag
              if constexpr (keep_null_values) {
                if (value.is_null()) {
                  *null_values_iter = true;
                }
                ++null_values_iter;
              }

              if (radix_bits > 0) {
                const Hash radix = hashed_value & radix_mask;
                ++histogram[radix];
              }
            }
          }

          // reference_chunk_offset is only used for ReferenceSegments
          if constexpr (is_reference_segment_iterable_v<IterableType>) {
            ++reference_chunk_offset;
          }

          ++it;
        }
      });

      // elements was allocated with the size of the chunk. As we might have skipped NULL values, we need to resize the
      // vector to the number of values actually written.
      elements.resize(std::distance(elements.begin(), elements_iter));
      null_values.resize(std::distance(null_values.begin(), null_values_iter));

      histograms[chunk_id] = std::move(histogram);

      if (Hyrise::get().is_multi_threaded()) {
        // Merge the local_output_bloom_filter into output_bloom_filter
        std::lock_guard<std::mutex> lock{output_bloom_filter_mutex};
        output_bloom_filter |= local_output_bloom_filter;
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  return radix_container;
}

/*
Build all the hash tables for the partitions of the build column. One job per partition
*/

template <typename BuildColumnType, typename HashedType>
std::vector<std::optional<PosHashTable<HashedType>>> build(const RadixContainer<BuildColumnType>& radix_container,
                                                           const JoinHashBuildMode mode, const size_t radix_bits,
                                                           const BloomFilter& input_bloom_filter) {
  Assert(input_bloom_filter.size() == BLOOM_FILTER_SIZE, "invalid input_bloom_filter");

  if (radix_container.empty()) return {};

  /*
  NUMA notes:
  The hash tables for each partition P should also reside on the same node as the build and probe partitions.
  */
  std::vector<std::optional<PosHashTable<HashedType>>> hash_tables;

  if (radix_bits == 0) {
    auto total_size = size_t{0};
    for (size_t partition_idx = 0; partition_idx < radix_container.size(); ++partition_idx) {
      total_size += radix_container[partition_idx].elements.size();
    }
    hash_tables.resize(1);
    hash_tables[0] = PosHashTable<HashedType>(mode, total_size);
  } else {
    hash_tables.resize(radix_container.size());
  }

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(radix_container.size());

  for (size_t partition_idx = 0; partition_idx < radix_container.size(); ++partition_idx) {
    // Skip empty partitions, so that we don't have too many empty jobs and hash tables
    if (radix_container[partition_idx].elements.empty()) {
      continue;
    }

    const std::hash<HashedType> hash_function;

    const auto insert_into_hash_table = [&, partition_idx]() {
      const auto hash_table_idx = radix_bits > 0 ? partition_idx : 0;
      const auto& elements = radix_container[partition_idx].elements;

      auto& hash_table = hash_tables[hash_table_idx];
      if (radix_bits > 0) {
        hash_table = PosHashTable<HashedType>(mode, elements.size());
      }
      for (const auto& element : elements) {
        DebugAssert(!(element.row_id == NULL_ROW_ID), "No NULL_ROW_IDs should make it to this point");

        const Hash hashed_value = hash_function(static_cast<HashedType>(element.value));
        if (!input_bloom_filter[hashed_value & BLOOM_FILTER_MASK]) {
          continue;
        }

        hash_table->emplace(element.value, element.row_id);
      }

      if (radix_bits > 0) {
        // In case only a single hash table is built, shrink to fit is called outside of the loop.
        hash_table->finalize();
      }
    };

    if (radix_bits == 0) {
      // Without radix partitioning, only a single hash table will be written. Parallelizing this would require a
      // concurrent hash table, which is likely more expensive.
      insert_into_hash_table();
    } else {
      jobs.emplace_back(std::make_shared<JobTask>(insert_into_hash_table));
    }
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // If radix partitioning is used, finalize is called above.
  if (radix_bits == 0) hash_tables[0]->finalize();

  return hash_tables;
}

template <typename T, typename HashedType, bool keep_null_values>
RadixContainer<T> partition_by_radix(const RadixContainer<T>& radix_container,
                                     std::vector<std::vector<size_t>>& histograms, const size_t radix_bits,
                                     const BloomFilter& input_bloom_filter = ALL_TRUE_BLOOM_FILTER) {
  if (radix_container.empty()) return radix_container;

  if constexpr (keep_null_values) {
    Assert(radix_container[0].elements.size() == radix_container[0].null_values.size(),
           "partition_by_radix() called with NULL consideration but radix container does not store any NULL "
           "value information");
  }

  const std::hash<HashedType> hash_function;

  const auto input_partition_count = radix_container.size();
  const auto output_partition_count = size_t{1} << radix_bits;

  // currently, we just do one pass
  const size_t pass = 0;
  const size_t radix_mask = static_cast<uint32_t>(pow(2, radix_bits * (pass + 1)) - 1);

  // allocate new (shared) output
  auto output = RadixContainer<T>(output_partition_count);

  Assert(histograms.size() == input_partition_count, "Expected one histogram per input partition");
  Assert(histograms[0].size() == output_partition_count, "Expected one histogram bucket per output partition");

  // Writing to std::vector<bool> is not thread-safe if the same byte is being written to. For now, we temporarily
  // use a std::vector<char> and compress it into an std::vector<bool> later.
  auto null_values_as_char = std::vector<std::vector<char>>(output_partition_count);

  // output_offsets_by_input_partition[input_partition_idx][output_partition_idx] holds the first offset in the
  // bucket written for input_partition_idx
  auto output_offsets_by_input_partition =
      std::vector<std::vector<size_t>>(input_partition_count, std::vector<size_t>(output_partition_count));
  for (auto output_partition_idx = size_t{0}; output_partition_idx < output_partition_count; ++output_partition_idx) {
    auto this_output_partition_size = size_t{0};
    for (auto input_partition_idx = size_t{0}; input_partition_idx < input_partition_count; ++input_partition_idx) {
      output_offsets_by_input_partition[input_partition_idx][output_partition_idx] = this_output_partition_size;
      this_output_partition_size += histograms[input_partition_idx][output_partition_idx];
    }

    output[output_partition_idx].elements.resize(this_output_partition_size);
    if (keep_null_values) {
      output[output_partition_idx].null_values.resize(this_output_partition_size);
      null_values_as_char[output_partition_idx].resize(this_output_partition_size);
    }
  }

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(input_partition_count);

  for (ChunkID input_partition_idx{0}; input_partition_idx < input_partition_count; ++input_partition_idx) {
    jobs.emplace_back(std::make_shared<JobTask>([&, input_partition_idx]() {
      const auto& input_partition = radix_container[input_partition_idx];
      for (auto input_idx = size_t{0}; input_idx < input_partition.elements.size(); ++input_idx) {
        const auto& element = input_partition.elements[input_idx];

        if constexpr (!keep_null_values) {
          DebugAssert(!(element.row_id == NULL_ROW_ID), "NULL_ROW_ID should not have made it this far");
        }

        const size_t radix = hash_function(static_cast<HashedType>(element.value)) & radix_mask;

        auto& output_idx = output_offsets_by_input_partition[input_partition_idx][radix];
        DebugAssert(output_idx < output[radix].elements.size(), "output_idx is completely out-of-bounds");
        if (input_partition_idx < input_partition_count - 1) {
          DebugAssert(output_idx < output_offsets_by_input_partition[input_partition_idx + 1][radix],
                      "output_idx goes into next range");
        }

        // In case NULL values have been materialized in materialize_input(), we need to keep them during the radix
        // clustering phase.
        if constexpr (keep_null_values) {
          null_values_as_char[radix][output_idx] = input_partition.null_values[input_idx];
        }

        output[radix].elements[output_idx] = element;

        ++output_idx;
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  jobs.clear();

  // Compress null_values_as_char into partition.null_values
  if constexpr (keep_null_values) {
    for (auto output_partition_idx = size_t{0}; output_partition_idx < output_partition_count; ++output_partition_idx) {
      jobs.emplace_back(std::make_shared<JobTask>([&, output_partition_idx]() {
        for (auto element_idx = size_t{0}; element_idx < output[output_partition_idx].null_values.size();
             ++element_idx) {
          output[output_partition_idx].null_values[element_idx] =
              null_values_as_char[output_partition_idx][element_idx];
        }
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  return output;
}

/*
  In the probe phase we take all partitions from the probe partition, iterate over them and compare each join candidate
  with the values in the hash table. Since build and probe are hashed using the same hash function, we can reduce the
  number of hash tables that need to be looked into to just 1.
  */
template <typename ProbeColumnType, typename HashedType, bool keep_null_values>
void probe(const RadixContainer<ProbeColumnType>& probe_radix_container,
           const std::vector<std::optional<PosHashTable<HashedType>>>& hash_tables,
           std::vector<RowIDPosList>& pos_lists_build_side, std::vector<RowIDPosList>& pos_lists_probe_side,
           const JoinMode mode, const Table& build_table, const Table& probe_table,
           const std::vector<OperatorJoinPredicate>& secondary_join_predicates) {
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(probe_radix_container.size());

  /*
    NUMA notes:
    At this point both input relations are partitioned using radix partitioning.
    Probing will be done per partition for both sides.
    Therefore, inputs for one partition should be located on the same NUMA node,
    and the job that probes that partition should also be on that NUMA node.
  */

  for (size_t partition_idx = 0; partition_idx < probe_radix_container.size(); ++partition_idx) {
    // Skip empty partitions to avoid empty output chunks
    if (probe_radix_container[partition_idx].elements.empty()) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, partition_idx]() {
      const auto& partition = probe_radix_container[partition_idx];
      const auto& elements = partition.elements;
      const auto& null_values = partition.null_values;

      RowIDPosList pos_list_build_side_local;
      RowIDPosList pos_list_probe_side_local;

      if constexpr (keep_null_values) {
        Assert(elements.size() == null_values.size(),
               "Hash join probe called with NULL consideration but inputs do not store any NULL value information");
      }

      const auto hash_table_idx = hash_tables.size() > 1 ? partition_idx : 0;
      if (!hash_tables.empty() && hash_tables.at(hash_table_idx)) {
        const auto& hash_table = *hash_tables[hash_table_idx];

        // The MultiPredicateJoinEvaluator use accessors internally. Those are not thread-safe, so we create one
        // evaluator per job.
        std::optional<MultiPredicateJoinEvaluator> multi_predicate_join_evaluator;
        if (!secondary_join_predicates.empty()) {
          multi_predicate_join_evaluator.emplace(build_table, probe_table, mode, secondary_join_predicates);
        }

        // Simple heuristic to estimate result size: half of the partition's rows will match
        // a more conservative pre-allocation would be the size of the build cluster
        const size_t expected_output_size = static_cast<size_t>(std::max(10.0, std::ceil(elements.size() / 2)));
        pos_list_build_side_local.reserve(static_cast<size_t>(expected_output_size));
        pos_list_probe_side_local.reserve(static_cast<size_t>(expected_output_size));

        for (auto partition_offset = size_t{0}; partition_offset < elements.size(); ++partition_offset) {
          const auto& probe_column_element = elements[partition_offset];

          if (mode == JoinMode::Inner && probe_column_element.row_id == NULL_ROW_ID) {
            // From previous joins, we could potentially have NULL values that do not refer to
            // an actual probe_column_element but to the NULL_ROW_ID. Hence, we can only skip for inner joins.
            continue;
          }

          auto [primary_predicate_matching_rows_iter, primary_predicate_matching_rows_end] =
              hash_table.find(static_cast<HashedType>(probe_column_element.value));

          if (primary_predicate_matching_rows_iter != primary_predicate_matching_rows_end) {
            // Key exists, thus we have at least one hit for the primary predicate

            // Since we cannot store NULL values directly in off-the-shelf containers,
            // we need to the check the NULL bit vector here because a NULL value (represented
            // as a zero) yields the same rows as an actual zero value.
            // For inner joins, we skip NULL values and output them for outer joins.
            // Note: If the materialization/radix partitioning phase did not explicitly consider
            // NULL values, they will not be handed to the probe function.
            if constexpr (keep_null_values) {
              if (null_values[partition_offset]) {
                pos_list_build_side_local.emplace_back(NULL_ROW_ID);
                pos_list_probe_side_local.emplace_back(probe_column_element.row_id);
                // ignore found matches and continue with next probe item
                continue;
              }
            }

            // If NULL values are discarded, the matching probe_column_element pairs will be written to the result pos
            // lists.
            if (!multi_predicate_join_evaluator) {
              for (; primary_predicate_matching_rows_iter != primary_predicate_matching_rows_end;
                   ++primary_predicate_matching_rows_iter) {
                const auto row_id = *primary_predicate_matching_rows_iter;
                pos_list_build_side_local.emplace_back(row_id);
                pos_list_probe_side_local.emplace_back(probe_column_element.row_id);
              }
            } else {
              auto match_found = false;
              for (; primary_predicate_matching_rows_iter != primary_predicate_matching_rows_end;
                   ++primary_predicate_matching_rows_iter) {
                const auto row_id = *primary_predicate_matching_rows_iter;
                if (multi_predicate_join_evaluator->satisfies_all_predicates(row_id, probe_column_element.row_id)) {
                  pos_list_build_side_local.emplace_back(row_id);
                  pos_list_probe_side_local.emplace_back(probe_column_element.row_id);
                  match_found = true;
                }
              }

              // We have not found matching items for all predicates.
              if constexpr (keep_null_values) {
                if (!match_found) {
                  pos_list_build_side_local.emplace_back(NULL_ROW_ID);
                  pos_list_probe_side_local.emplace_back(probe_column_element.row_id);
                }
              }
            }

          } else {
            // We have not found matching items for the first predicate. Only continue for non-equi join modes.
            // We use constexpr to prune this conditional for the equi-join implementation.
            // Note, the outer relation (i.e., left relation for LEFT OUTER JOINs) is the probing
            // relation since the relations are swapped upfront.
            if constexpr (keep_null_values) {
              pos_list_build_side_local.emplace_back(NULL_ROW_ID);
              pos_list_probe_side_local.emplace_back(probe_column_element.row_id);
            }
          }
        }
      } else {
        // When there is no hash table, we might still need to handle the values of the probe side for LEFT
        // and RIGHT joins. We use constexpr to prune this conditional for the equi-join implementation.
        if constexpr (keep_null_values) {
          // We assume that the relations have been swapped previously, so that the outer relation is the probing
          // relation.
          // Since we did not find a hash table, we know that there is no match in the build column for this partition.
          // Hence we are going to write NULL values for each row.

          pos_list_build_side_local.reserve(elements.size());
          pos_list_probe_side_local.reserve(elements.size());

          for (auto partition_offset = size_t{0}; partition_offset < elements.size(); ++partition_offset) {
            const auto& element = elements[partition_offset];
            pos_list_build_side_local.emplace_back(NULL_ROW_ID);
            pos_list_probe_side_local.emplace_back(element.row_id);
          }
        }
      }

      pos_lists_build_side[partition_idx] = std::move(pos_list_build_side_local);
      pos_lists_probe_side[partition_idx] = std::move(pos_list_probe_side_local);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

template <typename ProbeColumnType, typename HashedType, JoinMode mode>
void probe_semi_anti(const RadixContainer<ProbeColumnType>& probe_radix_container,
                     const std::vector<std::optional<PosHashTable<HashedType>>>& hash_tables,
                     std::vector<RowIDPosList>& pos_lists, const Table& build_table, const Table& probe_table,
                     const std::vector<OperatorJoinPredicate>& secondary_join_predicates) {
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(probe_radix_container.size());

  for (size_t partition_idx = 0; partition_idx < probe_radix_container.size(); ++partition_idx) {
    // Skip empty partitions to avoid empty output chunks
    if (probe_radix_container[partition_idx].elements.empty()) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, partition_idx]() {
      // Get information from work queue
      const auto& partition = probe_radix_container[partition_idx];
      const auto& elements = partition.elements;
      const auto& null_values = partition.null_values;

      RowIDPosList pos_list_local;

      const auto hash_table_idx = hash_tables.size() > 1 ? partition_idx : 0;
      if (!hash_tables.empty() && hash_tables.at(hash_table_idx)) {
        // Valid hash table found, so there is at least one match in this partition
        const auto& hash_table = *hash_tables[hash_table_idx];

        // Accessors are not thread-safe, so we create one evaluator per job
        MultiPredicateJoinEvaluator multi_predicate_join_evaluator(build_table, probe_table, mode,
                                                                   secondary_join_predicates);

        for (auto partition_offset = size_t{0}; partition_offset < elements.size(); ++partition_offset) {
          const auto& probe_column_element = elements[partition_offset];

          if constexpr (mode == JoinMode::Semi) {
            // NULLs on the probe side are never emitted
            if (probe_column_element.row_id.chunk_offset == INVALID_CHUNK_OFFSET) {
              // Could be either skipped or NULL
              continue;
            }
          } else if constexpr (mode == JoinMode::AntiNullAsFalse) {  // NOLINT - doesn't like `else if`
            // NULL values on the probe side always lead to the tuple being emitted for AntiNullAsFalse, irrespective
            // of secondary predicates (`NULL("as false") AND <anything>` is always false)
            if (null_values[partition_offset]) {
              pos_list_local.emplace_back(probe_column_element.row_id);
              continue;
            }
          } else if constexpr (mode == JoinMode::AntiNullAsTrue) {  // NOLINT - doesn't like `else if`
            if (null_values[partition_offset]) {
              // Primary predicate is TRUE, as long as we do not support secondary predicates with AntiNullAsTrue.
              // This means that the probe value never gets emitted
              continue;
            }
          }

          auto any_build_column_value_matches = false;

          if (secondary_join_predicates.empty()) {
            any_build_column_value_matches = hash_table.contains(static_cast<HashedType>(probe_column_element.value));
          } else {
            auto [primary_predicate_matching_rows_iter, primary_predicate_matching_rows_end] =
                hash_table.find(static_cast<HashedType>(probe_column_element.value));

            for (; primary_predicate_matching_rows_iter != primary_predicate_matching_rows_end;
                 ++primary_predicate_matching_rows_iter) {
              const auto row_id = *primary_predicate_matching_rows_iter;
              if (multi_predicate_join_evaluator.satisfies_all_predicates(row_id, probe_column_element.row_id)) {
                any_build_column_value_matches = true;
                break;
              }
            }
          }

          if ((mode == JoinMode::Semi && any_build_column_value_matches) ||
              ((mode == JoinMode::AntiNullAsTrue || mode == JoinMode::AntiNullAsFalse) &&
               !any_build_column_value_matches)) {
            pos_list_local.emplace_back(probe_column_element.row_id);
          }
        }
      } else if constexpr (mode == JoinMode::AntiNullAsFalse) {  // NOLINT - doesn't like `else if`
        // no hash table on other side, but we are in AntiNullAsFalse mode which means all tuples from the probing side
        // get emitted.
        pos_list_local.reserve(elements.size());
        for (auto partition_offset = size_t{0}; partition_offset < elements.size(); ++partition_offset) {
          auto& probe_column_element = elements[partition_offset];
          pos_list_local.emplace_back(probe_column_element.row_id);
        }
      } else if constexpr (mode == JoinMode::AntiNullAsTrue) {  // NOLINT - doesn't like `else if`
        // no hash table on other side, but we are in AntiNullAsTrue mode which means all tuples from the probing side
        // get emitted. That is, except NULL values, which only get emitted if the build table is empty.
        const auto build_table_is_empty = build_table.row_count() == 0;
        pos_list_local.reserve(elements.size());
        for (auto partition_offset = size_t{0}; partition_offset < elements.size(); ++partition_offset) {
          auto& probe_column_element = elements[partition_offset];
          // A NULL on the probe side never gets emitted, except when the build table is empty.
          // This is because `NULL NOT IN <empty list>` is actually true
          if (null_values[partition_offset] && !build_table_is_empty) {
            continue;
          }
          pos_list_local.emplace_back(probe_column_element.row_id);
        }
      }

      pos_lists[partition_idx] = std::move(pos_list_local);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

using PosLists = std::vector<std::shared_ptr<const AbstractPosList>>;
using PosListsByChunk = std::vector<std::shared_ptr<PosLists>>;

/**
 * Returns a vector where each entry with index i references a PosLists object. The PosLists object
 * contains the position list of every segment/chunk in column i.
 * @param input_table
 */
// See usage in _on_execute() for doc.
inline PosListsByChunk setup_pos_lists_by_chunk(const std::shared_ptr<const Table>& input_table) {
  Assert(input_table->type() == TableType::References, "Function only works for reference tables");

  std::map<PosLists, std::shared_ptr<PosLists>> shared_pos_lists_by_pos_lists;

  PosListsByChunk pos_lists_by_segment(input_table->column_count());
  auto pos_lists_by_segment_it = pos_lists_by_segment.begin();

  const auto input_chunks_count = input_table->chunk_count();
  const auto input_columns_count = input_table->column_count();

  // For every column, for every chunk
  for (ColumnID column_id{0}; column_id < input_columns_count; ++column_id) {
    // Get all the input pos lists so that we only have to pointer cast the segments once
    auto pos_list_ptrs = std::make_shared<PosLists>(input_table->chunk_count());
    auto pos_lists_iter = pos_list_ptrs->begin();

    // Iterate over every chunk and add the chunks segment with column_id to pos_list_ptrs
    for (ChunkID chunk_id{0}; chunk_id < input_chunks_count; ++chunk_id) {
      const auto chunk = input_table->get_chunk(chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      const auto& ref_segment_uncasted = chunk->get_segment(column_id);
      const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(ref_segment_uncasted);
      *pos_lists_iter = ref_segment->pos_list();
      ++pos_lists_iter;
    }

    // pos_list_ptrs contains all position lists of the reference segments for the column_id.
    auto iter = shared_pos_lists_by_pos_lists.emplace(*pos_list_ptrs, pos_list_ptrs).first;

    *pos_lists_by_segment_it = iter->second;
    ++pos_lists_by_segment_it;
  }

  return pos_lists_by_segment;
}

/**
 *
 * @param output_segments [in/out] Vector to which the newly created reference segments will be written.
 * @param input_table Table which all the position lists reference
 * @param input_pos_list_ptrs_sptrs_by_segments Contains all position lists to all columns of input table
 * @param pos_list contains the positions of rows to use from the input table
 */
inline void write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                                  const PosListsByChunk& input_pos_list_ptrs_sptrs_by_segments,
                                  std::shared_ptr<RowIDPosList> pos_list) {
  std::map<std::shared_ptr<PosLists>, std::shared_ptr<RowIDPosList>> output_pos_list_cache;

  // We might use this later, but want to have it outside of the for loop
  std::shared_ptr<Table> dummy_table;

  // Add segments from input table to output chunk
  // for every column for every row in pos_list: get corresponding PosList of input_pos_list_ptrs_sptrs_by_segments
  // and add it to new_pos_list which is added to output_segments
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        const auto& input_table_pos_lists = input_pos_list_ptrs_sptrs_by_segments[column_id];

        auto iter = output_pos_list_cache.find(input_table_pos_lists);
        if (iter == output_pos_list_cache.end()) {
          // Get the row ids that are referenced
          auto new_pos_list = std::make_shared<RowIDPosList>(pos_list->size());
          auto new_pos_list_iter = new_pos_list->begin();
          auto common_chunk_id = std::optional<ChunkID>{};
          for (const auto& row : *pos_list) {
            if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
              *new_pos_list_iter = row;
              common_chunk_id = INVALID_CHUNK_ID;
            } else {
              const auto& referenced_pos_list = *(*input_table_pos_lists)[row.chunk_id];
              *new_pos_list_iter = referenced_pos_list[row.chunk_offset];

              // Check if the current row matches the ChunkIDs that we have seen in previous rows
              const auto referenced_chunk_id = referenced_pos_list[row.chunk_offset].chunk_id;
              if (!common_chunk_id) {
                common_chunk_id = referenced_chunk_id;
              } else if (*common_chunk_id != referenced_chunk_id) {
                common_chunk_id = INVALID_CHUNK_ID;
              }
            }
            ++new_pos_list_iter;
          }
          if (common_chunk_id && *common_chunk_id != INVALID_CHUNK_ID) {
            // Track the occuring chunk ids and set the single chunk guarantee if possible. Generally, this is the case
            // if both of the following are true: (1) The probe side input already had this guarantee and (2) no radix
            // partitioning was used. If multiple small PosLists were merged (see MIN_SIZE in join_hash.cpp), this
            // guarantee cannot be given.
            new_pos_list->guarantee_single_chunk();
          }

          iter = output_pos_list_cache.emplace(input_table_pos_lists, new_pos_list).first;
        }

        auto reference_segment = std::static_pointer_cast<const ReferenceSegment>(
            input_table->get_chunk(ChunkID{0})->get_segment(column_id));
        output_segments.push_back(std::make_shared<ReferenceSegment>(
            reference_segment->referenced_table(), reference_segment->referenced_column_id(), iter->second));
      } else {
        // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencing to.
        // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceSegment that
        // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceSegment ref
        // it.
        if (!dummy_table) dummy_table = Table::create_dummy_table(input_table->column_definitions());
        output_segments.push_back(std::make_shared<ReferenceSegment>(dummy_table, column_id, pos_list));
      }
    } else {
      // Check if the PosList references a single chunk. This is easier than tracking the flag through materialization,
      // radix partitioning, and so on. Also, actually checking for this property instead of simply forwarding it may
      // allows us to set guarantee_single_chunk in more cases. In cases where more than one chunk is referenced, this
      // should be cheap. In the other cases, the cost of iterating through the PosList are likely to be amortized in
      // following operators. See the comment at the previous call of guarantee_single_chunk to understand when this
      // guarantee might not be given.
      // This is not part of PosList as other operators should have a better understanding of how they emit references.
      auto common_chunk_id = std::optional<ChunkID>{};
      for (const auto& row : *pos_list) {
        if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
          common_chunk_id = INVALID_CHUNK_ID;
          break;
        } else {
          if (!common_chunk_id) {
            common_chunk_id = row.chunk_id;
          } else if (*common_chunk_id != row.chunk_id) {
            common_chunk_id = INVALID_CHUNK_ID;
            break;
          }
        }
      }
      if (common_chunk_id && *common_chunk_id != INVALID_CHUNK_ID) {
        pos_list->guarantee_single_chunk();
      }

      output_segments.push_back(std::make_shared<ReferenceSegment>(input_table, column_id, pos_list));
    }
  }
}

}  // namespace opossum
