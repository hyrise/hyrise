#pragma once

#include <boost/container/small_vector.hpp>
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

// For semi and anti joins, we only care whether a value exists or not, so there is no point in tracking the position
// in the input table of more than one occurrence of a value. However, if we have secondary predicates, we do need to
// track all occurrences of a value as that first position might be disqualified later.
enum class JoinHashBuildMode { AllPositions, SinglePosition };

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

// Stores the mapping from HashedType to positions. Conceptually, this is similar to an (unordered_)multimap, but it
// has some optimizations for the performance-critical probe() method. Instead of storing the matches directly in the
// hashmap (think map<HashedType, PosList>), we store an offset. This keeps the hashmap small and makes it easier to
// cache.
template <typename HashedType>
class PosHashTable {
  // In case we consider runtime to be more relevant, the flat hash map performs better (measured to be mostly on par
  // with bytell hash map and in some cases up to 5% faster) but is significantly larger than the bytell hash map.
  //
  // The hash table stores the relative offset of a SmallPosList (see below) in the vector of SmallPosLists. The range
  // of the offset does not limit the number of rows in the partition but the number of distinct values. If we end up
  // with a partition that has more values, the partitioning algorithm is at fault.
  using Offset = uint32_t;
  using HashTable = ska::bytell_hash_map<HashedType, Offset>;

  // The small_vector holds the first n values in local storage and only resorts to heap storage after that. 1 is chosen
  // as n because in many cases, we join on primary key attributes where by definition we have only one match on the
  // smaller side.
  using SmallPosList = boost::container::small_vector<RowID, 1>;

 public:
  explicit PosHashTable(const JoinHashBuildMode mode, const size_t max_size)
      : _hash_table(), _pos_lists(max_size + 1), _mode(mode) {
    // _pos_lists is initialized with an additional element to make the enforcement of the assertions easier.
    _hash_table.reserve(max_size);
  }

  // For a value seen on the build side, add the value to the hash map.
  // The row id is only added in the AllPositions mode. For SinglePosition, as used e.g., by semi joins, the actual
  // row id is irrelevant and is not stored. As such, while find() would return an iterator, the row ids retrieved
  // by dereferencing that iterator are incomplete. To keep people from accidentally dereferencing that iterator, we
  // prohibit find() and require the use of contains() in the SinglePosition mode.
  template <typename InputType>
  void emplace(const InputType& value, RowID row_id) {
    const auto casted_value = static_cast<HashedType>(value);

    // If casted_value is already present in the hash table, this returns an iterator to the existing value. If not, it
    // inserts a mapping from casted_value to the index into _values, which is defined by the previously inserted
    // number of values.
    const auto it = _hash_table.emplace(casted_value, _hash_table.size());
    if (_mode == JoinHashBuildMode::AllPositions) {
      auto& pos_list = _pos_lists[it.first->second];
      pos_list.emplace_back(row_id);

      DebugAssert(_hash_table.size() < _pos_lists.size(), "Hash table too big for pre-allocated data structures");
      DebugAssert(_hash_table.size() < std::numeric_limits<Offset>::max(), "Hash table too big for offset");
    }
  }

  void shrink_to_fit() {
    _pos_lists.resize(_hash_table.size());
    _pos_lists.shrink_to_fit();
    for (auto& pos_list : _pos_lists) {
      pos_list.shrink_to_fit();
    }

    // For very small hash tables, a linear search performs better. In that case, replace the hash table with a vector
    // of value/offset pairs.  The boundary was determined experimentally and chosen conservatively.
    if (_hash_table.size() <= 10) {
      Assert(!_values, "shrink_to_fit called twice");

      _values = std::vector<std::pair<HashedType, Offset>>{};
      _values->reserve(_hash_table.size());
      for (const auto& [value, offset] : _hash_table) {
        _values->emplace_back(std::pair<HashedType, Offset>{value, offset});
      }
      _hash_table.clear();
    } else {
      _hash_table.shrink_to_fit();
    }
  }

  // For a value seen on the probe side, return an iterator into the matching positions on the build side
  template <typename InputType>
  const std::vector<SmallPosList>::const_iterator find(const InputType& value) const {
    DebugAssert(_mode == JoinHashBuildMode::AllPositions, "find is invalid for SinglePosition mode, use contains");

    const auto casted_value = static_cast<HashedType>(value);
    if (!_values) {
      const auto hash_table_iter = _hash_table.find(casted_value);
      if (hash_table_iter == _hash_table.end()) return end();
      return _pos_lists.begin() + hash_table_iter->second;
    } else {
      const auto values_iter =
          std::find_if(_values->begin(), _values->end(), [&](const auto& pair) { return pair.first == casted_value; });
      if (values_iter == _values->end()) return end();
      return _pos_lists.begin() + values_iter->second;
    }
  }

  // For a value seen on the probe side, return whether it has been seen on the build side
  template <typename InputType>
  bool contains(const InputType& value) const {
    const auto casted_value = static_cast<HashedType>(value);

    if (!_values) {
      return _hash_table.find(casted_value) != _hash_table.end();
    } else {
      const auto values_iter =
          std::find_if(_values->begin(), _values->end(), [&](const auto& pair) { return pair.first == casted_value; });
      return values_iter != _values->end();
    }
  }

  const std::vector<SmallPosList>::const_iterator begin() const { return _pos_lists.begin(); }

  const std::vector<SmallPosList>::const_iterator end() const { return _pos_lists.end(); }

 private:
  HashTable _hash_table;
  std::vector<SmallPosList> _pos_lists;
  JoinHashBuildMode _mode;
  std::optional<std::vector<std::pair<HashedType, Offset>>> _values{std::nullopt};
};

template <typename T, typename HashedType, bool keep_null_values>
RadixContainer<T> materialize_input(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                    std::vector<std::vector<size_t>>& histograms, const size_t radix_bits) {
  // Retrieve input chunk_count as it might change during execution if we work on a non-reference table
  auto chunk_count = in_table->chunk_count();

  const std::hash<HashedType> hash_function;
  // list of all elements that will be partitioned
  auto radix_container = RadixContainer<T>{};
  radix_container.resize(chunk_count);

  // fan-out
  const size_t num_radix_partitions = 1ull << radix_bits;

  // currently, we just do one pass
  const auto pass = size_t{0};
  const auto radix_mask = static_cast<size_t>(pow(2, radix_bits * (pass + 1)) - 1);

  // create histograms per chunk
  histograms.resize(chunk_count);

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(chunk_count);

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    if (!in_table->get_chunk(chunk_id)) continue;

    jobs.emplace_back(std::make_shared<JobTask>([&, in_table, chunk_id]() {
      const auto chunk_in = in_table->get_chunk(chunk_id);

      // Skip chunks that were physically deleted
      if (!chunk_in) return;

      auto& elements = radix_container[chunk_id].elements;
      auto& null_values = radix_container[chunk_id].null_values;

      elements.resize(chunk_in->size());
      if constexpr (keep_null_values) {
        null_values.resize(chunk_in->size());
      }

      auto elements_iter = elements.begin();
      [[maybe_unused]] auto null_values_iter = null_values.begin();

      // prepare histogram
      auto histogram = std::vector<size_t>(num_radix_partitions);

      auto reference_chunk_offset = ChunkOffset{0};

      const auto segment = chunk_in->get_segment(column_id);
      segment_with_iterators<T>(*segment, [&](auto it, const auto end) {
        using IterableType = typename decltype(it)::IterableType;

        while (it != end) {
          const auto& value = *it;

          if (!value.is_null() || keep_null_values) {
            // TODO(anyone): static_cast is almost always safe, since HashType is big enough. Only for double-vs-long
            // joins an information loss is possible when joining with longs that cannot be losslessly converted to
            // double. See #1550 for details.
            const Hash hashed_value = hash_function(static_cast<HashedType>(value.value()));

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

          // reference_chunk_offset is only used for ReferenceSegments
          if constexpr (is_reference_segment_iterable_v<IterableType>) {
            ++reference_chunk_offset;
          }

          ++it;

          if (elements_iter == elements.end()) {
            // The last chunk has changed its size since we allocated elements. This is due to a concurrent insert
            // into that chunk. In any case, those inserts will not be visible to our current transaction, so we can
            // ignore them.
            break;
          }
        }
      });

      // elements was allocated with the size of the chunk. As we might have skipped NULL values, we need to resize the
      // vector to the number of values actually written.
      elements.resize(std::distance(elements.begin(), elements_iter));

      histograms[chunk_id] = std::move(histogram);
    }));
    jobs.back()->schedule();
  }
  Hyrise::get().scheduler()->wait_for_tasks(jobs);

  return radix_container;
}

/*
Build all the hash tables for the partitions of the build column. One job per partition
*/

template <typename BuildColumnType, typename HashedType>
std::vector<std::optional<PosHashTable<HashedType>>> build(const RadixContainer<BuildColumnType>& radix_container,
                                                           const JoinHashBuildMode mode, const size_t radix_bits) {
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
    hash_tables = {PosHashTable<HashedType>(mode, total_size)};
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

    const auto insert_into_hash_table = [&, partition_idx]() {
      const auto hash_table_idx = radix_bits > 0 ? partition_idx : 0;
      const auto& elements = radix_container[partition_idx].elements;

      auto& hash_table = hash_tables[hash_table_idx];
      if (radix_bits > 0) {
        hash_table = PosHashTable<HashedType>(mode, elements.size());
      }
      for (const auto& element : elements) {
        DebugAssert(!(element.row_id == NULL_ROW_ID), "No NULL_ROW_IDs should make it to this point");

        hash_table->emplace(element.value, element.row_id);
      }

      if (radix_bits > 0) {
        // In case only a single hash table is built, shrink to fit is called outside of the loop.
        hash_table->shrink_to_fit();
      }
    };

    if (radix_bits == 0) {
      // Without radix partitioning, only a single hash table will be written. Parallelizing this would require a
      // concurrent hash table, which is likely more expensive.
      insert_into_hash_table();
    } else {
      jobs.emplace_back(std::make_shared<JobTask>(insert_into_hash_table));
      jobs.back()->schedule();
    }
  }
  Hyrise::get().scheduler()->wait_for_tasks(jobs);

  // If radix partitioning is used, shrink_to_fit is called above.
  if (radix_bits == 0) hash_tables[0]->shrink_to_fit();

  return hash_tables;
}

template <typename T, typename HashedType, bool keep_null_values>
RadixContainer<T> partition_by_radix(const RadixContainer<T>& radix_container,
                                     std::vector<std::vector<size_t>>& histograms, const size_t radix_bits) {
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
    jobs.back()->schedule();
  }
  Hyrise::get().scheduler()->wait_for_tasks(jobs);
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
      jobs.back()->schedule();
    }
    Hyrise::get().scheduler()->wait_for_tasks(jobs);
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
           std::vector<PosList>& pos_lists_build_side, std::vector<PosList>& pos_lists_probe_side, const JoinMode mode,
           const Table& build_table, const Table& probe_table,
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

      PosList pos_list_build_side_local;
      PosList pos_list_probe_side_local;

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

          const auto& primary_predicate_matching_rows =
              hash_table.find(static_cast<HashedType>(probe_column_element.value));

          if (primary_predicate_matching_rows != hash_table.end()) {
            // Key exists, thus we have at least one hit for the primary predicate

            // Since we cannot store NULL values directly in off-the-shelf containers,
            // we need to the check the NULL bit vector here because a NULL value (represented
            // as a zero) yields the same rows as an actual zero value.
            // For inner joins, we skip NULL values and output them for outer joins.
            // Note, if the materialization/radix partitioning phase did not explicitly consider
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
              for (const auto& row_id : *primary_predicate_matching_rows) {
                pos_list_build_side_local.emplace_back(row_id);
                pos_list_probe_side_local.emplace_back(probe_column_element.row_id);
              }
            } else {
              auto match_found = false;
              for (const auto& row_id : *primary_predicate_matching_rows) {
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
    jobs.back()->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);
}

template <typename ProbeColumnType, typename HashedType, JoinMode mode>
void probe_semi_anti(const RadixContainer<ProbeColumnType>& probe_radix_container,
                     const std::vector<std::optional<PosHashTable<HashedType>>>& hash_tables,
                     std::vector<PosList>& pos_lists, const Table& build_table, const Table& probe_table,
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

      PosList pos_list_local;

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
          } else if constexpr (mode == JoinMode::AntiNullAsFalse) {  // NOLINT - doesn't like else if constexpr
            // NULL values on the probe side always lead to the tuple being emitted for AntiNullAsFalse, irrespective
            // of secondary predicates (`NULL("as false") AND <anything>` is always false)
            if (null_values[partition_offset]) {
              pos_list_local.emplace_back(probe_column_element.row_id);
              continue;
            }
          } else if constexpr (mode == JoinMode::AntiNullAsTrue) {  // NOLINT - doesn't like else if constexpr
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
            const auto primary_predicate_matching_rows =
                hash_table.find(static_cast<HashedType>(probe_column_element.value));

            if (primary_predicate_matching_rows != hash_table.end()) {
              for (const auto& row_id : *primary_predicate_matching_rows) {
                if (multi_predicate_join_evaluator.satisfies_all_predicates(row_id, probe_column_element.row_id)) {
                  any_build_column_value_matches = true;
                  break;
                }
              }
            }
          }

          if ((mode == JoinMode::Semi && any_build_column_value_matches) ||
              ((mode == JoinMode::AntiNullAsTrue || mode == JoinMode::AntiNullAsFalse) &&
               !any_build_column_value_matches)) {
            pos_list_local.emplace_back(probe_column_element.row_id);
          }
        }
      } else if constexpr (mode == JoinMode::AntiNullAsFalse) {  // NOLINT - doesn't like else if constexpr
        // no hash table on other side, but we are in AntiNullAsFalse mode which means all tuples from the probing side
        // get emitted.
        pos_list_local.reserve(elements.size());
        for (auto partition_offset = size_t{0}; partition_offset < elements.size(); ++partition_offset) {
          auto& probe_column_element = elements[partition_offset];
          pos_list_local.emplace_back(probe_column_element.row_id);
        }
      } else if constexpr (mode == JoinMode::AntiNullAsTrue) {  // NOLINT - doesn't like else if constexpr
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
    jobs.back()->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);
}

using PosLists = std::vector<std::shared_ptr<const PosList>>;
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
                                  std::shared_ptr<PosList> pos_list) {
  std::map<std::shared_ptr<PosLists>, std::shared_ptr<PosList>> output_pos_list_cache;

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
          auto new_pos_list = std::make_shared<PosList>(pos_list->size());
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
      // following operators.
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
