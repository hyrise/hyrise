#include "join_hash.hpp"

#include <boost/container/small_vector.hpp>
#include <boost/lexical_cast.hpp>

#include <cmath>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bytell_hash_map.hpp"
#include "join_hash/hash_traits.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/abstract_segment_visitor.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "type_cast.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"
#include "utils/uninitialized_vector.hpp"

namespace opossum {

JoinHash::JoinHash(const std::shared_ptr<const AbstractOperator>& left,
                   const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                   const ColumnIDPair& column_ids, const PredicateCondition predicate_condition,
                   const size_t radix_bits)
    : AbstractJoinOperator(OperatorType::JoinHash, left, right, mode, column_ids, predicate_condition),
      _radix_bits(radix_bits) {
  DebugAssert(predicate_condition == PredicateCondition::Equals, "Operator not supported by Hash Join.");
}

const std::string JoinHash::name() const { return "JoinHash"; }

std::shared_ptr<AbstractOperator> JoinHash::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinHash>(copied_input_left, copied_input_right, _mode, _column_ids, _predicate_condition);
}

void JoinHash::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinHash::_on_execute() {
  std::shared_ptr<const AbstractOperator> build_operator;
  std::shared_ptr<const AbstractOperator> probe_operator;
  ColumnID build_column_id;
  ColumnID probe_column_id;

  // This is the expected implementation for swapping tables:
  // (1) if left or right outer join, outer relation becomes probe relation (we have to swap only for left outer)
  // (2) for a semi and anti join the inputs are always swapped
  bool inputs_swapped = (_mode == JoinMode::Left || _mode == JoinMode::Anti || _mode == JoinMode::Semi);

  // (3) else the smaller relation will become build relation, the larger probe relation
  if (!inputs_swapped && _input_left->get_output()->row_count() > _input_right->get_output()->row_count()) {
    inputs_swapped = true;
  }

  if (inputs_swapped) {
    // luckily we don't have to swap the operation itself here, because we only support the commutative Equi Join.
    build_operator = _input_right;
    probe_operator = _input_left;
    build_column_id = _column_ids.second;
    probe_column_id = _column_ids.first;
  } else {
    build_operator = _input_left;
    probe_operator = _input_right;
    build_column_id = _column_ids.first;
    probe_column_id = _column_ids.second;
  }

  auto adjusted_column_ids = std::make_pair(build_column_id, probe_column_id);

  auto build_input = build_operator->get_output();
  auto probe_input = probe_operator->get_output();

  _impl = make_unique_by_data_types<AbstractReadOnlyOperatorImpl, JoinHashImpl>(
      build_input->column_data_type(build_column_id), probe_input->column_data_type(probe_column_id), build_operator,
      probe_operator, _mode, adjusted_column_ids, _predicate_condition, inputs_swapped, _radix_bits);
  return _impl->_on_execute();
}

void JoinHash::_on_cleanup() { _impl.reset(); }

using Hash = size_t;

/*
This is how elements of the input relations are saved after materialization.
The original value is used to detect hash collisions.
*/
template <typename T>
struct PartitionedElement {
  PartitionedElement() : row_id(NULL_ROW_ID), partition_hash(0), value(T()) {}
  PartitionedElement(RowID row, Hash hash, T val) : row_id(row), partition_hash(hash), value(val) {}

  RowID row_id;
  Hash partition_hash{0};
  T value;
};

// Initializing the partition vector takes some time. This is not necessary, because it will be overwritten anyway.
// The uninitialized_vector behaves like a regular std::vector, but the entries are initially invalid.
template <typename T>
using Partition = std::conditional_t<std::is_trivially_destructible_v<T>, uninitialized_vector<PartitionedElement<T>>,
                                     std::vector<PartitionedElement<T>>>;

// The small_vector holds the first n values in local storage and only resorts to heap storage after that. 1 is chosen
// as n because in many cases, we join on primary key attributes where by definition we have only one match on the
// smaller side.
using SmallPosList = boost::container::small_vector<RowID, 1>;

// In case we consider runtime to be more relevant, the flat hash map performs better (measured to be mostly on par
// with bytell hash map and in some cases up to 5% faster) but is significantly larger than the bytell hash map.
template <typename T>
using HashTable = ska::bytell_hash_map<T, SmallPosList>;

/*
This struct contains radix-partitioned data in a contiguous buffer,
as well as a list of offsets for each partition.
*/
template <typename T>
struct RadixContainer {
  std::shared_ptr<Partition<T>> elements;
  std::vector<size_t> partition_offsets;
};

/*
Build all the hash tables for the partitions of Left. We parallelize this process for all partitions of Left
*/
template <typename LeftType, typename HashedType>
std::vector<std::optional<HashTable<HashedType>>> build(const RadixContainer<LeftType>& radix_container) {
  /*
  NUMA notes:
  The hashtables for each partition P should also reside on the same node as the two vectors leftP and rightP.
  */
  std::vector<std::optional<HashTable<HashedType>>> hashtables;
  hashtables.resize(radix_container.partition_offsets.size() - 1);

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(radix_container.partition_offsets.size() - 1);

  for (size_t current_partition_id = 0; current_partition_id < (radix_container.partition_offsets.size() - 1);
       ++current_partition_id) {
    const auto partition_left_begin = radix_container.partition_offsets[current_partition_id];
    const auto partition_left_end = radix_container.partition_offsets[current_partition_id + 1];
    const auto partition_size = partition_left_end - partition_left_begin;

    // Prune empty partitions, so that we don't have too many empty hash tables
    if (partition_size == 0) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, partition_left_begin, partition_left_end, current_partition_id,
                                                 partition_size]() {
      auto& partition_left = static_cast<Partition<LeftType>&>(*radix_container.elements);
      // slightly oversize the hash table to avoid unnecessary rebuilds
      auto hashtable = HashTable<HashedType>(static_cast<size_t>(partition_size * 1.2));

      for (size_t partition_offset = partition_left_begin; partition_offset < partition_left_end; ++partition_offset) {
        auto& element = partition_left[partition_offset];

        auto casted_value = type_cast<HashedType>(std::move(element.value));
        auto it = hashtable.find(casted_value);
        if (it != hashtable.end()) {
          it->second.emplace_back(element.row_id);
        } else {
          hashtable.emplace(casted_value, SmallPosList{element.row_id});
        }
      }

      hashtables[current_partition_id] = std::move(hashtable);
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return std::move(hashtables);
}

template <typename T, typename HashedType>
std::shared_ptr<Partition<T>> materialize_input(const std::shared_ptr<const Table>& in_table, ColumnID column_id,
                                                std::vector<std::vector<size_t>>& histograms, const size_t radix_bits,
                                                bool keep_nulls = false) {
  // list of all elements that will be partitioned
  auto elements = std::make_shared<Partition<T>>(in_table->row_count());

  // fan-out
  const size_t num_partitions = 1ull << radix_bits;

  // currently, we just do one pass
  size_t pass = 0;
  size_t mask = static_cast<uint32_t>(pow(2, radix_bits * (pass + 1)) - 1);

  auto chunk_offsets = std::vector<size_t>(in_table->chunk_count());

  // fill work queue
  size_t output_offset = 0;
  for (ChunkID chunk_id{0}; chunk_id < in_table->chunk_count(); chunk_id++) {
    auto segment = in_table->get_chunk(chunk_id)->get_segment(column_id);

    chunk_offsets[chunk_id] = output_offset;
    output_offset += segment->size();
  }

  // create histograms per chunk
  histograms.resize(chunk_offsets.size());

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(in_table->chunk_count());

  for (ChunkID chunk_id{0}; chunk_id < in_table->chunk_count(); ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      // Get information from work queue
      auto output_offset = chunk_offsets[chunk_id];
      auto output_iterator = elements->begin() + output_offset;
      auto segment = in_table->get_chunk(chunk_id)->get_segment(column_id);

      // prepare histogram
      auto histogram = std::vector<size_t>(num_partitions);

      resolve_segment_type<T>(*segment, [&, chunk_id, keep_nulls](auto& typed_segment) {
        auto reference_chunk_offset = ChunkOffset{0};
        auto iterable = create_iterable_from_segment<T>(typed_segment);

        iterable.for_each([&, chunk_id, keep_nulls](const auto& value) {
          if (!value.is_null() || keep_nulls) {
            const Hash hashed_value = std::hash<HashedType>{}(type_cast<HashedType>(value.value()));

            /*
            For ReferenceSegments we do not use the RowIDs from the referenced tables.
            Instead, we use the index in the ReferenceSegment itself. This way we can later correctly dereference
            values from different inputs (important for Multi Joins).
            */
            if constexpr (std::is_same_v<std::decay<decltype(typed_segment)>, ReferenceSegment>) {
              *(output_iterator++) =
                  PartitionedElement<T>{RowID{chunk_id, reference_chunk_offset}, hashed_value, value.value()};
            } else {
              *(output_iterator++) =
                  PartitionedElement<T>{RowID{chunk_id, value.chunk_offset()}, hashed_value, value.value()};
            }

            const Hash radix = hashed_value & mask;
            histogram[radix]++;
          }
          // reference_chunk_offset is only used for ReferenceSegments
          if constexpr (std::is_same_v<std::decay<decltype(typed_segment)>, ReferenceSegment>) {
            reference_chunk_offset++;
          }
        });
      });

      if constexpr (std::is_same_v<Partition<T>, uninitialized_vector<PartitionedElement<T>>>) {  // NOLINT
        // Because the vector is uninitialized, we need to manually fill up all slots that we did not use
        auto output_offset_end = chunk_id < chunk_offsets.size() - 1 ? chunk_offsets[chunk_id + 1] : elements->size();
        while (output_iterator != elements->begin() + output_offset_end) {
          *(output_iterator++) = PartitionedElement<T>{};
        }
      }

      histograms[chunk_id] = std::move(histogram);
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return elements;
}

template <typename T>
RadixContainer<T> partition_radix_parallel(const std::shared_ptr<Partition<T>>& materialized,
                                           const std::shared_ptr<std::vector<size_t>>& chunk_offsets,
                                           std::vector<std::vector<size_t>>& histograms, const size_t radix_bits,
                                           bool keep_nulls = false) {
  // fan-out
  const size_t num_partitions = 1ull << radix_bits;

  // currently, we just do one pass
  size_t pass = 0;
  size_t mask = static_cast<uint32_t>(pow(2, radix_bits * (pass + 1)) - 1);

  // allocate new (shared) output
  auto output = std::make_shared<Partition<T>>();
  output->resize(materialized->size());

  auto& offsets = static_cast<std::vector<size_t>&>(*chunk_offsets);

  RadixContainer<T> radix_output;
  radix_output.elements = output;
  radix_output.partition_offsets.resize(num_partitions + 1);

  // use histograms to calculate partition offsets
  size_t offset = 0;
  std::vector<std::vector<size_t>> output_offsets_by_chunk(offsets.size(), std::vector<size_t>(num_partitions));
  for (size_t partition_id = 0; partition_id < num_partitions; ++partition_id) {
    radix_output.partition_offsets[partition_id] = offset;
    for (ChunkID chunk_id{0}; chunk_id < offsets.size(); ++chunk_id) {
      output_offsets_by_chunk[chunk_id][partition_id] = offset;
      offset += histograms[chunk_id][partition_id];
    }
  }
  radix_output.partition_offsets[num_partitions] = offset;

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(offsets.size());

  for (ChunkID chunk_id{0}; chunk_id < offsets.size(); ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      size_t input_offset = offsets[chunk_id];
      auto& output_offsets = output_offsets_by_chunk[chunk_id];

      size_t input_size = 0;
      if (chunk_id < offsets.size() - 1) {
        input_size = offsets[chunk_id + 1] - input_offset;
      } else {
        input_size = materialized->size() - input_offset;
      }

      auto& out = static_cast<Partition<T>&>(*output);
      for (size_t chunk_offset = input_offset; chunk_offset < input_offset + input_size; ++chunk_offset) {
        auto& element = (*materialized)[chunk_offset];

        if (!keep_nulls && element.row_id.chunk_offset == INVALID_CHUNK_OFFSET) {
          continue;
        }

        const size_t radix = element.partition_hash & mask;

        out[output_offsets[radix]++] = element;
      }
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return radix_output;
}

/*
  In the probe phase we take all partitions from the right partition, iterate over them and compare each join candidate
  with the values in the hash table. Since Left and Right are hashed using the same hash function, we can reduce the
  number of hash tables that need to be looked into to just 1.
  */
template <typename RightType, typename HashedType>
void probe(const RadixContainer<RightType>& radix_container,
           const std::vector<std::optional<HashTable<HashedType>>>& hashtables, std::vector<PosList>& pos_lists_left,
           std::vector<PosList>& pos_lists_right, const JoinMode mode) {
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(radix_container.partition_offsets.size() - 1);

  /*
    NUMA notes:
    At this point both input relations are partitioned using radix partitioning.
    Probing will be done per partition for both sides.
    Therefore, inputs for one partition should be located on the same NUMA node,
    and the job that probes that partition should also be on that NUMA node.
    */

  for (size_t current_partition_id = 0; current_partition_id < (radix_container.partition_offsets.size() - 1);
       ++current_partition_id) {
    const auto partition_begin = radix_container.partition_offsets[current_partition_id];
    const auto partition_end = radix_container.partition_offsets[current_partition_id + 1];

    // Skip empty partitions to avoid empty output chunks
    if (partition_begin == partition_end) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, partition_begin, partition_end, current_partition_id]() {
      // Get information from work queue
      auto& partition = static_cast<Partition<RightType>&>(*radix_container.elements);
      PosList pos_list_left_local;
      PosList pos_list_right_local;

      if (hashtables[current_partition_id].has_value()) {
        const auto& hashtable = hashtables.at(current_partition_id).value();

        // simple heuristic to estimate result size: half of the partition's rows will match
        // a more conservative pre-allocation would be the size of the left cluster
        const size_t expected_output_size = std::max(10.0, std::ceil((partition_end - partition_begin) / 2));
        pos_list_left_local.reserve(expected_output_size);
        pos_list_right_local.reserve(expected_output_size);

        for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
          auto& row = partition[partition_offset];

          if (mode == JoinMode::Inner && row.row_id.chunk_offset == INVALID_CHUNK_OFFSET) {
            continue;
          }

          const auto& rows_iter = hashtable.find(type_cast<HashedType>(row.value));

          if (rows_iter != hashtable.end()) {
            // Key exists, thus we have at least one hit
            const auto& matching_rows = rows_iter->second;
            for (const auto& row_id : matching_rows) {
              if (row_id.chunk_offset != INVALID_CHUNK_OFFSET) {
                pos_list_left_local.emplace_back(row_id);
                pos_list_right_local.emplace_back(row.row_id);
              }
            }
            // We assume that the relations have been swapped previously,
            // so that the outer relation is the probing relation.
          } else if (mode == JoinMode::Left || mode == JoinMode::Right) {
            pos_list_left_local.emplace_back(NULL_ROW_ID);
            pos_list_right_local.emplace_back(row.row_id);
          }
        }
      } else if (mode == JoinMode::Left || mode == JoinMode::Right) {
        /*
          We assume that the relations have been swapped previously,
          so that the outer relation is the probing relation.

          Since we did not find a proper hash table,
          we know that there is no match in Left for this partition.
          Hence we are going to write NULL values for each row.
          */

        pos_list_left_local.reserve(partition_end - partition_begin);
        pos_list_right_local.reserve(partition_end - partition_begin);

        for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
          auto& row = partition[partition_offset];
          pos_list_left_local.emplace_back(NULL_ROW_ID);
          pos_list_right_local.emplace_back(row.row_id);
        }
      }

      if (!pos_list_left_local.empty()) {
        pos_lists_left[current_partition_id] = std::move(pos_list_left_local);
        pos_lists_right[current_partition_id] = std::move(pos_list_right_local);
      }
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);
}

template <typename RightType, typename HashedType>
void probe_semi_anti(const RadixContainer<RightType>& radix_container,
                     const std::vector<std::optional<HashTable<HashedType>>>& hashtables,
                     std::vector<PosList>& pos_lists, const JoinMode mode) {
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(radix_container.partition_offsets.size() - 1);

  for (size_t current_partition_id = 0; current_partition_id < (radix_container.partition_offsets.size() - 1);
       ++current_partition_id) {
    const auto partition_begin = radix_container.partition_offsets[current_partition_id];
    const auto partition_end = radix_container.partition_offsets[current_partition_id + 1];

    // Skip empty partitions to avoid empty output chunks
    if (partition_begin == partition_end) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, partition_begin, partition_end, current_partition_id]() {
      // Get information from work queue
      auto& partition = static_cast<Partition<RightType>&>(*radix_container.elements);

      PosList pos_list_local;

      if (hashtables[current_partition_id].has_value()) {
        // Valid hashtable found, so there is at least one match in this partition

        for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
          auto& row = partition[partition_offset];

          if (row.row_id.chunk_offset == INVALID_CHUNK_OFFSET) {
            continue;
          }

          const auto& hashtable = hashtables[current_partition_id].value();
          const auto it = hashtable.find(type_cast<HashedType>(row.value));

          if ((mode == JoinMode::Semi && it != hashtable.end()) || (mode == JoinMode::Anti && it == hashtable.end())) {
            // Semi: found at least one match for this row -> match
            // Anti: no matching rows found -> match
            pos_list_local.emplace_back(row.row_id);
          }
        }
      } else if (mode == JoinMode::Anti) {
        // no hashtable on other side, but we are in Anti mode
        pos_list_local.reserve(partition_end - partition_begin);
        for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
          auto& row = partition[partition_offset];
          pos_list_local.emplace_back(row.row_id);
        }
      }

      if (!pos_list_local.empty()) {
        pos_lists[current_partition_id] = std::move(pos_list_local);
      }
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);
}

using PosLists = std::vector<std::shared_ptr<const PosList>>;
using PosListsBySegment = std::vector<std::shared_ptr<PosLists>>;

// See usage in _on_execute() for doc.
PosListsBySegment setup_pos_lists_by_segment(const std::shared_ptr<const Table>& input_table) {
  DebugAssert(input_table->type() == TableType::References, "Function only works for reference tables");

  std::map<PosLists, std::shared_ptr<PosLists>> shared_pos_lists_by_pos_lists;

  PosListsBySegment pos_lists_by_segment(input_table->column_count());
  auto pos_lists_by_segment_it = pos_lists_by_segment.begin();

  const auto& input_chunks = input_table->chunks();

  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    // Get all the input pos lists so that we only have to pointer cast the segments once
    auto pos_list_ptrs = std::make_shared<PosLists>(input_table->chunk_count());
    auto pos_lists_iter = pos_list_ptrs->begin();

    for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); chunk_id++) {
      const auto& ref_segment_uncasted = input_chunks[chunk_id]->segments()[column_id];
      const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(ref_segment_uncasted);
      *pos_lists_iter = ref_segment->pos_list();
      ++pos_lists_iter;
    }

    auto iter = shared_pos_lists_by_pos_lists.emplace(*pos_list_ptrs, pos_list_ptrs).first;

    *pos_lists_by_segment_it = iter->second;
    ++pos_lists_by_segment_it;
  }

  return pos_lists_by_segment;
}

void write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                           const PosListsBySegment& input_pos_list_ptrs_sptrs_by_segments,
                           std::shared_ptr<PosList> pos_list) {
  std::map<std::shared_ptr<PosLists>, std::shared_ptr<PosList>> output_pos_list_cache;

  // We might use this later, but want to have it outside of the for loop
  std::shared_ptr<Table> dummy_table;

  // Add segments from input table to output chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        const auto& input_table_pos_lists = input_pos_list_ptrs_sptrs_by_segments[column_id];

        auto iter = output_pos_list_cache.find(input_table_pos_lists);
        if (iter == output_pos_list_cache.end()) {
          // Get the row ids that are referenced
          auto new_pos_list = std::make_shared<PosList>(pos_list->size());
          auto new_pos_list_iter = new_pos_list->begin();
          for (const auto& row : *pos_list) {
            if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
              *new_pos_list_iter = row;
            } else {
              const auto& referenced_pos_list = *(*input_table_pos_lists)[row.chunk_id];
              *new_pos_list_iter = referenced_pos_list[row.chunk_offset];
            }
            ++new_pos_list_iter;
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
      output_segments.push_back(std::make_shared<ReferenceSegment>(input_table, column_id, pos_list));
    }
  }
}

template <typename LeftType, typename RightType>
class JoinHash::JoinHashImpl : public AbstractJoinOperatorImpl {
 public:
  JoinHashImpl(const std::shared_ptr<const AbstractOperator>& left,
               const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
               const ColumnIDPair& column_ids, const PredicateCondition predicate_condition, const bool inputs_swapped,
               const size_t radix_bits)
      : _left(left),
        _right(right),
        _mode(mode),
        _column_ids(column_ids),
        _predicate_condition(predicate_condition),
        _inputs_swapped(inputs_swapped) {
    /*
      Setting number of bits for radix clustering:
      The number of bits is used to create probe partitions with a size that can
      be expected to fit into the L2 cache.
      This should incorporate hardware knowledge, once available in Hyrise.
      As of now, we assume a L2 cache size of 256 KB.
      We estimate the size the following way:
        - we assume each key appears once (that is an overestimation space-wise, but we
        aim rather for a hash map that is slightly smaller than L2 than slightly larger)
        - each entry in the hash map is a data structure holding the actual value
        and the RowID
    */
    const auto build_relation_size = _left->get_output()->row_count();
    const auto probe_relation_size = _right->get_output()->row_count();

    if (build_relation_size > probe_relation_size) {
      /*
        Hash joins perform best for join relations with a small left join partner. In case the
        optimizer selects the hash join due to such a situation, but neglects that the
        input will be switched (e.g., due to the join type), the user will be warned.
      */
      std::string warning{"Left relation larger than right relation hash join"};
      warning += inputs_swapped ? " (input relations have been swapped)." : ".";
      PerformanceWarning(warning);
    }

    const auto l2_cache_size = 256'000;  // bytes

    // To get a pessimistic estimation (ensure that the hash table fits within the cache), we assume
    // that each value maps to a PosList with a single RowID. For the used small_vector's, we assume a
    // size of 2*RowID per PosList. For sizing, see comments:
    // https://probablydance.com/2018/05/28/a-new-fast-hash-table-in-response-to-googles-new-fast-hash-table/
    const auto complete_hash_map_size =
        // number of items in map
        (build_relation_size *
         // key + value (and one byte overhead, see link above)
         (sizeof(LeftType) + 2 * sizeof(RowID) + 1))
        // fill factor
        / 0.8;

    const auto adaption_factor = 2.0f;  // don't occupy the whole L2 cache
    const auto cluster_count = std::max(1.0, (adaption_factor * complete_hash_map_size) / l2_cache_size);

    _radix_bits = std::ceil(std::log2(cluster_count));
  }

 protected:
  const std::shared_ptr<const AbstractOperator> _left, _right;
  const JoinMode _mode;
  const ColumnIDPair _column_ids;
  const PredicateCondition _predicate_condition;
  const bool _inputs_swapped;

  std::shared_ptr<Table> _output_table;

  size_t _radix_bits;

  // Determine correct type for hashing
  using HashedType = typename JoinHashTraits<LeftType, RightType>::HashType;

  std::shared_ptr<const Table> _on_execute() override {
    /*
    Preparing output table by adding columns from left table.
    */
    TableColumnDefinitions output_column_definitions;

    auto right_in_table = _right->get_output();
    auto left_in_table = _left->get_output();

    if (_inputs_swapped) {
      // Semi/Anti joins are always swapped but do not need the outer relation
      if (_mode == JoinMode::Semi || _mode == JoinMode::Anti) {
        output_column_definitions = right_in_table->column_definitions();
      } else {
        output_column_definitions =
            concatenated(right_in_table->column_definitions(), left_in_table->column_definitions());
      }
    } else {
      output_column_definitions =
          concatenated(left_in_table->column_definitions(), right_in_table->column_definitions());
    }

    _output_table = std::make_shared<Table>(output_column_definitions, TableType::References);

    /*
     * This flag is used in the materialization and probing phases.
     * When dealing with an OUTER join, we need to make sure that we keep the NULL values for the outer relation.
     * In the current implementation, the relation on the right is always the outer relation.
     */
    auto keep_nulls = (_mode == JoinMode::Left || _mode == JoinMode::Right);

    // Pre-partitioning
    // Save chunk offsets into the input relation
    size_t left_chunk_count = left_in_table->chunk_count();
    size_t right_chunk_count = right_in_table->chunk_count();

    auto left_chunk_offsets = std::make_shared<std::vector<size_t>>();
    auto right_chunk_offsets = std::make_shared<std::vector<size_t>>();

    left_chunk_offsets->resize(left_chunk_count);
    right_chunk_offsets->resize(right_chunk_count);

    size_t offset_left = 0;
    for (ChunkID i{0}; i < left_chunk_count; ++i) {
      left_chunk_offsets->operator[](i) = offset_left;
      offset_left += left_in_table->get_chunk(i)->size();
    }

    size_t offset_right = 0;
    for (ChunkID i{0}; i < right_chunk_count; ++i) {
      right_chunk_offsets->operator[](i) = offset_right;
      offset_right += right_in_table->get_chunk(i)->size();
    }

    Timer performance_timer;

    // Materialization phase
    std::vector<std::vector<size_t>> histograms_left;
    std::vector<std::vector<size_t>> histograms_right;
    /*
    NUMA notes:
    The materialized vectors don't have any strong NUMA preference because they haven't been partitioned yet.
    However, it would be a good idea to keep each materialized vector on one node if possible.
    This helps choosing a scheduler node for the radix phase (see below).
    */
    // Scheduler note: parallelize this at some point. Currently, the amount of jobs would be too high
    auto materialized_left = materialize_input<LeftType, HashedType>(left_in_table, _column_ids.first, histograms_left,
                                                                     _radix_bits);
    // 'keep_nulls' makes sure that the relation on the right materializes NULL values when executing an OUTER join.
    auto materialized_right = materialize_input<RightType, HashedType>(
        right_in_table, _column_ids.second, histograms_right, _radix_bits, keep_nulls);

    // Radix Partitioning phase
    /*
    NUMA notes:
    If the input vectors (the materialized vectors) reside on a specific node, the partitioning worker for
    this phase should be scheduled on the same node.
    Additionally, the output vectors in this phase are partitioned by a radix key. Therefore it would be good
    to pin the outputs from both sides on the same node for each radix partition. For example, if there are
    only two radix partitions A and B, the partitions leftA and rightA should be on the same node, and the
    partitions leftB and leftB should also be on the same node.
    */
    // Scheduler note: parallelize this at some point. Currently, the amount of jobs would be too high
    auto radix_left =
        partition_radix_parallel<LeftType>(materialized_left, left_chunk_offsets, histograms_left, _radix_bits);
    // 'keep_nulls' makes sure that the relation on the right keeps NULL values when executing an OUTER join.
    auto radix_right = partition_radix_parallel<RightType>(materialized_right, right_chunk_offsets, histograms_right,
                                                           _radix_bits, keep_nulls);

    // Build phase
    auto hashtables = build<LeftType, HashedType>(radix_left);

    // Probe phase
    std::vector<PosList> left_pos_lists;
    std::vector<PosList> right_pos_lists;
    const size_t partition_count = radix_right.partition_offsets.size() - 1;
    left_pos_lists.resize(partition_count);
    right_pos_lists.resize(partition_count);
    for (size_t i = 0; i < partition_count; i++) {
      // simple heuristic: half of the rows of the right relation will match
      const size_t result_rows_per_partition = _right->get_output()->row_count() / partition_count / 2;

      left_pos_lists[i].reserve(result_rows_per_partition);
      right_pos_lists[i].reserve(result_rows_per_partition);
    }
    /*
    NUMA notes:
    The workers for each radix partition P should be scheduled on the same node as the input data:
    leftP, rightP and hashtableP.
    */
    if (_mode == JoinMode::Semi || _mode == JoinMode::Anti) {
      probe_semi_anti<RightType, HashedType>(radix_right, hashtables, right_pos_lists, _mode);
    } else {
      probe<RightType, HashedType>(radix_right, hashtables, left_pos_lists, right_pos_lists, _mode);
    }

    auto only_output_right_input = _inputs_swapped && (_mode == JoinMode::Semi || _mode == JoinMode::Anti);

    /**
     * Two Caches to avoid redundant reference materialization for Reference input tables. As there might be
     *  quite a lot Partitions (>500 seen), input Chunks (>500 seen), and columns (>50 seen), this speeds up
     *  write_output_chunks a lot.
     *
     * They do two things:
     *      - Make it possible to re-use output pos lists if two segments in the input table have exactly the same
     *          PosLists Chunk by Chunk
     *      - Avoid creating the std::vector<const PosList*> for each Partition over and over again.
     *
     * They hold one entry per column in the table, not per BaseSegment in a single chunk
     */
    PosListsBySegment left_pos_lists_by_segment;
    PosListsBySegment right_pos_lists_by_segment;

    // left_pos_lists_by_segment will only be needed if left is a reference table and being output
    if (left_in_table->type() == TableType::References && !only_output_right_input) {
      left_pos_lists_by_segment = setup_pos_lists_by_segment(left_in_table);
    }

    // right_pos_lists_by_segment will only be needed if right is a reference table
    if (right_in_table->type() == TableType::References) {
      right_pos_lists_by_segment = setup_pos_lists_by_segment(right_in_table);
    }

    for (size_t partition_id = 0; partition_id < left_pos_lists.size(); ++partition_id) {
      // moving the values into a shared pos list saves us some work in write_output_segments. We know that
      // left_pos_lists and right_pos_lists will not be used again.
      auto left = std::make_shared<PosList>(std::move(left_pos_lists[partition_id]));
      auto right = std::make_shared<PosList>(std::move(right_pos_lists[partition_id]));

      if (left->empty() && right->empty()) {
        continue;
      }

      Segments output_segments;

      // we need to swap back the inputs, so that the order of the output columns is not harmed
      if (_inputs_swapped) {
        write_output_segments(output_segments, right_in_table, right_pos_lists_by_segment, right);

        // Semi/Anti joins are always swapped but do not need the outer relation
        if (!only_output_right_input) {
          write_output_segments(output_segments, left_in_table, left_pos_lists_by_segment, left);
        }
      } else {
        write_output_segments(output_segments, left_in_table, left_pos_lists_by_segment, left);
        write_output_segments(output_segments, right_in_table, right_pos_lists_by_segment, right);
      }

      _output_table->append_chunk(output_segments);
    }

    return _output_table;
  }
};

}  // namespace opossum
