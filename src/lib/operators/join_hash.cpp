#include "join_hash.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "product.hpp"

#include "storage/column_visitable.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinHash::JoinHash(const std::shared_ptr<const AbstractOperator> left,
                   const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                   const std::pair<ColumnID, ColumnID> &column_ids, const ScanType scan_type)
    : AbstractJoinOperator(left, right, mode, column_ids, scan_type) {
  DebugAssert(scan_type == ScanType::OpEquals, "Operator not supported by Hash Join.");
}

const std::string JoinHash::name() const { return "JoinHash"; }

uint8_t JoinHash::num_in_tables() const { return 2; }

uint8_t JoinHash::num_out_tables() const { return 1; }

std::shared_ptr<const Table> JoinHash::_on_execute() {
  std::shared_ptr<const AbstractOperator> build_operator;
  std::shared_ptr<const AbstractOperator> probe_operator;
  bool inputs_swapped;
  ColumnID build_column_id;
  ColumnID probe_column_id;

  /*
  This is the expected implementation for swapping tables:
  (1) if left or right outer join, outer relation becomes probe relation (we have to swap only for left outer)
  (2) else the smaller relation will become build relation, the larger probe relation
  (3) for full outer joins we currently don't have an implementation.
  */
  if (_mode == JoinMode::Left || (_mode != JoinMode::Right &&
                                  (_input_left->get_output()->row_count() > _input_right->get_output()->row_count()))) {
    // luckily we don't have to swap the operation itself here, because we only support the commutative Equi Join.
    inputs_swapped = true;
    build_operator = _input_right;
    probe_operator = _input_left;
    build_column_id = _column_ids.second;
    probe_column_id = _column_ids.first;
  } else {
    inputs_swapped = false;
    build_operator = _input_left;
    probe_operator = _input_right;
    build_column_id = _column_ids.first;
    probe_column_id = _column_ids.second;
  }

  auto adjusted_column_ids = std::make_pair(build_column_id, probe_column_id);

  auto build_input = build_operator->get_output();
  auto probe_input = probe_operator->get_output();

  _impl = make_unique_by_column_types<AbstractReadOnlyOperatorImpl, JoinHashImpl>(
      build_input->column_type(build_column_id), probe_input->column_type(probe_column_id), build_operator,
      probe_operator, _mode, adjusted_column_ids, _scan_type, inputs_swapped);
  return _impl->_on_execute();
}

void JoinHash::_on_cleanup() { _impl.reset(); }

// currently using 32bit Murmur
using Hash = uint32_t;

// We need to use the impl pattern because the join operator depends on the type of the columns
template <typename LeftType, typename RightType>
class JoinHash::JoinHashImpl : public AbstractJoinOperatorImpl {
 public:
  JoinHashImpl(const std::shared_ptr<const AbstractOperator> left, const std::shared_ptr<const AbstractOperator> right,
               const JoinMode mode, const std::pair<ColumnID, ColumnID> &column_ids, const ScanType scan_type,
               const bool inputs_swapped)
      : _left(left),
        _right(right),
        _mode(mode),
        _column_ids(column_ids),
        _scan_type(scan_type),
        _inputs_swapped(inputs_swapped),
        _output_table(std::make_shared<Table>()) {
    // Setting comparator to Equal Comparison -> That is the only supported comparison type for Hash Joins
    _comparator = [](LeftType left_value, RightType right_value) { return value_equal(left_value, right_value); };
  }

  virtual ~JoinHashImpl() = default;

 protected:
  const std::shared_ptr<const AbstractOperator> _left, _right;
  const JoinMode _mode;
  const std::pair<ColumnID, ColumnID> _column_ids;
  const ScanType _scan_type;

  const bool _inputs_swapped;
  const std::shared_ptr<Table> _output_table;
  std::function<bool(LeftType, RightType)> _comparator;

  const unsigned int _partitioning_seed = 13;
  const size_t _radix_bits = 9;

  /*
  This is how elements of the input relations are saved after materialization.
  The original value is used to detect hash collisions.
  */
  template <typename T>
  struct PartitionedElement {
    PartitionedElement() : row_id(RowID{ChunkID{0}, 0}), partition_hash(0), value(T()) {}
    PartitionedElement(RowID row, Hash hash, T val) : row_id(row), partition_hash(hash), value(val) {}

    RowID row_id;
    Hash partition_hash;
    T value;
  };

  template <typename T>
  using Partition = std::vector<PartitionedElement<T>>;

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
  Visitor approach for materialization of columns
  */
  template <typename T>
  struct ColumnBuilder : public ColumnVisitable {
    explicit ColumnBuilder(ChunkID chunk_id, std::shared_ptr<std::vector<ChunkOffset>> offsets = nullptr)
        : _chunk_id(chunk_id),
          _materialized_chunk(std::make_shared<pmr_vector<std::pair<RowID, T>>>()),
          _offsets(offsets) {}

    void handle_value_column(BaseColumn &column, std::shared_ptr<ColumnVisitableContext>) override {
      auto &vc_column = static_cast<ValueColumn<T> &>(column);
      _materialized_chunk = vc_column.materialize(_chunk_id, _offsets);
    }
    void handle_dictionary_column(BaseColumn &column, std::shared_ptr<ColumnVisitableContext>) override {
      auto &dict_column = static_cast<DictionaryColumn<T> &>(column);
      _materialized_chunk = dict_column.materialize(_chunk_id, _offsets);
    }
    void handle_reference_column(ReferenceColumn &ref_column, std::shared_ptr<ColumnVisitableContext>) override {
      const auto referenced_table = ref_column.referenced_table();

      /*
      The pos_list might be unsorted. In that case, we would have to jump around from chunk to chunk.
      One-chunk-at-a-time processing should be faster. For this, we place a pair {chunk_offset, original_position}
      into a vector for each chunk. A potential optimization would be to only do this if the pos_list is really
      unsorted.
      Compare with table scan implemention for further reference.
      */

      std::vector<std::shared_ptr<std::vector<ChunkOffset>>> all_chunk_offsets(referenced_table->chunk_count());

      for (ChunkID chunk_id{0}; chunk_id < referenced_table->chunk_count(); ++chunk_id) {
        all_chunk_offsets[chunk_id] = std::make_shared<std::vector<ChunkOffset>>();
      }

      for (auto &pos : *(ref_column.pos_list())) {
        all_chunk_offsets[pos.chunk_id]->emplace_back(pos.chunk_offset);
      }

      for (ChunkID chunk_id{0}; chunk_id < referenced_table->chunk_count(); ++chunk_id) {
        if (all_chunk_offsets[chunk_id]->empty()) {
          continue;
        }
        auto &chunk = referenced_table->get_chunk(chunk_id);
        auto referenced_column = chunk.get_column(ref_column.referenced_column_id());

        ColumnBuilder<T> builder = ColumnBuilder<T>(chunk_id, all_chunk_offsets[chunk_id]);
        referenced_column->visit(builder);

        auto new_materialized_chunk = builder._materialized_chunk;

        /*
        We want to get the whole ReferenceColumn chunk, even if it actually consists of several Value or
        DictionaryColumn chunks.
        That's why we need to store intermediate results and append to the result vector.
        */
        if (_materialized_chunk) {
          _materialized_chunk->reserve(_materialized_chunk->size() + new_materialized_chunk->size());
          _materialized_chunk->insert(std::end(*_materialized_chunk), std::begin(*new_materialized_chunk),
                                      std::end(*new_materialized_chunk));

        } else {
          _materialized_chunk = new_materialized_chunk;
        }
      }
    }

    ChunkID _chunk_id;
    std::shared_ptr<pmr_vector<std::pair<RowID, T>>> _materialized_chunk;
    std::shared_ptr<std::vector<ChunkOffset>> _offsets;
  };

  template <typename T>
  std::shared_ptr<Partition<T>> _materialize_input(const std::shared_ptr<const Table> in_table, ColumnID column_id,
                                                   std::vector<std::shared_ptr<std::vector<size_t>>> &histograms) {
    // list of all elements that will be partitioned
    auto elements = std::make_shared<Partition<T>>();
    elements->resize(in_table->row_count());

    // arbitrary seed for the first hash iteration
    unsigned int seed = _partitioning_seed;

    // fan-out
    const size_t num_partitions = 1 << _radix_bits;

    // currently, we just do one pass
    size_t pass = 0;
    size_t mask = static_cast<uint32_t>(pow(2, _radix_bits * (pass + 1)) - 1);

    auto chunk_offsets = std::vector<size_t>(in_table->chunk_count());

    // fill work queue
    size_t output_offset = 0;
    for (ChunkID chunk_id{0}; chunk_id < in_table->chunk_count(); chunk_id++) {
      auto column = in_table->get_chunk(chunk_id).get_column(column_id);

      chunk_offsets[chunk_id] = output_offset;
      output_offset += column->size();
    }

    // create histograms per chunk
    histograms = std::vector<std::shared_ptr<std::vector<size_t>>>();
    histograms.resize(chunk_offsets.size());

    std::vector<std::shared_ptr<AbstractTask>> jobs;
    jobs.reserve(in_table->chunk_count());

    for (ChunkID chunk_id{0}; chunk_id < in_table->chunk_count(); ++chunk_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
        // Get information from work queue
        auto output_offset = chunk_offsets[chunk_id];
        auto column = in_table->get_chunk(chunk_id).get_column(column_id);
        auto &output = static_cast<Partition<T> &>(*elements);

        // prepare histogram
        histograms[chunk_id] = std::make_shared<std::vector<size_t>>(num_partitions);

        auto &histogram = static_cast<std::vector<size_t> &>(*histograms[chunk_id]);

        // Materialize the chunk
        ColumnBuilder<T> builder = ColumnBuilder<T>(chunk_id);
        column->visit(builder);

        auto const &materialized = static_cast<pmr_vector<std::pair<RowID, T>> &>(*builder._materialized_chunk);

        size_t row_id = output_offset;

        /*
        For ReferenceColumns we do not use the RowIDs from the referenced tables.
        Instead, we use the index in the ReferenceColumn itself. This way we can later correctly dereference
        values from different inputs (important for Multi Joins).
        For performance reasons this if statement is around the for loop.
        */
        if (auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
          // hash and add to the other elements
          ChunkOffset offset = 0;
          for (auto &&elem : materialized) {
            output[row_id] = PartitionedElement<T>{RowID{chunk_id, offset}, murmur2<T>(elem.second, seed), elem.second};

            const Hash radix = (output[row_id].partition_hash >> (32 - _radix_bits * (pass + 1))) & mask;
            histogram[radix]++;

            row_id++;
            offset++;
          }
        } else {
          // hash and add to the other elements
          for (auto &&elem : materialized) {
            output[row_id] = PartitionedElement<T>{elem.first, murmur2<T>(elem.second, seed), elem.second};

            const Hash radix = (output[row_id].partition_hash >> (32 - _radix_bits * (pass + 1))) & mask;
            histogram[radix]++;

            row_id++;
          }
        }
      }));
      jobs.back()->schedule();
    }

    CurrentScheduler::wait_for_tasks(jobs);

    return elements;
  }

  template <typename T>
  RadixContainer<T> _partition_radix_parallel(std::shared_ptr<Partition<T>> materialized,
                                              std::shared_ptr<std::vector<size_t>> chunk_offsets,
                                              std::vector<std::shared_ptr<std::vector<size_t>>> &histograms) {
    // fan-out
    const size_t num_partitions = 1 << _radix_bits;

    // currently, we just do one pass
    size_t pass = 0;
    size_t mask = static_cast<uint32_t>(pow(2, _radix_bits * (pass + 1)) - 1);

    // allocate new (shared) output
    auto output = std::make_shared<Partition<T>>();
    output->resize(materialized->size());

    auto &offsets = static_cast<std::vector<size_t> &>(*chunk_offsets);

    RadixContainer<T> radix_output;
    radix_output.elements = output;
    radix_output.partition_offsets.resize(num_partitions + 1);

    // use histograms to calculate partition offsets
    for (ChunkID chunk_id{0}; chunk_id < offsets.size(); ++chunk_id) {
      size_t local_sum = 0;
      auto &histogram = static_cast<std::vector<size_t> &>(*histograms[chunk_id]);

      for (size_t partition_id = 0; partition_id < num_partitions; ++partition_id) {
        // update local prefix sum
        local_sum += histogram[partition_id];
        // update output partition offsets
        radix_output.partition_offsets[partition_id] += histogram[partition_id];
        // save offsets
        histogram[partition_id] = local_sum;
      }
    }

    /*
    At this point, partition_offsets only contains the size of each partition.
    We now calculate the offsets by adding up the sizes previous partitions.
    */
    size_t offset = 0;
    for (size_t partition_id = 0; partition_id < num_partitions + 1; ++partition_id) {
      size_t next_offset = offset + radix_output.partition_offsets[partition_id];
      radix_output.partition_offsets[partition_id] = offset;
      offset = next_offset;
    }

    std::vector<std::shared_ptr<AbstractTask>> jobs;
    jobs.reserve(offsets.size());

    for (ChunkID chunk_id{0}; chunk_id < offsets.size(); ++chunk_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id] {
        // calculate output offsets for each partition
        auto output_offsets = std::vector<size_t>(num_partitions, 0);

        // add up the output offsets for chunks before this one
        for (ChunkID i{0}; i < chunk_id; ++i) {
          for (size_t j = 0; j < num_partitions; ++j) {
            output_offsets[j] += histograms[i]->operator[](j);
          }
        }
        for (auto i = chunk_id; i < offsets.size(); ++i) {
          for (size_t j = 1; j < num_partitions; ++j) {
            output_offsets[j] += histograms[i]->operator[](j - 1);
          }
        }

        size_t input_offset = offsets[chunk_id];

        size_t input_size = 0;
        if (chunk_id < offsets.size() - 1) {
          input_size = offsets[chunk_id + 1] - input_offset;
        } else {
          input_size = materialized->size() - input_offset;
        }

        auto &out = static_cast<Partition<T> &>(*output);
        for (size_t i = input_offset; i < input_offset + input_size; ++i) {
          auto &element = (*materialized)[i];
          const size_t radix = (element.partition_hash >> (32 - _radix_bits * (pass + 1))) & mask;

          out[output_offsets[radix]++] = element;
        }
      }));
      jobs.back()->schedule();
    }

    CurrentScheduler::wait_for_tasks(jobs);

    return radix_output;
  }

  /*
  Build all the hash tables for the partitions of Left. We parallelize this process for all partitions of Left
  */
  void _build(const RadixContainer<LeftType> &radix_container,
              std::vector<std::shared_ptr<HashTable<LeftType>>> &hashtables) {
    std::vector<std::shared_ptr<AbstractTask>> jobs;
    jobs.reserve(radix_container.partition_offsets.size() - 1);

    for (size_t current_partition_id = 0; current_partition_id < (radix_container.partition_offsets.size() - 1);
         ++current_partition_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&]() {
        auto &partition_left = static_cast<Partition<LeftType> &>(*radix_container.elements);
        const auto &partition_left_begin = radix_container.partition_offsets[current_partition_id];
        const auto &partition_left_end = radix_container.partition_offsets[current_partition_id + 1];
        const auto partition_size = partition_left_end - partition_left_begin;

        // Prune empty partitions, so that we don't have too many empty hash tables
        if (partition_size == 0) {
          return;
        }

        auto hashtable = std::make_shared<HashTable<LeftType>>(partition_size);

        for (size_t i = partition_left_begin; i < partition_left_end; ++i) {
          auto &element = partition_left[i];
          hashtable->put(element.value, element.row_id);
        }

        hashtables[current_partition_id] = hashtable;
      }));
      jobs.back()->schedule();
    }

    CurrentScheduler::wait_for_tasks(jobs);
  }

  /*
  In the probe phase we take all partitions from the right partition, iterate over them and compare each join candidate
  with the values in the hash table. Since Left and Right are hashed using the same hash function, we can reduce the
  number of hash tables that need to be looked into to just 1.
  */
  void _probe(const RadixContainer<RightType> &radix_container,
              const std::vector<std::shared_ptr<HashTable<LeftType>>> &hashtables,
              std::vector<std::shared_ptr<PosList>> &pos_list_left,
              std::vector<std::shared_ptr<PosList>> &pos_list_right) {
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
      jobs.emplace_back(std::make_shared<JobTask>([&]() {
        // Get information from work queue
        auto &partition = static_cast<Partition<RightType> &>(*radix_container.elements);
        const auto &partition_begin = radix_container.partition_offsets[current_partition_id];
        const auto &partition_end = radix_container.partition_offsets[current_partition_id + 1];

        // Skip empty partitions to avoid empty output chunks
        if ((partition_end - partition_begin) == 0) {
          return;
        }

        if (hashtables[current_partition_id]) {
          auto &hashtable = hashtables.at(current_partition_id);
          auto pos_list_left_local = std::make_shared<PosList>();
          auto pos_list_right_local = std::make_shared<PosList>();

          for (size_t i = partition_begin; i < partition_end; ++i) {
            auto &row = partition[i];
            auto row_ids = hashtable->get(row.value);

            if (row_ids) {
              for (auto &row_id : *row_ids) {
                pos_list_left_local->emplace_back(row_id);
                pos_list_right_local->emplace_back(row.row_id);
              }
              // We assume that the relations have been swapped previously,
              // so that the outer relation is the probing relation.
            } else if (_mode == JoinMode::Left || _mode == JoinMode::Right) {
              pos_list_left_local->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
              pos_list_right_local->emplace_back(row.row_id);
            }
          }

          if (!pos_list_left_local->empty()) {
            pos_list_left[current_partition_id] = pos_list_left_local;
            pos_list_right[current_partition_id] = pos_list_right_local;
          }
        } else if (_mode == JoinMode::Left || _mode == JoinMode::Right) {
          /*
          We assume that the relations have been swapped previously,
          so that the outer relation is the probing relation.

          Since we did not find a proper hash table,
          we know that there is no match in Left for this partition.
          Hence we are going to write NULL values for each row.
          */

          auto pos_list_left_local = std::make_shared<PosList>();
          auto pos_list_right_local = std::make_shared<PosList>();

          for (size_t i = partition_begin; i < partition_end; ++i) {
            auto &row = partition[i];
            pos_list_left_local->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
            pos_list_right_local->emplace_back(row.row_id);
          }
          if (!pos_list_left_local->empty()) {
            pos_list_left[current_partition_id] = pos_list_left_local;
            pos_list_right[current_partition_id] = pos_list_right_local;
          }
        }
      }));
      jobs.back()->schedule();
    }

    CurrentScheduler::wait_for_tasks(jobs);
  }

  /*
  Copy the column meta-data from input to output table.
  */
  static void _copy_table_metadata(const std::shared_ptr<const Table> in_table,
                                   const std::shared_ptr<Table> out_table) {
    for (ColumnID column_id{0}; column_id < in_table->col_count(); ++column_id) {
      // TODO(anyone): Refine since not all column are nullable
      out_table->add_column_definition(in_table->column_name(column_id), in_table->column_type(column_id), true);
    }
  }

  std::shared_ptr<const Table> _on_execute() override {
    /*
    Preparing output table by adding columns from left table.
    */

    auto _right_in_table = _right->get_output();
    auto _left_in_table = _left->get_output();

    if (_inputs_swapped) {
      _copy_table_metadata(_right_in_table, _output_table);
      _copy_table_metadata(_left_in_table, _output_table);
    } else {
      _copy_table_metadata(_left_in_table, _output_table);
      _copy_table_metadata(_right_in_table, _output_table);
    }

    // Pre-partitioning
    // Save chunk offsets into the input relation
    size_t left_chunk_count = _left_in_table->chunk_count();
    size_t right_chunk_count = _right_in_table->chunk_count();

    auto left_chunk_offsets = std::make_shared<std::vector<size_t>>();
    auto right_chunk_offsets = std::make_shared<std::vector<size_t>>();

    left_chunk_offsets->resize(left_chunk_count);
    right_chunk_offsets->resize(right_chunk_count);

    size_t offset_left = 0;
    for (ChunkID i{0}; i < left_chunk_count; ++i) {
      left_chunk_offsets->operator[](i) = offset_left;
      offset_left += _left_in_table->get_chunk(i).size();
    }

    size_t offset_right = 0;
    for (ChunkID i{0}; i < right_chunk_count; ++i) {
      right_chunk_offsets->operator[](i) = offset_right;
      offset_right += _right_in_table->get_chunk(i).size();
    }

    // Materialization phase
    std::vector<std::shared_ptr<std::vector<size_t>>> histograms_left;
    std::vector<std::shared_ptr<std::vector<size_t>>> histograms_right;
    /*
    NUMA notes:
    The materialized vectors don't have any strong NUMA preference because they haven't been partitioned yet.
    However, it would be a good idea to keep each materialized vector on one node if possible.
    This helps choosing a scheduler node for the radix phase (see below).
    */
    // Scheduler note: parallelize this at some point. Currently, the amount of jobs would be too high
    auto materialized_left = _materialize_input<LeftType>(_left_in_table, _column_ids.first, histograms_left);
    auto materialized_right = _materialize_input<RightType>(_right_in_table, _column_ids.second, histograms_right);

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
    auto radix_left = _partition_radix_parallel<LeftType>(materialized_left, left_chunk_offsets, histograms_left);
    auto radix_right = _partition_radix_parallel<RightType>(materialized_right, right_chunk_offsets, histograms_right);

    // Build phase
    std::vector<std::shared_ptr<HashTable<LeftType>>> hashtables;
    hashtables.resize(radix_left.partition_offsets.size() - 1);
    /*
    NUMA notes:
    The hashtables for each partition P should also reside on the same node as the two vectors leftP and rightP.
    */
    _build(radix_left, hashtables);

    // Probe phase
    std::vector<std::shared_ptr<PosList>> left_pos_lists;
    std::vector<std::shared_ptr<PosList>> right_pos_lists;
    left_pos_lists.resize(radix_right.partition_offsets.size() - 1);
    right_pos_lists.resize(radix_right.partition_offsets.size() - 1);
    /*
    NUMA notes:
    The workers for each radix partition P should be scheduled on the same node as the input data:
    leftP, rightP and hashtableP.
    */
    _probe(radix_right, hashtables, left_pos_lists, right_pos_lists);

    /*
    Add columns to output chunk.
    We assume that either all Chunks contain ReferenceColumns or all Chunk contain Value/DictionaryColumns.
    But we expect that it is not possible to have both ReferenceColumns and Value/DictionaryColumn in one table.
    */
    auto ref_col_left =
        std::dynamic_pointer_cast<ReferenceColumn>(_left_in_table->get_chunk(ChunkID{0}).get_column(ColumnID{0}))
            ? true
            : false;
    auto ref_col_right =
        std::dynamic_pointer_cast<ReferenceColumn>(_right_in_table->get_chunk(ChunkID{0}).get_column(ColumnID{0}))
            ? true
            : false;

    for (size_t i = 0; i < left_pos_lists.size(); i++) {
      auto &left = left_pos_lists[i];
      auto &right = right_pos_lists[i];

      if (!right && !left) {
        continue;
      } else if (!right || !left) {
        Fail("JoinHash: either left or right pos_list is empty. Should not happen");
      }

      Chunk output_chunk;

      // we need to swap back the inputs, so that the order of the output columns is not harmed
      if (_inputs_swapped) {
        write_output_chunks(output_chunk, _right_in_table, right, ref_col_right);
        write_output_chunks(output_chunk, _left_in_table, left, ref_col_left);
      } else {
        write_output_chunks(output_chunk, _left_in_table, left, ref_col_left);
        write_output_chunks(output_chunk, _right_in_table, right, ref_col_right);
      }
      _output_table->add_chunk(std::move(output_chunk));
    }

    return _output_table;
  }

  static void write_output_chunks(Chunk &output_chunk, const std::shared_ptr<const Table> input_table,
                                  std::shared_ptr<PosList> pos_list, bool is_ref_column) {
    // Add columns from input table to output chunk
    for (ColumnID column_id{0}; column_id < input_table->col_count(); ++column_id) {
      std::shared_ptr<BaseColumn> column;

      if (is_ref_column) {
        auto ref_col =
            std::dynamic_pointer_cast<ReferenceColumn>(input_table->get_chunk(ChunkID{0}).get_column(column_id));

        // Get all the input pos lists so that we only have to pointer cast the columns once
        auto input_pos_lists = std::vector<std::shared_ptr<const PosList>>();
        for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); chunk_id++) {
          // This works because we assume that the columns have to be either all ReferenceColumns or none.
          auto ref_column =
              std::dynamic_pointer_cast<ReferenceColumn>(input_table->get_chunk(chunk_id).get_column(column_id));
          input_pos_lists.push_back(ref_column->pos_list());
        }

        // Get the row ids that are referenced
        auto new_pos_list = std::make_shared<PosList>();
        for (const auto &row : *pos_list) {
          new_pos_list->push_back(input_pos_lists.at(row.chunk_id)->at(row.chunk_offset));
        }
        column = std::make_shared<ReferenceColumn>(ref_col->referenced_table(), ref_col->referenced_column_id(),
                                                   new_pos_list);
      } else {
        column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
      }

      output_chunk.add_column(column);
    }
  }
};

}  // namespace opossum
