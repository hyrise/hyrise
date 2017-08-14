#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "table_materializer.hpp"

namespace opossum {

/**
* Note: What should this class be named, since it was specifically made for the sort merge join and not for general
* partitioning? Also, we default to range partition sort instead of radix partitioning for the non equi case.
*
* Performs radix partitioning for the sort merge join. The radix partitioning algorithm partitions on the basis
* of the least significant bits of the values because the values there are much more evenly distributed than for the
* most significant bits. As a result, equal values always get moved to the same partition and the partitions are
* sorted in themselves but in between the partitions. This is okay for the equi join, because we are only interested
* in equlity. In the case of a non-equi join however, complete sortedness is required, because join matches exist
* beyond partition borders. Therefore, the partitioner defaults to a range partitioning algorithm for the non-equi-join.
* General partitioning process:
* -> Input chunks are materialized and sorted. Every value is stored together with its row id.
* -> Then, either radix partitioning or range partitioning is performed.
* -> At last, the resulting partitions are sorted.
**/
template <typename T>
class RadixPartitionSort {
 public:
  RadixPartitionSort(const std::shared_ptr<const Table> left, const std::shared_ptr<const Table> right,
                     std::pair<std::string, std::string> column_names, bool equi_case, size_t partition_count)
    : _input_table_left{left}, _input_table_right{right}, _left_column_name{column_names.first},
      _right_column_name{column_names.second}, _equi_case{equi_case},
      _partition_count{partition_count} {
    DebugAssert(partition_count > 0, "partition_count must be > 0");
    DebugAssert((partition_count & (partition_count - 1)) == 0,
                "partition_count must be a power of two, i.e. 1, 2, 4, 8...");
    DebugAssert(left != nullptr, "left input operator is null");
    DebugAssert(right != nullptr, "right input operator is null");
  }

  virtual ~RadixPartitionSort() = default;

 protected:
  using MatTablePtr = std::shared_ptr<MaterializedTable<T>>;
  /**
  * The ChunkStatistics structure is used to gather statistics regarding a chunk's values in order to
  * be able to appropriately reserve space for the partitioning output.
  **/
  struct ChunkStatistics {
    explicit ChunkStatistics(size_t partition_count) {
      partition_histogram.resize(partition_count);
      insert_position.resize(partition_count);
    }
    // Used to count the number of entries for each partition from a specific chunk
    std::vector<size_t> partition_histogram;
    std::vector<size_t> insert_position;
  };

  /**
  * The TableStatistics structure is used to gather statistics regarding the value distribution of a table
  *  and its chunks in order to be able to appropriately reserve space for the partitioning output.
  **/
  struct TableStatistics {
    TableStatistics(size_t chunk_count, size_t partition_count) {
      partition_histogram.resize(partition_count);
      chunk_statistics.reserve(chunk_count);
      for (size_t i = 0; i < chunk_count; ++i) {
        chunk_statistics.push_back(ChunkStatistics(partition_count));
      }
    }
    // Used to count the number of entries for each partition from the whole table
    std::vector<size_t> partition_histogram;
    std::vector<ChunkStatistics> chunk_statistics;
  };

  // Input parameters
  std::shared_ptr<const Table> _input_table_left;
  std::shared_ptr<const Table> _input_table_right;
  const std::string _left_column_name;
  const std::string _right_column_name;
  bool _equi_case;

  // The partition count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
  // It is asserted to be a power of two in the constructor.
  size_t _partition_count;

  MatTablePtr _output_left;
  MatTablePtr _output_right;

  // Radix calculation for arithmetic types
  template <typename T2>
  static typename std::enable_if<std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value,
                                                                                          uint32_t radix_bitmask) {
    return static_cast<uint32_t>(value) & radix_bitmask;
  }

  // Radix calculation for non-arithmetic types
  template <typename T2>
  static typename std::enable_if<!std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value,
                                                                                           uint32_t radix_bitmask) {
    auto result = reinterpret_cast<const uint32_t*>(value.c_str());
    return *result & radix_bitmask;
  }

  /**
  * Determines the total size of a materialized table.
  **/
  static size_t _materialized_table_size(MatTablePtr table) {
    size_t total_size = 0;
    for (auto chunk : *table) {
      total_size += chunk->size();
    }

    return total_size;
  }

  /**
  * Concatenates multiple materialized chunks to a single materialized chunk.
  **/
  static MatTablePtr _concatenate_chunks(MatTablePtr input_chunks) {
    auto output_table = std::make_shared<MaterializedTable<T>>(1);
    output_table->at(0) = std::make_shared<MaterializedChunk<T>>();

    // Reserve the required space and move the data to the output
    auto output_chunk = output_table->at(0);
    output_chunk->reserve(_materialized_table_size(input_chunks));
    for (auto& chunk : *input_chunks) {
      output_chunk->insert(output_chunk->end(), chunk->begin(), chunk->end());
    }

    return output_table;
  }

  /**
  * Performs the partitioning on a materialized table using a partitioning function that determines for each
  * value the appropriate partition id. This is how the partitioning works:
  * -> Count for each chunk how many of its values belong in each of the partitions using histograms.
  * -> Aggregate the per-chunk histograms to a histogram for the whole table. For each chunk it is noted where
  *    it will be inserting values in each partition.
  * -> Reserve the appropriate space for each output partition to avoid ongoing vector resizing.
  * -> At last, each value of each chunk is moved to the appropriate partition.
  **/
  MatTablePtr _partition(MatTablePtr input_chunks, std::function<size_t(const T&)> partitioner) {
    auto output_table = std::make_shared<MaterializedTable<T>>(_partition_count);
    TableStatistics table_statistics(input_chunks->size(), _partition_count);

    // Count for every chunk the number of entries for each partition in parallel
    std::vector<std::shared_ptr<AbstractTask>> histogram_jobs;
    for (size_t chunk_number = 0; chunk_number < input_chunks->size(); ++chunk_number) {
      auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
      auto input_chunk = input_chunks->at(chunk_number);

      // Count the number of entries for each partition to be able to reserve the appropriate output space later.
      // Note: Does this make sense from a performance view?
      // Alternative 1: Straight up appending the output chunks: Downside: ongoing vector resizing
      // Alternative 2: Estimating the output sizes using samples. Downside: overestimation (unused reserved space)
      // and underestimation (vector resizing required)
      // But then we we would not be able to derive the insert positions based on these counts, which
      // are important for parallel partitioning
      auto job = std::make_shared<JobTask>([input_chunk, &chunk_statistics, &partitioner] {
        for (auto& entry : *input_chunk) {
          auto partition_id = partitioner(entry.value);
          ++chunk_statistics.partition_histogram[partition_id];
        }
      });

      histogram_jobs.push_back(job);
      job->schedule();
    }

    CurrentScheduler::wait_for_tasks(histogram_jobs);


    // Aggregate the chunks histograms to a table histogram and initialize the insert positions for each chunk
    for (auto& chunk_statistics : table_statistics.chunk_statistics) {
      for (size_t partition_id = 0; partition_id < _partition_count; ++partition_id) {
        chunk_statistics.insert_position[partition_id] = table_statistics.partition_histogram[partition_id];
        table_statistics.partition_histogram[partition_id] += chunk_statistics.partition_histogram[partition_id];
      }
    }

    // Reserve the appropriate output space for the partitions
    for (size_t partition_id = 0; partition_id < _partition_count; ++partition_id) {
      auto partition_size = table_statistics.partition_histogram[partition_id];
      output_table->at(partition_id) = std::make_shared<MaterializedChunk<T>>(partition_size);
    }

    // Move each entry into its appropriate partition in parallel
    std::vector<std::shared_ptr<AbstractTask>> partition_jobs;
    for (size_t chunk_number = 0; chunk_number < input_chunks->size(); ++chunk_number) {
      auto job = std::make_shared<JobTask>([chunk_number, output_table, input_chunks, &table_statistics, &partitioner] {
        auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
        for (auto& entry : *input_chunks->at(chunk_number)) {
          auto partition_id = partitioner(entry.value);
          output_table->at(partition_id)->at(chunk_statistics.insert_position[partition_id]++) = entry;
        }
      });
      partition_jobs.push_back(job);
      job->schedule();
    }

    CurrentScheduler::wait_for_tasks(partition_jobs);

    return output_table;
  }

  /**
  * Performs least significant bit radix partitioning which is used in the equi join case.
  * Note: if we used the most significant bits, we could also use this for non-equi joins.
  * Then, however we would have to deal with skewed partitions. Other ideas:
  * - hand select the partitioning bits based on statistics.
  * - consolidate partitions in order to reduce skew.
  **/
  MatTablePtr _radix_partition(MatTablePtr input_chunks) {
    DebugAssert(_partition_count > 0 && (_partition_count & (_partition_count - 1)) == 0,
                "_partition_count must be a power of two greater than zero, i.e. 1, 2, 4, 8...");
    auto radix_bitmask = _partition_count - 1;
    return _partition(input_chunks, [=] (const T& value) {
      return get_radix<T>(value, radix_bitmask);
    });
  }

  /**
  * Picks sample values from a materialized table that are used to determine partition range bounds.
  **/
  void _pick_sample_values(std::vector<std::map<T, size_t>>& sample_values, MatTablePtr table) {
    for (size_t chunk_number = 0; chunk_number < table->size(); ++chunk_number) {
      auto chunk_values = table->at(chunk_number);
      for (size_t partition_id = 0; partition_id < _partition_count - 1; ++partition_id) {
        size_t pos = static_cast<size_t>(chunk_values->size() * (partition_id / static_cast<float>(_partition_count)));
        ++sample_values[partition_id][chunk_values->at(pos).value];
      }
    }
  }

  /**
  * Performs the radix partition sort for the non equi case which requires the complete table to be sorted
  * and not only the partitions in themselves.
  **/
  std::pair<MatTablePtr, MatTablePtr> _range_partition(MatTablePtr input_left, MatTablePtr input_right) {
    std::vector<std::map<T, size_t>> sample_values(_partition_count);

    _pick_sample_values(sample_values, input_left);
    _pick_sample_values(sample_values, input_right);

    // Pick the most common sample values for each partition for the split values.
    // The last partition does not need a split value because it covers all values that are bigger than all split values
    // The split values mark the ranges of the partitions.
    // A split value is the end of a range and the start of the next one.
    std::vector<T> split_values(_partition_count);
    for (size_t partition_id = 0; partition_id < _partition_count - 2; ++partition_id) {
      // Pick the values with the highest count
      split_values[partition_id] = std::max_element(sample_values[partition_id].begin(),
                                                  sample_values[partition_id].end(),
        [] (auto& a, auto& b) {
          return a.second < b.second;
      })->second;
    }

    // Implements range partitioning
    auto partition_count = _partition_count;
    auto partitioner = [partition_count, &split_values](const T& value) {
      // Find the first split value that is greater or equal to the entry.
      // The split values are sorted in ascending order.
      // Note: can we do this faster? (binary search?)
      for (size_t partition_id = 0; partition_id < partition_count; ++partition_id) {
        if (value <= split_values[partition_id]) {
          return partition_id;
        }
      }

      // The value is greater than all split values, which means it belongs in the last partition.
      return partition_count - 1;
    };

    auto output_left = _partition(input_left, partitioner);
    auto output_right = _partition(input_right, partitioner);

    return std::pair<MatTablePtr, MatTablePtr>(output_left, output_right);
  }

  /**
  * Sorts all partitions of a materialized table.
  **/
  void _sort_partitions(MatTablePtr partitions) {
    for (auto partition : *partitions) {
      std::sort(partition->begin(), partition->end(), [](auto& left, auto& right) {
        return left.value < right.value;
      });
    }
  }

 public:
  /**
  * Executes the partitioning and sorting.
  **/
  void execute() {
    // Sort the chunks of the input tables
    TableMaterializer<T> table_materializer(true /* sorting enabled */);
    auto chunks_left = table_materializer.materialize(_input_table_left, _left_column_name);
    auto chunks_right = table_materializer.materialize(_input_table_right, _right_column_name);

    if (_partition_count == 1) {
      _output_left = _concatenate_chunks(chunks_left);
      _output_right = _concatenate_chunks(chunks_right);
    } else if (_equi_case) {
      _output_left = _radix_partition(chunks_left);
      _output_right = _radix_partition(chunks_right);
    } else {
      auto result = _range_partition(chunks_left, chunks_right);
      _output_left = result.first;
      _output_right = result.second;
    }

    // Sort each partition (right now std::sort -> but maybe can be replaced with
    // an algorithm more efficient, if subparts are already sorted [InsertionSort?!])
    _sort_partitions(_output_left);
    _sort_partitions(_output_right);

    DebugAssert(_materialized_table_size(_output_left) == _input_table_left->row_count(),
                "left output has wrong size");
    DebugAssert(_materialized_table_size(_output_right) == _input_table_right->row_count(),
                "right output has wrong size");
  }

  /**
  * Gets the output of the partitioning containing the two materialized tables.
  **/
  std::pair<MatTablePtr, MatTablePtr> get_output() {
    return std::make_pair(_output_left, _output_right);
  }
};

}  // namespace opossum
