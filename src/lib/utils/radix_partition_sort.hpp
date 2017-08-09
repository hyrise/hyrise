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

template <typename T>
class RadixPartitionSort {
 public:
  RadixPartitionSort(const std::shared_ptr<const AbstractOperator> left,
                     const std::shared_ptr<const AbstractOperator> right,
                     std::pair<std::string, std::string> column_names, bool equi_case, size_t partition_count)
    : _left_column_name{column_names.first}, _right_column_name{column_names.second}, _equi_case{equi_case},
      _partition_count{partition_count} {
    DebugAssert(partition_count > 0, "partition_count must be > 0");
    DebugAssert(left != nullptr, "left input operator is null");
    DebugAssert(right != nullptr, "right input operator is null");

    _input_table_left = left->get_output();
    _input_table_right = right->get_output();
  }

  virtual ~RadixPartitionSort() = default;

 protected:
  using MatTablePtr = std::shared_ptr<MaterializedTable<T>>;
  /**
  * The ChunkStatistics structure is used to gather statistics regarding a chunks values in order to
  * be able to pick appropriate value ranges for the partitions.
  **/
  struct ChunkStatistics {
    ChunkStatistics(size_t partition_count) {
      partition_histogram.resize(partition_count);
      insert_position.resize(partition_count);
    }
    // Used to count the number of entries for each partition from a specific chunk
    std::vector<size_t> partition_histogram;
    std::vector<size_t> insert_position;
  };

  /**
  * The TablelStatistics structure is used to gather statistics regarding the value distribution of a table
  *  and its chunks in order to be able to pick appropriate value ranges for the partitions.
  **/
  struct TableStatistics {
    TableStatistics(size_t chunk_count, size_t partition_count) {
      partition_histogram.resize(partition_count);
      chunk_statistics.reserve(chunk_count);
      for(size_t i = 0; i < chunk_count; i++) {
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

  // the partition count should be a power of two, i.e. 1, 2, 4, 8, 16, ...
  size_t _partition_count;

  MatTablePtr _output_left;
  MatTablePtr _output_right;

  // Radix calculation for arithmetic types
  template <typename T2>
  typename std::enable_if<std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value, uint8_t radix_bitmask) {
    return static_cast<uint32_t>(value) & radix_bitmask;
  }

  // Radix calculation for non-arithmetic types
  template <typename T2>
  typename std::enable_if<!std::is_arithmetic<T2>::value, uint32_t>::type get_radix(T2 value, uint32_t radix_bitmask) {
    auto result = reinterpret_cast<const uint32_t*>(value.c_str());
    return *result & radix_bitmask;
  }

  /**
  * Determines the total size of a materialized table.
  **/
  size_t _materialized_table_size(MatTablePtr table) {
    size_t total_size = 0;
    for (auto chunk : *table) {
      total_size += chunk->size();
    }

    return total_size;
  }

  /**
  * Concatenates multiple materialized chunks to a single materialized chunk.
  **/
  MatTablePtr _concatenate_chunks(MatTablePtr input_chunks) {
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

  MatTablePtr _partition(MatTablePtr input_chunks, std::function<size_t(T&)> partitioner) {
    auto output_table = std::make_shared<MaterializedTable<T>>(_partition_count);
    TableStatistics table_statistics(input_chunks->size(), _partition_count);

    // Each chunk should prepare additional data to enable partitioning
    for (size_t chunk_number = 0; chunk_number < input_chunks->size(); chunk_number++) {
      auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
      auto input_chunk = input_chunks->at(chunk_number);

      // Count the number of entries for each partition
      for (auto& entry : *input_chunk) {
        auto partition_id = partitioner(entry.value);
        chunk_statistics.partition_histogram[partition_id]++;
      }
    }

    // Aggregate the chunks histograms to a table histogram and initialize the insert positions for each chunk
    for (auto& chunk_statistics : table_statistics.chunk_statistics) {
      for (size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
        chunk_statistics.insert_position[partition_id] = table_statistics.partition_histogram[partition_id];
        table_statistics.partition_histogram[partition_id] += chunk_statistics.partition_histogram[partition_id];
      }
    }

    // Reserve the appropriate output space for the partitions
    for (size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
      auto partition_size = table_statistics.partition_histogram[partition_id];
      output_table->at(partition_id) = std::make_shared<MaterializedChunk<T>>(partition_size);
    }

    // Move each entry into its appropriate partition
    for (size_t chunk_number = 0; chunk_number < input_chunks->size(); chunk_number++) {
      auto& chunk_statistics = table_statistics.chunk_statistics[chunk_number];
      for (auto& entry : *input_chunks->at(chunk_number)) {
        auto partition_id = partitioner(entry.value);
        output_table->at(partition_id)->at(chunk_statistics.insert_position[partition_id]++) = entry;
      }
    }

    return output_table;
  }

  /**
  * Performs least significant bit radix partitioning which is used in the equi join case.
  **/
  MatTablePtr _radix_partition(MatTablePtr input_chunks) {
    auto radix_bitmask = _partition_count - 1;
    return _partition(input_chunks, [=] (T& value) {
      return get_radix<T>(value, radix_bitmask);
    });
  }

  /**
  * Todo
  **/
  T pick_sample_values(std::vector<std::map<T, size_t>>& sample_values, MatTablePtr table) {
    auto max_value = table->at(0)->at(0).value;

    for (size_t chunk_number = 0; chunk_number < table->size(); chunk_number++) {
      auto chunk_values = table->at(chunk_number);

      // Since the chunks are sorted, the maximum values are at the back of them
      if (max_value < chunk_values->back().value) {
        max_value = chunk_values->back().value;
      }

      for(size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
        size_t pos = static_cast<size_t>(chunk_values->size() * (partition_id / static_cast<float>(_partition_count)));
        sample_values[partition_id][chunk_values->at(pos).value]++;
      }
    }

    return max_value;
  }

  /**
  * Performs the radix partition sort for the non equi case which requires the complete table to be sorted
  * and not only the partitions in themselves.
  **/
  std::pair<MatTablePtr, MatTablePtr> _range_partition(MatTablePtr input_left, MatTablePtr input_right) {
    std::vector<std::map<T, size_t>> sample_values(_partition_count);

    auto max_value_left = pick_sample_values(sample_values, input_left);
    auto max_value_right = pick_sample_values(sample_values, input_right);

    auto max_value = std::max(max_value_left, max_value_right);

    // Pick the split values to be the most common sample value for each partition
    std::vector<T> split_values(_partition_count);
    for (size_t partition_id = 0; partition_id < _partition_count - 1; partition_id++) {
    split_values[partition_id] = std::max_element(sample_values[partition_id].begin(),
                                                  sample_values[partition_id].end(),
      [] (auto& a, auto& b) {
        return a.second < b.second;
      })->second;
    }
    split_values.back() = max_value;

    // Implements range partitioning
    auto partitioner = [this, &split_values](T& value) {
      for(size_t partition_id = 0; partition_id < _partition_count; partition_id++) {
        if(value <= split_values[partition_id]) {
          return partition_id;
        }
      }
      throw std::logic_error("There is no partition for this value");
    };

    auto output_left = _partition(input_left, partitioner);
    auto output_right = _partition(input_right, partitioner);

    return std::pair<MatTablePtr, MatTablePtr>(output_left, output_right);

  }

  /**
  * Prints out the values of a materialized table.
  **/
  void print_table(MatTablePtr table) {
    std::cout << "----" << std::endl;
    for (auto chunk : *table) {
      for (auto& row : *chunk) {
        std::cout << row.value << std::endl;
      }
      std::cout << "----" << std::endl;
    }
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
  * Executes the partitioning and sorting
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

  std::pair<MatTablePtr, MatTablePtr> get_output() {
    return std::make_pair(_output_left, _output_right);
  }
};

}  // namespace opossum
