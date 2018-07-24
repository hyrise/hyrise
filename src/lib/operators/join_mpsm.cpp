#include "join_mpsm.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "join_mpsm/radix_cluster_sort_numa.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/topology.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"

#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

// A cluster is a chunk of values which agree on their last bits
STRONG_TYPEDEF(size_t, ClusterID);

/**
   * This class is the entry point to the Multi Phase Sort Merge Join, which is a variant of the Sort Merge Join
   * built for higher performance on NUMA Systems. It is implemented to reduce random reads between NUMA Nodes so
   * as to not incur overhead through inter node communication.
   * The algorithmic NUMA awareness stems from the algorithm clustering the values according to their
   * n least significant bits, where n is the number of numa nodes. For the LHS the values in the clusters are then
   * reshuffled so that each NUMA Node holds one cluster which is sorted. For the RHS the clusters are simply
   * partitioned and then sorted per partition. This enables the join to perform only linear reads and writes between
   * NUMA clusters, avoiding the latency assiociated with these operations.
   * The implementation of NUMA awareness hinges on explicitly allocating data on NUMA nodes using pmr_allocators and
   * scheduling operations to always run on the same node as the data.
**/

namespace opossum {
JoinMPSM::JoinMPSM(const std::shared_ptr<const AbstractOperator>& left,
                   const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                   const std::pair<ColumnID, ColumnID>& column_ids, const PredicateCondition op)
    : AbstractJoinOperator(OperatorType::JoinMPSM, left, right, mode, column_ids, op) {
  // Validate the parameters
  DebugAssert(mode != JoinMode::Cross, "This operator does not support cross joins.");
  DebugAssert(left != nullptr, "The left input operator is null.");
  DebugAssert(right != nullptr, "The right input operator is null.");
  DebugAssert(op == PredicateCondition::Equals, "Only Equi joins are supported by MPSM join.");
}

std::shared_ptr<const Table> JoinMPSM::_on_execute() {
  // Check column types
  const auto& left_column_type = input_table_left()->column_data_type(_column_ids.first);
  DebugAssert(left_column_type == input_table_right()->column_data_type(_column_ids.second),
              "Left and right column types do not match. The mpsm join requires matching column types");

  // Create implementation to compute the join result
  _impl = make_unique_by_data_type<AbstractJoinOperatorImpl, JoinMPSMImpl>(
      left_column_type, *this, _column_ids.first, _column_ids.second, _predicate_condition, _mode);

  return _impl->_on_execute();
}

void JoinMPSM::_on_cleanup() { _impl.reset(); }

std::shared_ptr<AbstractOperator> JoinMPSM::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<JoinMPSM>(recreated_input_left, recreated_input_right, _mode, _column_ids,
                                    _predicate_condition);
}

const std::string JoinMPSM::name() const { return "Join MPSM"; }

template <typename T>
class JoinMPSM::JoinMPSMImpl : public AbstractJoinOperatorImpl {
 public:
  JoinMPSMImpl<T>(JoinMPSM& mpsm_join, ColumnID left_column_id, ColumnID right_column_id, const PredicateCondition op,
                  JoinMode mode)
      : _mpsm_join{mpsm_join},
        _left_column_id{left_column_id},
        _right_column_id{right_column_id},
        _op{op},
        _mode{mode} {
    _cluster_count = _determine_number_of_clusters();
    _output_pos_lists_left.resize(_cluster_count);
    _output_pos_lists_right.resize(_cluster_count);
    for (auto cluster = ClusterID{0}; cluster < _cluster_count; ++cluster) {
      _output_pos_lists_left[cluster].resize(1);
      _output_pos_lists_right[cluster].resize(_cluster_count);
    }
  }

 protected:
  JoinMPSM& _mpsm_join;

  // Contains the materialized sorted input tables
  std::unique_ptr<MaterializedNUMAPartitionList<T>> _sorted_left_table;
  std::unique_ptr<MaterializedNUMAPartitionList<T>> _sorted_right_table;

  // Contains the null value row ids if a join column is an outer join column
  std::unique_ptr<PosList> _null_rows_left;
  std::unique_ptr<PosList> _null_rows_right;

  const ColumnID _left_column_id;
  const ColumnID _right_column_id;

  const PredicateCondition _op;
  const JoinMode _mode;

  // the cluster count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
  ClusterID _cluster_count;

  // Contains the output row ids for each cluster
  std::vector<std::vector<std::shared_ptr<PosList>>> _output_pos_lists_left;
  std::vector<std::vector<std::shared_ptr<PosList>>> _output_pos_lists_right;

  /**
   * The TablePosition is a utility struct that is used during the merge phase to identify the
   * elements in our sorted temporary list by position.
  **/
  struct TableRange;
  struct TablePosition {
    TablePosition() = default;
    TablePosition(NodeID partition, ClusterID cluster, size_t index)
        : cluster{cluster}, index{index}, partition{partition} {}

    ClusterID cluster;
    size_t index;
    NodeID partition;

    TableRange to(TablePosition position) { return TableRange(*this, position); }
  };

  /**
    * The TableRange is a utility struct that is used to define ranges of rows in a sorted input table spanning from
    * a start position to an end position.
  **/
  struct TableRange {
    TableRange(TablePosition start_position, TablePosition end_position) : start{start_position}, end{end_position} {
      DebugAssert(start.partition == end.partition, "Table ranges are only allowed over the same position");
    }
    TableRange(NodeID partition, ClusterID cluster, size_t start_index, size_t end_index)
        : start{TablePosition(partition, cluster, start_index)}, end{TablePosition(partition, cluster, end_index)} {
      DebugAssert(start.partition == end.partition, "Table ranges are only allowed over the same position");
    }

    TablePosition start;
    TablePosition end;

    // Executes the given action for every row id of the table in this range.
    template <typename F>
    void for_every_row_id(std::unique_ptr<MaterializedNUMAPartitionList<T>>& table, F action) {
      for (auto cluster = start.cluster; cluster <= end.cluster; ++cluster) {
        const auto current_cluster = (*table)[end.partition]._chunk_columns[cluster];
        size_t start_index = 0;
        // For the end index we need to find out how long the cluster is on this partition
        size_t end_index = current_cluster->size();

        // See whether we have more specific start or end indices
        if (cluster == start.cluster) start_index = start.index;
        if (cluster == end.cluster) end_index = end.index;

        for (size_t index = start_index; index < end_index; ++index) {
          action((*current_cluster)[index].row_id);
        }
      }
    }
  };

  /**
  * Determines the number of clusters to be used for the join.
  * The number of clusters must be a power of two, i.e. 1, 2, 4, 8, 16...
  **/
  ClusterID _determine_number_of_clusters() {
    // Get the next lower power of two of the bigger chunk number
    const size_t numa_nodes = Topology::get().nodes().size();
    return static_cast<ClusterID>(std::pow(2, std::floor(std::log2(numa_nodes))));
  }

  /**
  * Gets the table position corresponding to the end of the table, i.e. the last entry of the last cluster.
  **/
  static std::vector<TablePosition> _end_of_table(std::unique_ptr<MaterializedNUMAPartitionList<T>>& table) {
    DebugAssert(table->size() > 0, "table has no chunks");
    auto end_positions = std::vector<TablePosition>{table->size()};
    for (auto& partition : (*table)) {
      auto last_cluster = partition._chunk_columns.size() - 1;
      auto node_id = partition._node_id;
      end_positions[node_id] = TablePosition(node_id, last_cluster, partition._chunk_columns[last_cluster]->size());
    }

    return end_positions;
  }

  /**
  * Represents the result of a value comparison.
  **/
  enum class ComparisonResult { Less, Greater, Equal };

  /**
  * Performs the join for two runs of a specified cluster.
  * A run is a series of rows in a cluster with the same value.
  **/
  void _join_runs(TableRange left_run, TableRange right_run, ComparisonResult comparison_result,
                  std::vector<bool>& left_joined) {
    auto cluster_number = left_run.start.cluster;
    auto partition_number = left_run.start.partition;
    switch (comparison_result) {
      case ComparisonResult::Equal:
        _emit_all_combinations(partition_number, cluster_number, left_run, right_run);

        // Since we step multiple times over the left chunk
        // we need to memorize the joined rows for the left and outer case
        if (_mode == JoinMode::Left || _mode == JoinMode::Outer) {
          for (auto joined_id = left_run.start.index; joined_id < left_run.end.index; ++joined_id) {
            left_joined[joined_id] = true;
          }
        }

        break;
      case ComparisonResult::Less:
        // This usually does something for the left join case
        // but we could hit an equal when stepping again over the left side
        break;
      case ComparisonResult::Greater:
        if (_mode == JoinMode::Right || _mode == JoinMode::Outer) {
          _emit_left_null_combinations(partition_number, cluster_number, right_run);
        }
        break;
    }
  }

  /**
  * Emits a combination of a left row id and a right row id to the join output.
  **/
  void _emit_combination(NodeID output_partition, ClusterID output_cluster, RowID left, RowID right) {
    _output_pos_lists_left[output_partition][output_cluster]->push_back(left);
    _output_pos_lists_right[output_partition][output_cluster]->push_back(right);
  }

  /**
  * Emits all the combinations of row ids from the left table range and the right table range to the join output.
  * I.e. the cross product of the ranges is emitted.
  **/
  void _emit_all_combinations(NodeID output_partition, ClusterID output_cluster, TableRange left_range,
                              TableRange right_range) {
    left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
      right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
        _emit_combination(output_partition, output_cluster, left_row_id, right_row_id);
      });
    });
  }

  /**
  * Emits all combinations of row ids from the left table range and a NULL value on the right side to the join output.
  **/
  void _emit_right_null_combinations(NodeID output_partition, ClusterID output_cluster,
                                     std::shared_ptr<MaterializedChunk<T>> left_chunk, std::vector<bool> left_joined) {
    for (auto entry_id = size_t{0}; entry_id < left_joined.size(); ++entry_id) {
      if (!left_joined[entry_id]) {
        _emit_combination(output_partition, output_cluster, (*left_chunk)[entry_id].row_id, NULL_ROW_ID);
      }
    }
  }

  /**
  * Emits all combinations of row ids from the right table range and a NULL value on the left side to the join output.
  **/
  void _emit_left_null_combinations(NodeID output_partition, ClusterID output_cluster, TableRange right_range) {
    right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
      _emit_combination(output_partition, output_cluster, NULL_ROW_ID, right_row_id);
    });
  }

  /**
  * Determines the length of the run starting at start_index in the values vector.
  * A run is a series of the same value.
  **/
  size_t _run_length(size_t start_index, std::shared_ptr<MaterializedChunk<T>> values) {
    if (start_index >= values->size()) {
      return 0;
    }

    auto start_position = values->begin() + start_index;
    auto result = std::upper_bound(start_position, values->end(), *start_position,
                                   [](const auto& a, const auto& b) { return a.value < b.value; });

    return result - start_position;
  }

  /**
  * Compares two values and creates a comparison result.
  **/
  ComparisonResult _compare(T left, T right) {
    if (left < right) {
      return ComparisonResult::Less;
    } else if (left == right) {
      return ComparisonResult::Equal;
    } else {
      return ComparisonResult::Greater;
    }
  }

  /**
  * Performs the join on a single cluster. Runs of entries with the same value are identified and handled together.
  * This constitutes the merge phase of the join. The output combinations of row ids are determined by _join_runs.
  **/
  void _join_cluster(ClusterID cluster_number) {
    // For MPSM join the left side is reshuffled to contain one cluster per NUMA node,
    // it is therefore the first (and only) cluster in the corresponding data structure
    const auto left_node_id = static_cast<NodeID>(cluster_number);
    const auto left_cluster_id = ClusterID{0};

    // The right side is not reshuffled and is worked on for each partition
    const auto right_cluster_id = cluster_number;

    _output_pos_lists_left[left_node_id][left_cluster_id] = std::make_shared<PosList>();

    std::shared_ptr<MaterializedChunk<T>> left_cluster =
        (*_sorted_left_table)[left_node_id]._chunk_columns[left_cluster_id];

    auto left_joined = std::vector<bool>(left_cluster->size(), false);

    for (auto right_node_id = NodeID{0}; right_node_id < static_cast<NodeID>(_cluster_count); ++right_node_id) {
      _output_pos_lists_right[right_node_id][right_cluster_id] = std::make_shared<PosList>();

      std::shared_ptr<MaterializedChunk<T>> right_cluster =
          (*_sorted_right_table)[right_node_id]._chunk_columns[right_cluster_id];

      auto left_run_start = size_t{0};
      auto right_run_start = size_t{0};

      auto left_run_end = left_run_start + _run_length(left_run_start, left_cluster);
      auto right_run_end = right_run_start + _run_length(right_run_start, right_cluster);

      const auto left_size = left_cluster->size();
      const auto right_size = right_cluster->size();

      while (left_run_start < left_size && right_run_start < right_size) {
        auto& left_value = (*left_cluster)[left_run_start].value;
        auto& right_value = (*right_cluster)[right_run_start].value;

        auto comparison_result = _compare(left_value, right_value);

        TableRange left_run(left_node_id, left_cluster_id, left_run_start, left_run_end);
        TableRange right_run(right_node_id, right_cluster_id, right_run_start, right_run_end);
        _join_runs(left_run, right_run, comparison_result, left_joined);

        // Advance to the next run on the smaller side or both if equal
        if (comparison_result == ComparisonResult::Equal) {
          // Advance both runs
          left_run_start = left_run_end;
          right_run_start = right_run_end;
          left_run_end = left_run_start + _run_length(left_run_start, left_cluster);
          right_run_end = right_run_start + _run_length(right_run_start, right_cluster);
        } else if (comparison_result == ComparisonResult::Less) {
          // Advance the left run
          left_run_start = left_run_end;
          left_run_end = left_run_start + _run_length(left_run_start, left_cluster);
        } else {
          // Advance the right run
          right_run_start = right_run_end;
          right_run_end = right_run_start + _run_length(right_run_start, right_cluster);
        }
      }

      // Join the rest of the unfinished side, which is relevant for outer joins and non-equi joins
      auto left_rest = TableRange(left_node_id, left_cluster_id, left_run_start, left_size);
      auto right_rest = TableRange(right_node_id, right_cluster_id, right_run_start, right_size);
      if (right_run_start < right_size) {
        _join_runs(left_rest, right_rest, ComparisonResult::Greater, left_joined);
      }
    }

    if (_mode == JoinMode::Left || _mode == JoinMode::Outer) {
      _emit_right_null_combinations(left_node_id, left_cluster_id, left_cluster, left_joined);
    }
  }

  /**
  * Performs the join on all clusters in parallel.
  **/
  void _perform_join() {
    auto jobs = std::vector<std::shared_ptr<JobTask>>();

    // Parallel join for each cluster
    for (auto cluster_number = ClusterID{0}; cluster_number < _cluster_count; ++cluster_number) {
      jobs.push_back(std::make_shared<JobTask>([this, cluster_number] { this->_join_cluster(cluster_number); }, true));
      jobs.back()->schedule(static_cast<NodeID>(cluster_number), SchedulePriority::JobTask);
    }

    CurrentScheduler::wait_for_tasks(jobs);
  }

  /**
  * Flattens the multiple pos lists into a single pos list
  **/
  std::shared_ptr<PosList> _concatenate_pos_lists(std::vector<std::vector<std::shared_ptr<PosList>>>& pos_lists) {
    auto output = std::make_shared<PosList>();

    // Determine the required space
    auto total_size = size_t{0};
    for (const auto& partition_lists : pos_lists) {
      for (const auto& pos_list : partition_lists) {
        total_size += pos_list->size();
      }
    }

    // Move the entries over the output pos list
    output->reserve(total_size);
    for (const auto& partition_lists : pos_lists) {
      for (const auto& pos_list : partition_lists) {
        output->insert(output->end(), pos_list->begin(), pos_list->end());
      }
    }

    return output;
  }

  /**
  * Adds the columns from an input table to the output table
  **/
  void _add_output_columns(ChunkColumns& output_columns, std::shared_ptr<const Table> input_table,
                           std::shared_ptr<const PosList> pos_list) {
    auto column_count = input_table->column_count();
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      // Add the column data (in the form of a poslist)
      if (input_table->type() == TableType::References) {
        // Create a pos_list referencing the original column instead of the reference column
        auto new_pos_list = _dereference_pos_list(input_table, column_id, pos_list);

        if (input_table->chunk_count() > 0) {
          const auto base_column = input_table->get_chunk(ChunkID{0})->get_column(column_id);
          const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(base_column);

          auto new_ref_column = std::make_shared<ReferenceColumn>(ref_column->referenced_table(),
                                                                  ref_column->referenced_column_id(), new_pos_list);
          output_columns.push_back(new_ref_column);
        } else {
          // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencING to
          // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceColumn that
          // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceColumn ref
          // it.
          const auto dummy_table = Table::create_dummy_table(input_table->column_definitions());
          output_columns.push_back(std::make_shared<ReferenceColumn>(dummy_table, column_id, pos_list));
        }
      } else {
        auto new_ref_column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
        output_columns.push_back(new_ref_column);
      }
    }
  }

  /**
  * Turns a pos list that is pointing to reference column entries into a pos list pointing to the original table.
  * This is done because there should not be any reference columns referencing reference columns.
  **/
  std::shared_ptr<PosList> _dereference_pos_list(std::shared_ptr<const Table>& input_table, ColumnID column_id,
                                                 std::shared_ptr<const PosList>& pos_list) {
    // Get all the input pos lists so that we only have to pointer cast the columns once
    auto input_pos_lists = std::vector<std::shared_ptr<const PosList>>();
    for (auto chunk_id = ChunkID{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
      auto b_column = input_table->get_chunk(chunk_id)->get_column(column_id);
      auto r_column = std::dynamic_pointer_cast<const ReferenceColumn>(b_column);
      input_pos_lists.push_back(r_column->pos_list());
    }

    // Get the row ids that are referenced
    auto new_pos_list = std::make_shared<PosList>();
    for (const auto& row : *pos_list) {
      if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
        new_pos_list->push_back(RowID{INVALID_CHUNK_ID, INVALID_CHUNK_OFFSET});
      } else {
        new_pos_list->push_back((*input_pos_lists[row.chunk_id])[row.chunk_offset]);
      }
    }

    return new_pos_list;
  }

 public:
  /**
  * Executes the MPSMJoin operator.
  **/
  std::shared_ptr<const Table> _on_execute() override {
    auto include_null_left = (_mode == JoinMode::Left || _mode == JoinMode::Outer);
    auto include_null_right = (_mode == JoinMode::Right || _mode == JoinMode::Outer);
    auto radix_clusterer =
        RadixClusterSortNUMA<T>(_mpsm_join.input_table_left(), _mpsm_join.input_table_right(), _mpsm_join._column_ids,
                                include_null_left, include_null_right, _cluster_count);
    // Sort and cluster the input tables
    auto sort_output = radix_clusterer.execute();
    _sorted_left_table = std::move(sort_output.clusters_left);
    _sorted_right_table = std::move(sort_output.clusters_right);
    _null_rows_left = std::move(sort_output.null_rows_left);
    _null_rows_right = std::move(sort_output.null_rows_right);

    // this generates the actual join results and fills the _output_pos_lists
    _perform_join();

    // merge the pos lists into single pos lists
    auto output_left = _concatenate_pos_lists(_output_pos_lists_left);
    auto output_right = _concatenate_pos_lists(_output_pos_lists_right);

    // Add the outer join rows which had a null value in their join column
    if (include_null_left) {
      for (auto row_id_left : *_null_rows_left) {
        output_left->push_back(row_id_left);
        output_right->push_back(NULL_ROW_ID);
      }
    }
    if (include_null_right) {
      for (auto row_id_right : *_null_rows_right) {
        output_left->push_back(NULL_ROW_ID);
        output_right->push_back(row_id_right);
      }
    }

    // Add the columns from both input tables to the output
    ChunkColumns output_columns;
    _add_output_columns(output_columns, _mpsm_join.input_table_left(), output_left);
    _add_output_columns(output_columns, _mpsm_join.input_table_right(), output_right);

    // Build the output_table with one Chunk
    auto output_column_definitions = concatenated(_mpsm_join.input_table_left()->column_definitions(),
                                                  _mpsm_join.input_table_right()->column_definitions());
    auto output_table = std::make_shared<Table>(output_column_definitions, TableType::References);

    output_table->append_chunk(output_columns);

    return output_table;
  }
};

}  // namespace opossum
