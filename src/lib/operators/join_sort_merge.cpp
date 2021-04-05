#include "join_sort_merge.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/functional/hash_fwd.hpp>

#include "hyrise.hpp"
#include "join_helper/join_output_writing.hpp"
#include "join_sort_merge/radix_cluster_sort.hpp"
#include "operators/multi_predicate_join/multi_predicate_join_evaluator.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {

/**
* TODO(anyone): Outer not-equal join (outer !=)
* TODO(anyone): Choose an appropriate number of clusters.
**/

bool JoinSortMerge::supports(const JoinConfiguration config) {
  if (config.left_data_type != config.right_data_type) {
    return false;
  }

  switch (config.join_mode) {
    case JoinMode::Inner:
      return true;
    case JoinMode::Cross:
      return false;
    case JoinMode::Semi:
      if (config.predicate_condition == PredicateCondition::NotEquals && !config.secondary_predicates) {
        return true;
      }
      if (config.predicate_condition == PredicateCondition::Equals) {
        return true;
      }
      return false;
    case JoinMode::AntiNullAsTrue:
      if (config.predicate_condition == PredicateCondition::NotEquals && !config.secondary_predicates) {
        return true;
      }
      if (config.predicate_condition == PredicateCondition::Equals) {
        return true;
      }
      return false;
    case JoinMode::AntiNullAsFalse:
      if (config.predicate_condition == PredicateCondition::Equals) {
        return true;
      }
      if (config.predicate_condition == PredicateCondition::NotEquals && !config.secondary_predicates) {
        return true;
      }
      return false;
    default:
      if (config.predicate_condition != PredicateCondition::NotEquals) {
        return true;
      }
      return false;
  }
}

/**
* The sort merge join performs a join on two input tables on specific join columns. For usage notes, see the
* join_sort_merge.hpp. This is how the join works:
* -> The input tables are materialized and clustered into a specified number of clusters.
*    /utils/radix_cluster_sort.hpp for more info on the clustering phase.
* -> The join is performed per cluster. For the joining phase, runs of entries with the same value are identified
*    and handled at once. If a join-match is identified, the corresponding row_ids are noted for the output.
* -> Using the join result, the output table is built using pos lists referencing the original tables.
**/
JoinSortMerge::JoinSortMerge(const std::shared_ptr<const AbstractOperator>& left,
                             const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                             const OperatorJoinPredicate& primary_predicate,
                             const std::vector<OperatorJoinPredicate>& secondary_predicates)
    : AbstractJoinOperator(OperatorType::JoinSortMerge, left, right, mode, primary_predicate, secondary_predicates,
                           std::make_unique<OperatorPerformanceData<OperatorSteps>>()) {}

std::shared_ptr<AbstractOperator> JoinSortMerge::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<JoinSortMerge>(copied_left_input, copied_right_input, _mode, _primary_predicate,
                                         _secondary_predicates);
}

void JoinSortMerge::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinSortMerge::_on_execute() {
  Assert(supports({_mode, _primary_predicate.predicate_condition,
                   left_input_table()->column_data_type(_primary_predicate.column_ids.first),
                   right_input_table()->column_data_type(_primary_predicate.column_ids.second),
                   !_secondary_predicates.empty(), left_input_table()->type(), right_input_table()->type()}),
         "JoinSortMerge doesn't support these parameters");

  Assert(_mode != JoinMode::Cross, "Sort merge join does not support cross joins.");

  std::shared_ptr<const Table> left_input_table_ptr = _left_input->get_output();
  std::shared_ptr<const Table> right_input_table_ptr = _right_input->get_output();

  // Check column types
  const auto& left_column_type = left_input_table()->column_data_type(_primary_predicate.column_ids.first);
  DebugAssert(left_column_type == right_input_table()->column_data_type(_primary_predicate.column_ids.second),
              "Left and right column types do not match. The sort merge join requires matching column types");

  // Create implementation to compute the join result
  resolve_data_type(left_column_type, [&](const auto type) {
    using ColumnDataType = typename decltype(type)::type;
    _impl = std::make_unique<JoinSortMergeImpl<ColumnDataType>>(
        *this, left_input_table_ptr, right_input_table_ptr, _primary_predicate.column_ids.first,
        _primary_predicate.column_ids.second, _primary_predicate.predicate_condition, _mode, _secondary_predicates,
        dynamic_cast<OperatorPerformanceData<JoinSortMerge::OperatorSteps>&>(*performance_data));
  });

  return _impl->_on_execute();
}

void JoinSortMerge::_on_cleanup() { _impl.reset(); }

const std::string& JoinSortMerge::name() const {
  static const auto name = std::string{"JoinSortMerge"};
  return name;
}

/**
** Start of implementation.
**/
template <typename T>
class JoinSortMerge::JoinSortMergeImpl : public AbstractReadOnlyOperatorImpl {
 public:
  JoinSortMergeImpl<T>(JoinSortMerge& sort_merge_join, const std::shared_ptr<const Table>& left_input_table,
                       const std::shared_ptr<const Table>& right_input_table, ColumnID left_column_id,
                       ColumnID right_column_id, const PredicateCondition op, JoinMode mode,
                       const std::vector<OperatorJoinPredicate>& secondary_join_predicates,
                       OperatorPerformanceData<JoinSortMerge::OperatorSteps>& performance_data)
      : _sort_merge_join{sort_merge_join},
        _left_input_table{left_input_table},
        _right_input_table{right_input_table},
        _performance{performance_data},
        _primary_left_column_id{left_column_id},
        _primary_right_column_id{right_column_id},
        _primary_predicate_condition{op},
        _mode{mode},
        _secondary_join_predicates{secondary_join_predicates} {
    _cluster_count = _determine_number_of_clusters();
    _output_pos_lists_left.resize(_cluster_count);
    _output_pos_lists_right.resize(_cluster_count);
  }

 protected:
  JoinSortMerge& _sort_merge_join;
  const std::shared_ptr<const Table> _left_input_table, _right_input_table;

  OperatorPerformanceData<JoinSortMerge::OperatorSteps>& _performance;

  // Contains the materialized sorted input tables
  std::unique_ptr<MaterializedSegmentList<T>> _sorted_left_table;
  std::unique_ptr<MaterializedSegmentList<T>> _sorted_right_table;

  // Contains the null value row ids if a join column is an outer join column
  RowIDPosList _null_rows_left;
  RowIDPosList _null_rows_right;

  const ColumnID _primary_left_column_id;
  const ColumnID _primary_right_column_id;

  const PredicateCondition _primary_predicate_condition;
  const JoinMode _mode;

  const std::vector<OperatorJoinPredicate>& _secondary_join_predicates;

  // the cluster count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
  size_t _cluster_count;

  // Contains the output row ids for each cluster
  std::vector<RowIDPosList> _output_pos_lists_left;
  std::vector<RowIDPosList> _output_pos_lists_right;

  struct RowHasher {
    size_t operator()(const RowID& row) const {
      auto seed = size_t{0};
      boost::hash_combine(seed, row.chunk_id);
      boost::hash_combine(seed, row.chunk_offset);
      return seed;
    }
  };

  using RowHashSet = std::unordered_set<RowID, RowHasher>;

  // These are used for outer joins with multiple predicates where the primary predicate is not equals.
  RowHashSet _left_row_ids_emitted{};
  RowHashSet _right_row_ids_emitted{};

  std::vector<RowHashSet> _left_row_ids_emitted_per_chunk;
  std::vector<RowHashSet> _right_row_ids_emitted_per_chunk;

  /**
   * The TablePosition is a utility struct that is used to define a specific position in a sorted input table.
  **/
  struct TableRange;
  struct TablePosition {
    TablePosition() = default;
    TablePosition(size_t init_cluster, size_t init_index) : cluster{init_cluster}, index{init_index} {}

    size_t cluster;
    size_t index;

    TableRange to(TablePosition position) { return TableRange(*this, position); }
  };

  TablePosition _end_of_left_table;
  TablePosition _end_of_right_table;

  /**
    * The TableRange is a utility struct that is used to define ranges of rows in a sorted input table spanning from
    * a start position to an end position.
  **/
  struct TableRange {
    TableRange(TablePosition start_position, TablePosition end_position) : start(start_position), end(end_position) {}
    TableRange(size_t cluster, size_t start_index, size_t end_index)
        : start{TablePosition(cluster, start_index)}, end{TablePosition(cluster, end_index)} {}

    TablePosition start;
    TablePosition end;

    // Executes the given action for every row id of the table in this range.
    template <typename F>
    void for_every_row_id(std::unique_ptr<MaterializedSegmentList<T>>& table, F action) {
// False positive with gcc and tsan (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=92194)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
      for (size_t cluster = start.cluster; cluster <= end.cluster; ++cluster) {
        size_t start_index = (cluster == start.cluster) ? start.index : 0;
        size_t end_index = (cluster == end.cluster) ? end.index : (*table)[cluster]->size();
        for (size_t index = start_index; index < end_index; ++index) {
          action((*(*table)[cluster])[index].row_id);
        }
      }
#pragma GCC diagnostic pop
    }
  };

  /**
  * Determines the number of clusters to be used for the join.
  * The number of clusters must be a power of two, i.e. 1, 2, 4, 8, 16...
  * TODO(anyone): How should we determine the number of clusters?
  **/
  size_t _determine_number_of_clusters() {
    // Get the next lower power of two of the bigger chunk number
    // Note: this is only provisional. There should be a reasonable calculation here based on hardware stats.
    size_t chunk_count_left = _sort_merge_join.left_input_table()->chunk_count();
    size_t chunk_count_right = _sort_merge_join.right_input_table()->chunk_count();
    return static_cast<size_t>(
        std::pow(2, std::floor(std::log2(std::max({size_t{1}, chunk_count_left, chunk_count_right})))));
  }

  /**
  * Gets the table position corresponding to the end of the table, i.e. the last entry of the last cluster.
  **/
  static TablePosition _end_of_table(std::unique_ptr<MaterializedSegmentList<T>>& table) {
    DebugAssert(!table->empty(), "table has no chunks");
    auto last_cluster = table->size() - 1;
    return TablePosition(last_cluster, (*table)[last_cluster]->size());
  }

  /**
  * Represents the result of a value comparison.
  **/
  enum class CompareResult { Less, Greater, Equal };

  void _emit_multipredicate_anti_null_as_true(size_t output_cluster, TableRange left_range, TableRange right_range,
                                              MultiPredicateJoinEvaluator& multi_predicate_join_evaluator) {
    left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
      auto match = false;
      for (const auto& right_row_id : _null_rows_right) {
        if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
          match = true;
          break;
        }
      }
      if (!match) {
        right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            match = true;
            return;
          }
        });
      }
      if (!match) {
        _emit_left_only(output_cluster, left_row_id);
      }
    });
  }

  void _emit_left_range_only_anti_null_as_true(size_t output_cluster, TableRange left_range,
                                               MultiPredicateJoinEvaluator& multi_predicate_join_evaluator) {
    left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
      auto match = false;
      for (const auto& right_row_id : _null_rows_right) {
        if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
          match = true;
          break;
        }
      }
      if (!match) {
        _emit_left_only(output_cluster, left_row_id);
      }
    });
  }

  /**
  * Performs the join for the left run of a specified cluster.
  * A run is a series of rows in a cluster with the same value.
  **/
  void _join_runs_anti_null_as_false(TableRange left_run, TableRange right_run, CompareResult compare_result,
                                     std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator,
                                     const size_t cluster_id) {
    switch (_primary_predicate_condition) {
      case PredicateCondition::Equals:
        // In the equal case, we only want to emit rows with values where we know that there is no match on in the
        // right table. That is the case if the comparison result is less. If we have the comparison result less
        // between the runs, it means that the runs the step before been equal, less or greater. In all cases we now
        // that there can not be a equal value on the right table if we have the comparison result less:
        // (1) -> [5] | [5] <- |  (1) -> [4] | [7] <- | (1) -> [6] | [3] <-
        //        [6] | [7]    |         [6] |        |            | [7]
        // ------------------- | -------------------- | --------------------
        // (2)    [5] | [5]    |  (2)   [4] | [7] <-  | (2) -> [6] | [3]
        //     -> [6] | [7] <- |     -> [6] |         |            | [7] <-
        // (We can not have a match for 6.)
        //  We know that if we find not equal math for a row the join condition with the multiple predicates will be
        // in any case false:  False AND ... . So we do not need to check in this case if the multiple predicates are
        // satisfied or not.
        if (compare_result == CompareResult::Less) {
          _emit_left_range_only(cluster_id, left_run);
        }
        // If the Anti join has multiple predicates we also need to check the equal cases since the primary equal
        // predicate could match but the multiple predicates might not. That means that we need to emit the row, if there
        // there is no combination where the multiple predicates are satisfied, because of as a result of that the
        // predicate expression is always false.
        if (compare_result == CompareResult::Equal && multi_predicate_join_evaluator) {
          _emit_multipredicate_semi_anti(cluster_id, left_run, right_run, *multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::NotEquals:
        // In the case of Anti not equals we only want to emit the rows where we know there is no equal match.
        if (compare_result == CompareResult::Equal) {
          _emit_left_range_only(cluster_id, left_run);
        }
        break;
      case PredicateCondition::GreaterThan:
        break;
      case PredicateCondition::GreaterThanEquals:
        break;
      case PredicateCondition::LessThan:
        break;
      case PredicateCondition::LessThanEquals:
        break;
      default:
        throw std::logic_error("Unknown PredicateCondition");
    }
  }

  void _join_runs_anti_null_as_true(TableRange left_run, TableRange right_run, CompareResult compare_result,
                                    std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator,
                                    const size_t cluster_id) {
    switch (_primary_predicate_condition) {
      case PredicateCondition::Equals:
        if (compare_result == CompareResult::Less && !multi_predicate_join_evaluator) {
          _emit_left_range_only(cluster_id, left_run);
        }
        if (compare_result == CompareResult::Less && multi_predicate_join_evaluator) {
          _emit_left_range_only_anti_null_as_true(cluster_id, left_run, *multi_predicate_join_evaluator);
        }
        if (compare_result == CompareResult::Equal && multi_predicate_join_evaluator) {
          _emit_multipredicate_anti_null_as_true(cluster_id, left_run, right_run, *multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::NotEquals:
        if (compare_result == CompareResult::Equal) {
          _emit_left_range_only(cluster_id, left_run);
        }
        break;
      case PredicateCondition::GreaterThan:
        break;
      case PredicateCondition::GreaterThanEquals:
        break;
      case PredicateCondition::LessThan:
        break;
      case PredicateCondition::LessThanEquals:
        break;
      default:
        throw std::logic_error("Unknown PredicateCondition");
    }
  }

  void _join_runs_semi(TableRange left_run, TableRange right_run, CompareResult compare_result,
                       std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator,
                       const size_t cluster_id) {
    switch (_primary_predicate_condition) {
      case PredicateCondition::Equals:
        if (compare_result == CompareResult::Equal) {
          // In the case of Semi equals all rows that have an equal match on the right table need to be checked that
          // there is at least one match where all multiple predicates are satisfied.
          if (multi_predicate_join_evaluator) {
            _emit_multipredicate_semi_anti(cluster_id, left_run, right_run, *multi_predicate_join_evaluator);
          }
          // If there are no equal predicates a single equal match is sufficient.
          else {
            _emit_left_range_only(cluster_id, left_run);
          }
        }
        break;
      case PredicateCondition::NotEquals:
        // We only get here if the right table has only one value. It must be ensured that the comparative value is not
        // the same.
        if (compare_result != CompareResult::Equal) {
          _emit_left_range_only(cluster_id, left_run);
        }
        break;
      case PredicateCondition::GreaterThan:
        break;
      case PredicateCondition::GreaterThanEquals:
        break;
      case PredicateCondition::LessThan:
        break;
      case PredicateCondition::LessThanEquals:
        break;
      default:
        throw std::logic_error("Unknown PredicateCondition");
    }
  }

  /**
  * Performs the join for two runs of a specified cluster.
  * A run is a series of rows in a cluster with the same value.
  **/
  void _join_runs(TableRange left_run, TableRange right_run, CompareResult compare_result,
                  std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator, const size_t cluster_id) {
    switch (_primary_predicate_condition) {
      case PredicateCondition::Equals:
        if (compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_id, left_run, right_run, multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Less) {
          if (_mode == JoinMode::Left || _mode == JoinMode::FullOuter) {
            _emit_right_primary_null_combinations(cluster_id, left_run);
          }
        } else if (compare_result == CompareResult::Greater) {
          if (_mode == JoinMode::Right || _mode == JoinMode::FullOuter) {
            _emit_left_primary_null_combinations(cluster_id, right_run);
          }
        }
        break;
      case PredicateCondition::NotEquals:
        if (compare_result == CompareResult::Greater) {
          _emit_qualified_combinations(cluster_id, left_run.start.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_id, left_run.end.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
          _emit_qualified_combinations(cluster_id, left_run, right_run.end.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Less) {
          _emit_qualified_combinations(cluster_id, left_run, right_run.start.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::GreaterThan:
        if (compare_result == CompareResult::Greater) {
          _emit_qualified_combinations(cluster_id, left_run.start.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_id, left_run.end.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::GreaterThanEquals:
        if (compare_result == CompareResult::Greater || compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_id, left_run.start.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::LessThan:
        if (compare_result == CompareResult::Less) {
          _emit_qualified_combinations(cluster_id, left_run, right_run.start.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_id, left_run, right_run.end.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::LessThanEquals:
        if (compare_result == CompareResult::Less || compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_id, left_run, right_run.start.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        }
        break;
      default:
        throw std::logic_error("Unknown PredicateCondition");
    }
  }

  /**
  * Emits a combination of a left row id and a right row id to the join output.
  **/
  void _emit_combination(size_t output_cluster, RowID left_row_id, RowID right_row_id) {
    _output_pos_lists_left[output_cluster].push_back(left_row_id);
    _output_pos_lists_right[output_cluster].push_back(right_row_id);
  }

  // Is only used for anti and semi Joins.
  void _emit_left_only(size_t output_cluster, RowID left_row_id) {
    _output_pos_lists_left[output_cluster].push_back(left_row_id);
  }

  /**
    * Emits all the combinations of row ids from the left table range and the right table range to the join output
    * where also the secondary predicates are satisfied.
    **/
  void _emit_qualified_combinations(size_t output_cluster, TableRange left_range, TableRange right_range,
                                    std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator) {
    if (multi_predicate_join_evaluator) {
      if (_mode == JoinMode::Inner) {
        _emit_combinations_multi_predicated_inner(output_cluster, left_range, right_range,
                                                  *multi_predicate_join_evaluator);
      } else if (_mode == JoinMode::Left) {
        _emit_combinations_multi_predicated_left_outer(output_cluster, left_range, right_range,
                                                       *multi_predicate_join_evaluator);
      } else if (_mode == JoinMode::Right) {
        _emit_combinations_multi_predicated_right_outer(output_cluster, left_range, right_range,
                                                        *multi_predicate_join_evaluator);
      } else if (_mode == JoinMode::FullOuter) {
        _emit_combinations_multi_predicated_full_outer(output_cluster, left_range, right_range,
                                                       *multi_predicate_join_evaluator);
      }
    } else {
      // no secondary join predicates
      left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
        right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
          _emit_combination(output_cluster, left_row_id, right_row_id);
        });
      });
    }
  }

  /**
   * Only for multi predicated inner joins.
   * Emits all the combinations of row ids from the left table range and the right table range to the join output
   * where the secondary predicates are satisfied.
   **/
  void _emit_combinations_multi_predicated_inner(size_t output_cluster, TableRange left_range, TableRange right_range,
                                                 MultiPredicateJoinEvaluator& multi_predicate_join_evaluator) {
    left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
      right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
        if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
          _emit_combination(output_cluster, left_row_id, right_row_id);
        }
      });
    });
  }

  /**
  * Only for multi predicated left outer joins.
  * Emits all the combinations of row ids from the left table range and the right table range to the join output
  * where the secondary predicates are satisfied.
  * For a left row id without a match, the combination [left row id|NULL row id] is emitted.
  **/
  void _emit_combinations_multi_predicated_left_outer(size_t output_cluster, TableRange left_range,
                                                      TableRange right_range,
                                                      MultiPredicateJoinEvaluator& multi_predicate_join_evaluator) {
    if (_primary_predicate_condition == PredicateCondition::Equals) {
      left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
        bool left_row_id_matched = false;
        right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            left_row_id_matched = true;
          }
        });
        if (!left_row_id_matched) {
          _emit_combination(output_cluster, left_row_id, NULL_ROW_ID);
        }
      });
    } else {
      // primary predicate is <, <=, >, or >=
      left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
        right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            _left_row_ids_emitted_per_chunk[output_cluster].emplace(left_row_id);
          }
        });
      });
    }
  }

  /**
    * Only for multi predicated right outer joins.
    * Emits all the combinations of row ids from the left table range and the right table range to the join output
    * where the secondary predicates are satisfied.
    * For a right row id without a match, the combination [NULL row id|right row id] is emitted.
    **/
  void _emit_combinations_multi_predicated_right_outer(size_t output_cluster, TableRange left_range,
                                                       TableRange right_range,
                                                       MultiPredicateJoinEvaluator& multi_predicate_join_evaluator) {
    if (_primary_predicate_condition == PredicateCondition::Equals) {
      right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
        bool right_row_id_matched = false;
        left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            right_row_id_matched = true;
          }
        });
        if (!right_row_id_matched) {
          _emit_combination(output_cluster, NULL_ROW_ID, right_row_id);
        }
      });
    } else {
      // primary predicate is <, <=, >, or >=
      right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
        left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            _right_row_ids_emitted_per_chunk[output_cluster].emplace(right_row_id);
          }
        });
      });
    }
  }

  /**
    * Only for multi predicated full outer joins.
    * Emits all the combinations of row ids from the left table range and the right table range to the join output
    * where the secondary predicates are satisfied.
    * For a left row id without a match, the combination [right row id|NULL row id] is emitted.
    * For a right row id without a match, the combination [NULL row id|right row id] is emitted.
    **/
  void _emit_combinations_multi_predicated_full_outer(size_t output_cluster, TableRange left_range,
                                                      TableRange right_range,
                                                      MultiPredicateJoinEvaluator& multi_predicate_join_evaluator) {
    if (_primary_predicate_condition == PredicateCondition::Equals) {
      std::set<RowID> matched_right_row_ids;

      left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
        bool left_row_id_matched = false;
        right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            left_row_id_matched = true;
            matched_right_row_ids.insert(right_row_id);
          }
        });
        if (!left_row_id_matched) {
          _emit_combination(output_cluster, left_row_id, NULL_ROW_ID);
        }
      });
      // add null value combinations for right row ids that have no match.
      right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
        if (matched_right_row_ids.count(right_row_id) == 0) {
          _emit_combination(output_cluster, NULL_ROW_ID, right_row_id);
        }
      });
    } else {
      left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
        right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            _left_row_ids_emitted_per_chunk[output_cluster].emplace(left_row_id);
            _right_row_ids_emitted_per_chunk[output_cluster].emplace(right_row_id);
          }
        });
      });
    }
  }

  // In the case of the anti-join, we need to check that there is no combination where all predicates are satisfied. In
  // the case of the semi-join, we need to check that there is at least one combination where all predicates are
  // satisfied.
  void _emit_multipredicate_semi_anti(size_t output_cluster, TableRange left_range, TableRange right_range,
                                      MultiPredicateJoinEvaluator& multi_predicate_join_evaluator) {
    left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
      auto match = false;
      right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
        if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
          match = true;
          return;
        }
      });
      if (match && _mode == JoinMode::Semi) {
        _emit_left_only(output_cluster, left_row_id);
      } else {
        if (!match && _mode == JoinMode::AntiNullAsFalse) {
          _emit_left_only(output_cluster, left_row_id);
        }
      }
    });
  }

  /**
  * Emits all combinations of row ids from the left table range and a NULL value on the right side
  * (regarding the primary predicate) to the join output.
  **/
  void _emit_right_primary_null_combinations(size_t output_cluster, TableRange left_range) {
    left_range.for_every_row_id(
        _sorted_left_table, [&](RowID left_row_id) { _emit_combination(output_cluster, left_row_id, NULL_ROW_ID); });
  }

  /**
  * Emits all combinations of row ids from the right table range and a NULL value on the left side
  * (regarding the primary predicate) to the join output.
  **/
  void _emit_left_primary_null_combinations(size_t output_cluster, TableRange right_range) {
    right_range.for_every_row_id(
        _sorted_right_table, [&](RowID right_row_id) { _emit_combination(output_cluster, NULL_ROW_ID, right_row_id); });
  }

  void _emit_left_range_only(size_t output_cluster, TableRange left_range) {
    left_range.for_every_row_id(_sorted_left_table,
                                [&](RowID left_row_id) { _emit_left_only(output_cluster, left_row_id); });
  }

  /**
  * Determines the length of the run starting at start_index in the values vector.
  * A run is a series of the same value.
  **/
  size_t _run_length(size_t start_index, std::shared_ptr<MaterializedSegment<T>> values) {
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
  CompareResult _compare(T left, T right) {
    if (left < right) {
      return CompareResult::Less;
    } else if (left == right) {
      return CompareResult::Equal;
    } else {
      return CompareResult::Greater;
    }
  }

  // Performs the join on a single cluster. Runs of entries with the same value are identified and handled together.
  // This constitutes the merge phase of the join. The output combinations of row ids are determined by _join_runs.
  // The main logic of the algorithm is the following:
  // We have for the left and right side a list of runs. There are also pointers that are pointing to the current run
  // on either side of the lists. The values of the runs are compared and then we determine which pointer will be
  // advanced. If the comparison result is equal we advance both pointers, if it is less we advance the left one if it
  // greater we advance the right one:
  //          Equal      |            Less      |         Greater
  // (1) -> [2] | [2] <- |  (1) -> [3] | [4] <- | (1) -> [6] | [4] <-
  //        [3] | [4]    |         [6] | [7]    |        [7] | [7]
  //        [6] | [7]    |         [7] | [9]    |        [8] | [9]
  // ------------------- | -------------------- | --------------------
  // (2)    [2] | [2]    |  (2)   [3] | [4] <-  | (2) -> [6] | [3]
  //     -> [3] | [4] <- |     -> [6] | [7]     |        [7] | [7] <-
  //        [6] | [7]    |        [7] | [9]     |        [8] | [9]
  // We can than use in every step the comparison information to join the combinations. For example if we have the
  // equal comparison and an equal join, we now that we emit the cross product between the rows from the left and right
  // and run.
  void _join_cluster(const size_t cluster_id,
                     std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator) {
    auto& left_cluster = (*_sorted_left_table)[cluster_id];
    auto& right_cluster = (*_sorted_right_table)[cluster_id];

    size_t left_run_start = 0;
    size_t right_run_start = 0;

    auto left_run_end = left_run_start + _run_length(left_run_start, left_cluster);
    auto right_run_end = right_run_start + _run_length(right_run_start, right_cluster);

    const size_t left_size = left_cluster->size();
    const size_t right_size = right_cluster->size();

    while (left_run_start < left_size && right_run_start < right_size) {
      auto& left_value = (*left_cluster)[left_run_start].value;
      auto& right_value = (*right_cluster)[right_run_start].value;

      auto compare_result = _compare(left_value, right_value);

      TableRange left_run(cluster_id, left_run_start, left_run_end);
      TableRange right_run(cluster_id, right_run_start, right_run_end);

      if (_mode == JoinMode::AntiNullAsFalse) {
        _join_runs_anti_null_as_false(left_run, right_run, compare_result, multi_predicate_join_evaluator, cluster_id);
      } else if (_mode == JoinMode::AntiNullAsTrue) {
        _join_runs_anti_null_as_true(left_run, right_run, compare_result, multi_predicate_join_evaluator, cluster_id);
      } else if (_mode == JoinMode::Semi) {
        _join_runs_semi(left_run, right_run, compare_result, multi_predicate_join_evaluator, cluster_id);
      } else {
        _join_runs(left_run, right_run, compare_result, multi_predicate_join_evaluator, cluster_id);
      }

      // Advance to the next run on the smaller side or both if equal
      if (compare_result == CompareResult::Equal) {
        // Advance both runs
        left_run_start = left_run_end;
        right_run_start = right_run_end;
        left_run_end = left_run_start + _run_length(left_run_start, left_cluster);
        right_run_end = right_run_start + _run_length(right_run_start, right_cluster);
      } else if (compare_result == CompareResult::Less) {
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
    auto right_rest = TableRange(cluster_id, right_run_start, right_size);
    auto left_rest = TableRange(cluster_id, left_run_start, left_size);

    if (left_run_start < left_size) {
      if (_mode == JoinMode::AntiNullAsFalse) {
        // If the predicate condition is not equal, we do not need to emit anything since there are no equal matches
        // in the last left runs.
        if (_primary_predicate_condition == PredicateCondition::Equals) {
          // If the predicate condition is equal we know that there are no matches for the rest of the left runs.
          // Through the radix clustering we know that there can also be no match inside the other clusters, because
          // if there would be it would be in this cluster.
          _emit_left_range_only(cluster_id, left_rest);
        }
      } else if (_mode == JoinMode::AntiNullAsTrue) {
        if (_primary_predicate_condition == PredicateCondition::Equals) {
          if (multi_predicate_join_evaluator) {
            _emit_left_range_only_anti_null_as_true(cluster_id, left_rest, *multi_predicate_join_evaluator);
          } else {
            _emit_left_range_only(cluster_id, left_rest);
          }
        }
      } else if (_mode == JoinMode::Semi) {
        if (_primary_predicate_condition == PredicateCondition::NotEquals) {
          // We only get here if the right table has only one value. Since we know at this that there is no equal match
          // to the one value, we can emit the rest of the left ranges.
          _emit_left_range_only(cluster_id, left_rest);
        }
      } else {
        _join_runs(left_rest, right_rest, CompareResult::Less, multi_predicate_join_evaluator, cluster_id);
      }
    } else if (right_run_start < right_size) {
      // We know that there is no match between the left runs and the rest of the right runs. In the case of Anti/Semi
      // we do not need to emit any rows anymore, since we already worked off all left runs.
      if ((_mode != JoinMode::AntiNullAsTrue && _mode != JoinMode::AntiNullAsFalse && _mode != JoinMode::Semi)) {
        _join_runs(left_rest, right_rest, CompareResult::Greater, multi_predicate_join_evaluator, cluster_id);
      }
    }
  }

  /**
  * Determines the smallest value in a sorted materialized table.
  **/
  T& _table_min_value(std::unique_ptr<MaterializedSegmentList<T>>& sorted_table) {
    DebugAssert(_primary_predicate_condition != PredicateCondition::Equals,
                "Complete table order is required for _table_min_value() which is only available in the non-equi case");
    DebugAssert(!sorted_table->empty(), "Sorted table has no partitions");

    for (const auto& partition : *sorted_table) {
      if (!partition->empty()) {
        return (*partition)[0].value;
      }
    }

    Fail("Every partition is empty");
  }

  /**
  * Determines the largest value in a sorted materialized table.
  **/
  T& _table_max_value(std::unique_ptr<MaterializedSegmentList<T>>& sorted_table) {
    DebugAssert(
        !(_primary_predicate_condition == PredicateCondition::Equals &&
          !((_mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue) &&
            _sort_merge_join.left_input_table()->row_count() > _sort_merge_join.right_input_table()->row_count())),
        "The table needs to be sorted for _table_max_value() which is only the case in the non-equi case");
    DebugAssert(!sorted_table->empty(), "Sorted table is empty");

    for (size_t partition_id = sorted_table->size() - 1; partition_id < sorted_table->size(); --partition_id) {
      if (!(*sorted_table)[partition_id]->empty()) {
        return (*sorted_table)[partition_id]->back().value;
      }
    }

    Fail("Every partition is empty");
  }

  /**
  * Looks for the first value in a sorted materialized table that fulfills the specified condition.
  * Returns the TablePosition of this element and whether a satisfying element has been found.
  **/
  template <typename Function>
  std::optional<TablePosition> _first_value_that_satisfies(std::unique_ptr<MaterializedSegmentList<T>>& sorted_table,
                                                           Function condition) {
    for (size_t partition_id = 0; partition_id < sorted_table->size(); ++partition_id) {
      auto partition = (*sorted_table)[partition_id];
      if (!partition->empty() && condition(partition->back().value)) {
        for (size_t index = 0; index < partition->size(); ++index) {
          if (condition((*partition)[index].value)) {
            return TablePosition(partition_id, index);
          }
        }
      }
    }

    return {};
  }

  /**
  * Looks for the first value in a sorted materialized table that fulfills the specified condition, but searches
  * the table in reverse order. Returns the TablePosition of this element, and a satisfying element has been found.
  **/
  template <typename Function>
  std::optional<TablePosition> _first_value_that_satisfies_reverse(
      std::unique_ptr<MaterializedSegmentList<T>>& sorted_table, Function condition) {
    for (size_t partition_id = sorted_table->size() - 1; partition_id < sorted_table->size(); --partition_id) {
      auto partition = (*sorted_table)[partition_id];
      if (!partition->empty() && condition((*partition)[0].value)) {
        for (size_t index = partition->size() - 1; index < partition->size(); --index) {
          if (condition((*partition)[index].value)) {
            return TablePosition(partition_id, index + 1);
          }
        }
      }
    }

    return {};
  }

  /**
  * Adds the rows without matches for right outer joins for non-equi operators (<, <=, >, >=).
  * This method adds those rows from the right table to the output that do not find a join partner.
  * The outer join for the equality operator is handled in _join_runs instead.
  **/
  void _right_outer_non_equi_join() {
    auto end_of_right_table = _end_of_table(_sorted_right_table);

    if (_sort_merge_join.left_input_table()->row_count() == 0) {
      _emit_left_primary_null_combinations(0, TablePosition(0, 0).to(end_of_right_table));
      return;
    }

    auto& left_min_value = _table_min_value(_sorted_left_table);
    auto& left_max_value = _table_max_value(_sorted_left_table);

    auto unmatched_range = std::optional<TableRange>{};

    if (_primary_predicate_condition == PredicateCondition::LessThan) {
      // Look for the first right value that is bigger than the smallest left value.
      auto result =
          _first_value_that_satisfies(_sorted_right_table, [&](const T& value) { return value > left_min_value; });
      if (result) {
        unmatched_range = TablePosition(0, 0).to(*result);
      }
    } else if (_primary_predicate_condition == PredicateCondition::LessThanEquals) {
      // Look for the first right value that is bigger or equal to the smallest left value.
      auto result =
          _first_value_that_satisfies(_sorted_right_table, [&](const T& value) { return value >= left_min_value; });
      if (result) {
        unmatched_range = TablePosition(0, 0).to(*result);
      }
    } else if (_primary_predicate_condition == PredicateCondition::GreaterThan) {
      // Look for the first right value that is smaller than the biggest left value.
      auto result = _first_value_that_satisfies_reverse(_sorted_right_table,
                                                        [&](const T& value) { return value < left_max_value; });
      if (result) {
        unmatched_range = (*result).to(end_of_right_table);
      }
    } else if (_primary_predicate_condition == PredicateCondition::GreaterThanEquals) {
      // Look for the first right value that is smaller or equal to the biggest left value.
      auto result = _first_value_that_satisfies_reverse(_sorted_right_table,
                                                        [&](const T& value) { return value <= left_max_value; });
      if (result) {
        unmatched_range = (*result).to(end_of_right_table);
      }
    }

    if (unmatched_range) {
      _emit_left_primary_null_combinations(0, *unmatched_range);
      unmatched_range->for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
        // Mark as emitted so that it doesn't get emitted again below
        _right_row_ids_emitted.emplace(right_row_id);
      });
    }

    // Add null-combinations for right row ids where the primary predicate was satisfied but the
    // secondary predicates were not.
    if (!_secondary_join_predicates.empty()) {
      for (const auto& cluster : *_sorted_right_table) {
        for (const auto& row : *cluster) {
          if (!_right_row_ids_emitted.contains(row.row_id)) {
            _emit_combination(0, NULL_ROW_ID, row.row_id);
          }
        }
      }
    }
  }

  /**
    * Adds the rows without matches for left outer joins for non-equi operators (<, <=, >, >=).
    * This method adds those rows from the left table to the output that do not find a join partner.
    * The outer join for the equality operator is handled in _join_runs instead.
    **/
  void _left_outer_non_equi_join() {
    auto end_of_left_table = _end_of_table(_sorted_left_table);

    if (_sort_merge_join.right_input_table()->row_count() == 0) {
      _emit_right_primary_null_combinations(0, TablePosition(0, 0).to(end_of_left_table));
      return;
    }

    auto& right_min_value = _table_min_value(_sorted_right_table);
    auto& right_max_value = _table_max_value(_sorted_right_table);

    auto unmatched_range = std::optional<TableRange>{};

    if (_primary_predicate_condition == PredicateCondition::LessThan) {
      // Look for the last left value that is smaller than the biggest right value.
      auto result = _first_value_that_satisfies_reverse(_sorted_left_table,
                                                        [&](const T& value) { return value < right_max_value; });
      if (result) {
        unmatched_range = (*result).to(end_of_left_table);
      }
    } else if (_primary_predicate_condition == PredicateCondition::LessThanEquals) {
      // Look for the last left value that is smaller or equal than the biggest right value.
      auto result = _first_value_that_satisfies_reverse(_sorted_left_table,
                                                        [&](const T& value) { return value <= right_max_value; });
      if (result) {
        unmatched_range = (*result).to(end_of_left_table);
      }
    } else if (_primary_predicate_condition == PredicateCondition::GreaterThan) {
      // Look for the first left value that is bigger than the smallest right value.
      auto result =
          _first_value_that_satisfies(_sorted_left_table, [&](const T& value) { return value > right_min_value; });
      if (result) {
        unmatched_range = TablePosition(0, 0).to(*result);
      }
    } else if (_primary_predicate_condition == PredicateCondition::GreaterThanEquals) {
      // Look for the first left value that is bigger or equal to the smallest right value.
      auto result =
          _first_value_that_satisfies(_sorted_left_table, [&](const T& value) { return value >= right_min_value; });
      if (result) {
        unmatched_range = TablePosition(0, 0).to(*result);
      }
    }

    if (unmatched_range) {
      _emit_right_primary_null_combinations(0, *unmatched_range);
      unmatched_range->for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
        // Mark as emitted so that it doesn't get emitted again below
        _left_row_ids_emitted.emplace(left_row_id);
      });
    }

    // Add null-combinations for left row ids where the primary predicate was satisfied but the
    // secondary predicates were not.
    if (!_secondary_join_predicates.empty()) {
      for (const auto& cluster : *_sorted_left_table) {
        for (const auto& row : *cluster) {
          if (!_left_row_ids_emitted.contains(row.row_id)) {
            _emit_combination(0, row.row_id, NULL_ROW_ID);
          }
        }
      }
    }
  }

  /**
  * Performs the join on all clusters in parallel.
  **/
  void _perform_join() {
    std::vector<std::shared_ptr<AbstractTask>> jobs;

    _left_row_ids_emitted_per_chunk.resize(_cluster_count);
    _right_row_ids_emitted_per_chunk.resize(_cluster_count);

    auto right_min_value = T{};
    auto right_max_value = T{};

    if (_mode == JoinMode::Semi && _primary_predicate_condition == PredicateCondition::NotEquals) {
      right_min_value = _table_min_value(_sorted_right_table);
      right_max_value = _table_max_value(_sorted_right_table);
    }

    // Parallel join for each cluster
    for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
      // Create output position lists
      _output_pos_lists_left[cluster_id] = RowIDPosList{};
      _output_pos_lists_right[cluster_id] = RowIDPosList{};

      // Avoid empty jobs for inner equi joins
      if ((_mode == JoinMode::Inner || _mode == JoinMode::Semi) &&
          _primary_predicate_condition == PredicateCondition::Equals) {
        if ((*_sorted_left_table)[cluster_id]->empty() || (*_sorted_right_table)[cluster_id]->empty()) {
          continue;
        }
      }

      _left_row_ids_emitted_per_chunk[cluster_id] = RowHashSet{};
      _right_row_ids_emitted_per_chunk[cluster_id] = RowHashSet{};

      const auto merge_row_count =
          (*_sorted_left_table)[cluster_id]->size() + (*_sorted_right_table)[cluster_id]->size();
      const auto join_cluster_task = [this, cluster_id, right_min_value, right_max_value] {
        // Accessors are not thread-safe, so we create one evaluator per job
        std::optional<MultiPredicateJoinEvaluator> multi_predicate_join_evaluator;
        if (!_secondary_join_predicates.empty()) {
          multi_predicate_join_evaluator.emplace(*_sort_merge_join._left_input->get_output(),
                                                 *_sort_merge_join.right_input()->get_output(), _mode,
                                                 _secondary_join_predicates);
        }
        if (_mode == JoinMode::Semi && _primary_predicate_condition == PredicateCondition::NotEquals) {
          // We need to check if there are at lest two values. As a result all left rows will always find at least one
          // match. Is that the case we want to emit all rows from the left table with the exception where the value
          // is NULL.
          // If there is only one value, we need to run the join algorithm and check for equality.
          if (right_min_value != right_max_value) {
            size_t start_index = 0;
            size_t end_index = (*_sorted_left_table)[cluster_id]->size();
            for (size_t index = start_index; index < end_index; ++index) {
              _output_pos_lists_left[cluster_id].push_back((*(*_sorted_left_table)[cluster_id])[index].row_id);
            }
            return;
          }
        }
        this->_join_cluster(cluster_id, multi_predicate_join_evaluator);
      };

      if (merge_row_count > JOB_SPAWN_THRESHOLD * 2) {
        jobs.push_back(std::make_shared<JobTask>(join_cluster_task));
      } else {
        join_cluster_task();
      }
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

    // The outer joins for the non-equi cases
    // Note: Equi outer joins can be integrated into the main algorithm, while these can not.
    if ((_mode == JoinMode::Left || _mode == JoinMode::FullOuter) &&
        _primary_predicate_condition != PredicateCondition::Equals) {
      for (auto& set : _left_row_ids_emitted_per_chunk) {
        _left_row_ids_emitted.merge(set);
      }
      _left_outer_non_equi_join();
    }
    if ((_mode == JoinMode::Right || _mode == JoinMode::FullOuter) &&
        _primary_predicate_condition != PredicateCondition::Equals) {
      for (auto& set : _right_row_ids_emitted_per_chunk) {
        _right_row_ids_emitted.merge(set);
      }
      _right_outer_non_equi_join();
    }
  }

  bool _check_if_table_contains_null(const std::shared_ptr<const Table> table) {
    const auto table_column_definition =
        table->column_definitions()[_sort_merge_join._primary_predicate.column_ids.second];
    if (table_column_definition.nullable == false) return false;
    const auto chunk_count = table->chunk_count();
    auto has_null = false;
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      auto segment = table->get_chunk(chunk_id)->get_segment(_sort_merge_join._primary_predicate.column_ids.second);
      segment_iterate<T>(*segment, [&](const auto& position) {
        if (position.is_null()) {
          has_null = true;
          return;
        }
      });
      if (has_null) return true;
    }
    return false;
  }

 public:
  /**
  * Executes the SortMergeJoin operator.
  **/
  std::shared_ptr<const Table> _on_execute() override {
    if (_mode == JoinMode::Semi) {
      if (_primary_predicate_condition == PredicateCondition::NotEquals &&
          _sort_merge_join.right_input_table()->empty()) {
        return _sort_merge_join.left_input_table();
      }
      if (_primary_predicate_condition == PredicateCondition::Equals && _sort_merge_join.right_input_table()->empty()) {
        return Table::create_dummy_table(_sort_merge_join.left_input_table()->column_definitions());
      }
    }
    if (_mode == JoinMode::AntiNullAsFalse && _sort_merge_join.right_input_table()->empty()) {
      return _sort_merge_join.left_input_table();
    }
    if (_mode == JoinMode::AntiNullAsTrue) {
      if (_sort_merge_join.right_input_table()->empty()) {
        return _sort_merge_join.left_input_table();
      }
      if (_secondary_join_predicates.empty()) {
        auto right_side_has_null = _check_if_table_contains_null(_sort_merge_join.right_input_table());
        if (right_side_has_null) {
          return Table::create_dummy_table(_sort_merge_join.left_input_table()->column_definitions());
        }
      }
    }

    auto sort = true;
    if (_mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue) {
      sort = !(_primary_predicate_condition == PredicateCondition::NotEquals);
    } else {
      sort = !(_primary_predicate_condition == PredicateCondition::Equals);
    }

    auto include_null_left =
        (_mode == JoinMode::Left || _mode == JoinMode::FullOuter || _mode == JoinMode::AntiNullAsFalse ||
         (_mode == JoinMode::AntiNullAsTrue && !_secondary_join_predicates.empty()));
    auto include_null_right = (_mode == JoinMode::Right || _mode == JoinMode::FullOuter ||
                               _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue);
    auto radix_clusterer =
        RadixClusterSort<T>(_sort_merge_join.left_input_table(), _sort_merge_join.right_input_table(),
                            _sort_merge_join._primary_predicate.column_ids, sort, include_null_left,
                            include_null_right, _cluster_count, _performance);
    // Sort and cluster the input tables
    auto sort_output = radix_clusterer.execute();
    _sorted_left_table = std::move(sort_output.clusters_left);
    _sorted_right_table = std::move(sort_output.clusters_right);
    _null_rows_left = std::move(*sort_output.null_rows_left);
    _null_rows_right = std::move(*sort_output.null_rows_right);
    _end_of_left_table = _end_of_table(_sorted_left_table);
    _end_of_right_table = _end_of_table(_sorted_right_table);

    Timer timer;

    if (_mode == JoinMode::AntiNullAsFalse) {
      include_null_right = false;
    }
    if (_mode == JoinMode::AntiNullAsTrue) {
      include_null_right = false;
      include_null_left = false;
    }

    if (_mode == JoinMode::AntiNullAsFalse && _primary_predicate_condition == PredicateCondition::NotEquals &&
        !_null_rows_right.empty()) {
      // We only want to emit the rows where the value is NULL.
      _output_pos_lists_left[0] = RowIDPosList{};
      _output_pos_lists_right[0] = RowIDPosList{};
    } else {
      _perform_join();
    }

    if (_mode == JoinMode::AntiNullAsTrue && !_secondary_join_predicates.empty()) {
      auto null_output_left = RowIDPosList();

      null_output_left.reserve(_null_rows_left.size());

      MultiPredicateJoinEvaluator multi_predicate_join_evaluator(*_sort_merge_join._left_input->get_output(),
                                                                 *_sort_merge_join.right_input()->get_output(), _mode,
                                                                 _secondary_join_predicates);
      for (const auto& row_id_left : _null_rows_left) {
        auto end_of_right_table = _end_of_table(_sorted_right_table);
        auto right_range = TablePosition(0, 0).to(end_of_right_table);
        auto match = false;
        right_range.for_every_row_id(_sorted_right_table, [&](RowID row_id_right) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(row_id_left, row_id_right)) {
            match = true;
            return;
          }
        });
        if (!match) {
          for (const auto& row_id_right : _null_rows_right) {
            if (multi_predicate_join_evaluator.satisfies_all_predicates(row_id_left, row_id_right)) {
              match = true;
              break;
            }
          }
        }
        if (!match) {
          null_output_left.push_back(row_id_left);
        }
      }
      if (!null_output_left.empty()) {
        _output_pos_lists_left.push_back(std::move(null_output_left));
        _output_pos_lists_right.push_back(RowIDPosList());
      }
    }

    if (include_null_left || include_null_right) {
      auto null_output_left = RowIDPosList();
      auto null_output_right = RowIDPosList();

      // Add the outer join rows which had a null value in their join column
      if (include_null_left) {
        null_output_left.reserve(_null_rows_left.size());
        null_output_right.insert(null_output_right.end(), _null_rows_left.size(), NULL_ROW_ID);
        for (const auto& row_id_left : _null_rows_left) {
          null_output_left.push_back(row_id_left);
        }
      }
      if (include_null_right) {
        null_output_left.insert(null_output_left.end(), _null_rows_right.size(), NULL_ROW_ID);
        null_output_right.reserve(_null_rows_right.size());
        for (const auto& row_id_right : _null_rows_right) {
          null_output_right.push_back(row_id_right);
        }
      }

      DebugAssert(null_output_left.size() == null_output_right.size(),
                  "Null positions lists are expected to be of equal length.");
      if (!null_output_left.empty()) {
        _output_pos_lists_left.push_back(std::move(null_output_left));
        _output_pos_lists_right.push_back(std::move(null_output_right));
      }
    }
    _performance.set_step_runtime(OperatorSteps::Merging, timer.lap());

    const auto create_left_side_pos_lists_by_segment = (_left_input_table->type() == TableType::References);
    const auto create_right_side_pos_lists_by_segment = (_right_input_table->type() == TableType::References);

    // A sort merge join's input can be heavily pre-filtered or the join results in very few matches. In contrast to
    // the hash join, we do not (for now) merge small partitions to keep the sorted chunk guarantees, which could be
    // exploited by subsequent operators.
    constexpr auto ALLOW_PARTITION_MERGE = false;
    auto output_column_order = OutputColumnOrder::LeftFirstRightSecond;
    if (_mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsTrue || _mode == JoinMode::AntiNullAsFalse) {
      output_column_order = OutputColumnOrder::LeftOnly;
    }
    auto output_chunks =
        write_output_chunks(_output_pos_lists_left, _output_pos_lists_right, _left_input_table, _right_input_table,
                            create_left_side_pos_lists_by_segment, create_right_side_pos_lists_by_segment,
                            output_column_order, ALLOW_PARTITION_MERGE);

    const ColumnID left_join_column = _sort_merge_join._primary_predicate.column_ids.first;
    const ColumnID right_join_column = static_cast<ColumnID>(_sort_merge_join.left_input_table()->column_count() +
                                                             _sort_merge_join._primary_predicate.column_ids.second);

    for (auto& chunk : output_chunks) {
      if (_sort_merge_join._primary_predicate.predicate_condition == PredicateCondition::Equals &&
          _mode == JoinMode::Inner) {
        chunk->finalize();
        // The join columns are sorted in ascending order (ensured by radix_cluster_sort)
        chunk->set_individually_sorted_by({SortColumnDefinition(left_join_column, SortMode::Ascending),
                                           SortColumnDefinition(right_join_column, SortMode::Ascending)});
      }
    }

    _performance.set_step_runtime(OperatorSteps::OutputWriting, timer.lap());

    auto result_table = _sort_merge_join._build_output_table(std::move(output_chunks));

    if (_mode != JoinMode::Left && _mode != JoinMode::Right && _mode != JoinMode::FullOuter &&
        _sort_merge_join._primary_predicate.predicate_condition == PredicateCondition::Equals &&
        _mode != JoinMode::Semi && _mode != JoinMode::AntiNullAsTrue && _mode != JoinMode::AntiNullAsFalse) {
      // Table clustering is not defined for columns storing NULL values. Additionally, clustering is not given for
      // non-equal predicates.
      result_table->set_value_clustered_by({left_join_column, right_join_column});
    }
    return result_table;
  }
};

}  // namespace opossum
