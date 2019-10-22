#include "join_sort_merge.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "join_sort_merge/radix_cluster_sort.hpp"
#include "operators/multi_predicate_join/multi_predicate_join_evaluator.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/abstract_segment_visitor.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {

/**
* TODO(anyone): Outer not-equal join (outer !=)
* TODO(anyone): Choose an appropriate number of clusters.
**/

bool JoinSortMerge::supports(const JoinConfiguration config) {
  return (config.predicate_condition != PredicateCondition::NotEquals || config.join_mode == JoinMode::Inner) &&
         config.left_data_type == config.right_data_type && config.join_mode != JoinMode::Semi &&
         config.join_mode != JoinMode::AntiNullAsTrue && config.join_mode != JoinMode::AntiNullAsFalse;
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
    : AbstractJoinOperator(OperatorType::JoinSortMerge, left, right, mode, primary_predicate, secondary_predicates) {}

std::shared_ptr<AbstractOperator> JoinSortMerge::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinSortMerge>(copied_input_left, copied_input_right, _mode, _primary_predicate,
                                         _secondary_predicates);
}

void JoinSortMerge::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinSortMerge::_on_execute() {
  Assert(supports({_mode, _primary_predicate.predicate_condition,
                   input_table_left()->column_data_type(_primary_predicate.column_ids.first),
                   input_table_right()->column_data_type(_primary_predicate.column_ids.second),
                   !_secondary_predicates.empty(), input_table_left()->type(), input_table_right()->type()}),
         "JoinSortMerge doesn't support these parameters");

  // Check column types
  const auto& left_column_type = input_table_left()->column_data_type(_primary_predicate.column_ids.first);
  DebugAssert(left_column_type == input_table_right()->column_data_type(_primary_predicate.column_ids.second),
              "Left and right column types do not match. The sort merge join requires matching column types");

  // Create implementation to compute the join result
  _impl = make_unique_by_data_type<AbstractJoinOperatorImpl, JoinSortMergeImpl>(
      left_column_type, *this, _primary_predicate.column_ids.first, _primary_predicate.column_ids.second,
      _primary_predicate.predicate_condition, _mode, _secondary_predicates);

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
class JoinSortMerge::JoinSortMergeImpl : public AbstractJoinOperatorImpl {
 public:
  JoinSortMergeImpl<T>(JoinSortMerge& sort_merge_join, ColumnID left_column_id, ColumnID right_column_id,
                       const PredicateCondition op, JoinMode mode,
                       const std::vector<OperatorJoinPredicate>& secondary_join_predicates)
      : _sort_merge_join{sort_merge_join},
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

  // Contains the materialized sorted input tables
  std::unique_ptr<MaterializedSegmentList<T>> _sorted_left_table;
  std::unique_ptr<MaterializedSegmentList<T>> _sorted_right_table;

  // Contains the null value row ids if a join column is an outer join column
  std::unique_ptr<PosList> _null_rows_left;
  std::unique_ptr<PosList> _null_rows_right;

  const ColumnID _primary_left_column_id;
  const ColumnID _primary_right_column_id;

  const PredicateCondition _primary_predicate_condition;
  const JoinMode _mode;

  const std::vector<OperatorJoinPredicate>& _secondary_join_predicates;
  // these are used for outer joins where the primary predicate is not Equals.
  std::map<RowID, bool> _left_row_ids_emitted{};
  std::map<RowID, bool> _right_row_ids_emitted{};

  // the cluster count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
  size_t _cluster_count;

  // Contains the output row ids for each cluster
  std::vector<std::shared_ptr<PosList>> _output_pos_lists_left;
  std::vector<std::shared_ptr<PosList>> _output_pos_lists_right;

  /**
   * The TablePosition is a utility struct that is used to define a specific position in a sorted input table.
  **/
  struct TableRange;
  struct TablePosition {
    TablePosition() = default;
    TablePosition(size_t cluster, size_t index) : cluster{cluster}, index{index} {}

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
      for (size_t cluster = start.cluster; cluster <= end.cluster; ++cluster) {
        size_t start_index = (cluster == start.cluster) ? start.index : 0;
        size_t end_index = (cluster == end.cluster) ? end.index : (*table)[cluster]->size();
        for (size_t index = start_index; index < end_index; ++index) {
          action((*(*table)[cluster])[index].row_id);
        }
      }
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
    size_t chunk_count_left = _sort_merge_join.input_table_left()->chunk_count();
    size_t chunk_count_right = _sort_merge_join.input_table_right()->chunk_count();
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

  /**
  * Performs the join for two runs of a specified cluster.
  * A run is a series of rows in a cluster with the same value.
  **/
  void _join_runs(TableRange left_run, TableRange right_run, CompareResult compare_result,
                  std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator) {
    size_t cluster_number = left_run.start.cluster;
    switch (_primary_predicate_condition) {
      case PredicateCondition::Equals:
        if (compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_number, left_run, right_run, multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Less) {
          if (_mode == JoinMode::Left || _mode == JoinMode::FullOuter) {
            _emit_right_primary_null_combinations(cluster_number, left_run);
          }
        } else if (compare_result == CompareResult::Greater) {
          if (_mode == JoinMode::Right || _mode == JoinMode::FullOuter) {
            _emit_left_primary_null_combinations(cluster_number, right_run);
          }
        }
        break;
      case PredicateCondition::NotEquals:
        if (compare_result == CompareResult::Greater) {
          _emit_qualified_combinations(cluster_number, left_run.start.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_number, left_run.end.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
          _emit_qualified_combinations(cluster_number, left_run, right_run.end.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Less) {
          _emit_qualified_combinations(cluster_number, left_run, right_run.start.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::GreaterThan:
        if (compare_result == CompareResult::Greater) {
          _emit_qualified_combinations(cluster_number, left_run.start.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_number, left_run.end.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::GreaterThanEquals:
        if (compare_result == CompareResult::Greater || compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_number, left_run.start.to(_end_of_left_table), right_run,
                                       multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::LessThan:
        if (compare_result == CompareResult::Less) {
          _emit_qualified_combinations(cluster_number, left_run, right_run.start.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        } else if (compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_number, left_run, right_run.end.to(_end_of_right_table),
                                       multi_predicate_join_evaluator);
        }
        break;
      case PredicateCondition::LessThanEquals:
        if (compare_result == CompareResult::Less || compare_result == CompareResult::Equal) {
          _emit_qualified_combinations(cluster_number, left_run, right_run.start.to(_end_of_right_table),
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
    _output_pos_lists_left[output_cluster]->push_back(left_row_id);
    _output_pos_lists_right[output_cluster]->push_back(right_row_id);
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
        _left_row_ids_emitted.emplace(left_row_id, false);
        right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            _left_row_ids_emitted[left_row_id] = true;
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
        _right_row_ids_emitted.emplace(right_row_id, false);
        left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            _right_row_ids_emitted[right_row_id] = true;
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
        // right_row_ids_with_match has no key `right_row_id`
        if (matched_right_row_ids.count(right_row_id) == 0) {
          _emit_combination(output_cluster, NULL_ROW_ID, right_row_id);
        }
      });
    } else {
      left_range.for_every_row_id(_sorted_left_table, [&](RowID left_row_id) {
        // If left_row_id not yet in _left_row_ids_emitted, this initializes it to false
        _left_row_ids_emitted[left_row_id];
        right_range.for_every_row_id(_sorted_right_table, [&](RowID right_row_id) {
          // If right_row_id not yet in _right_row_id_has_match, this initializes it to false
          _right_row_ids_emitted[right_row_id];
          if (multi_predicate_join_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
            _emit_combination(output_cluster, left_row_id, right_row_id);
            _left_row_ids_emitted[left_row_id] = true;
            _right_row_ids_emitted[right_row_id] = true;
          }
        });
      });
    }
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

  /**
  * Performs the join on a single cluster. Runs of entries with the same value are identified and handled together.
  * This constitutes the merge phase of the join. The output combinations of row ids are determined by _join_runs.
  **/
  void _join_cluster(size_t cluster_number,
                     std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator) {
    auto& left_cluster = (*_sorted_left_table)[cluster_number];
    auto& right_cluster = (*_sorted_right_table)[cluster_number];

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

      TableRange left_run(cluster_number, left_run_start, left_run_end);
      TableRange right_run(cluster_number, right_run_start, right_run_end);
      _join_runs(left_run, right_run, compare_result, multi_predicate_join_evaluator);

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
    auto right_rest = TableRange(cluster_number, right_run_start, right_size);
    auto left_rest = TableRange(cluster_number, left_run_start, left_size);
    if (left_run_start < left_size) {
      _join_runs(left_rest, right_rest, CompareResult::Less, multi_predicate_join_evaluator);
    } else if (right_run_start < right_size) {
      _join_runs(left_rest, right_rest, CompareResult::Greater, multi_predicate_join_evaluator);
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
    DebugAssert(_primary_predicate_condition != PredicateCondition::Equals,
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

    if (_sort_merge_join.input_table_left()->row_count() == 0) {
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
        _right_row_ids_emitted[right_row_id] = true;
      });
    }

    // Add null-combinations for right row ids where the primary predicate was satisfied but the
    // secondary predicates were not.
    for (const auto& right_row_id : _right_row_ids_emitted) {
      if (!right_row_id.second) {
        _emit_combination(0, NULL_ROW_ID, right_row_id.first);
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

    if (_sort_merge_join.input_table_right()->row_count() == 0) {
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
        _left_row_ids_emitted[left_row_id] = true;
      });
    }

    // Add null-combinations for left row ids where the primary predicate was satisfied but the
    // secondary predicates were not.
    for (const auto& left_row_id : _left_row_ids_emitted) {
      if (!left_row_id.second) {
        _emit_combination(0, left_row_id.first, NULL_ROW_ID);
      }
    }
  }

  /**
  * Performs the join on all clusters in parallel.
  **/
  void _perform_join() {
    std::vector<std::shared_ptr<AbstractTask>> jobs;

    // Parallel join for each cluster
    for (size_t cluster_number = 0; cluster_number < _cluster_count; ++cluster_number) {
      // Create output position lists
      _output_pos_lists_left[cluster_number] = std::make_shared<PosList>();
      _output_pos_lists_right[cluster_number] = std::make_shared<PosList>();

      // Avoid empty jobs for inner equi joins
      if (_mode == JoinMode::Inner && _primary_predicate_condition == PredicateCondition::Equals) {
        if ((*_sorted_left_table)[cluster_number]->empty() || (*_sorted_right_table)[cluster_number]->empty()) {
          continue;
        }
      }

      jobs.push_back(std::make_shared<JobTask>([this, cluster_number] {
        // Accessors are not thread-safe, so we create one evaluator per job
        std::optional<MultiPredicateJoinEvaluator> multi_predicate_join_evaluator;
        if (!_secondary_join_predicates.empty()) {
          multi_predicate_join_evaluator.emplace(*_sort_merge_join._input_left->get_output(),
                                                 *_sort_merge_join.input_right()->get_output(), _mode,
                                                 _secondary_join_predicates);
        }

        this->_join_cluster(cluster_number, multi_predicate_join_evaluator);
      }));
      jobs.back()->schedule();
    }

    Hyrise::get().scheduler()->wait_for_tasks(jobs);

    // The outer joins for the non-equi cases
    // Note: Equi outer joins can be integrated into the main algorithm, while these can not.
    if ((_mode == JoinMode::Left || _mode == JoinMode::FullOuter) &&
        _primary_predicate_condition != PredicateCondition::Equals) {
      _left_outer_non_equi_join();
    }
    if ((_mode == JoinMode::Right || _mode == JoinMode::FullOuter) &&
        _primary_predicate_condition != PredicateCondition::Equals) {
      _right_outer_non_equi_join();
    }
  }

  /**
  * Concatenates a vector of pos lists into a single new pos list.
  **/
  std::shared_ptr<PosList> _concatenate_pos_lists(std::vector<std::shared_ptr<PosList>>& pos_lists) {
    auto output = std::make_shared<PosList>();

    // Determine the required space
    size_t total_size = 0;
    for (auto& pos_list : pos_lists) {
      total_size += pos_list->size();
    }

    // Move the entries over the output pos list
    output->reserve(total_size);
    for (auto& pos_list : pos_lists) {
      output->insert(output->end(), pos_list->begin(), pos_list->end());
    }

    return output;
  }

  /**
  * Adds the segments from an input table to the output table
  **/
  void _add_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                            const std::shared_ptr<const PosList>& pos_list) {
    auto column_count = input_table->column_count();
    for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
      // Add the segment data (in the form of a poslist)
      if (input_table->type() == TableType::References) {
        // Create a pos_list referencing the original segment instead of the reference segment
        auto new_pos_list = _dereference_pos_list(input_table, column_id, pos_list);

        if (input_table->chunk_count() > 0) {
          const auto base_segment = input_table->get_chunk(ChunkID{0})->get_segment(column_id);
          const auto ref_segment = std::dynamic_pointer_cast<const ReferenceSegment>(base_segment);

          auto new_ref_segment = std::make_shared<ReferenceSegment>(ref_segment->referenced_table(),
                                                                    ref_segment->referenced_column_id(), new_pos_list);
          output_segments.push_back(new_ref_segment);
        } else {
          // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencing to.
          // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceSegment that
          // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceSegment ref
          // it.
          const auto dummy_table = Table::create_dummy_table(input_table->column_definitions());
          output_segments.push_back(std::make_shared<ReferenceSegment>(dummy_table, column_id, pos_list));
        }
      } else {
        auto new_ref_segment = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);
        output_segments.push_back(new_ref_segment);
      }
    }
  }

  /**
  * Turns a pos list that is pointing to reference segment entries into a pos list pointing to the original table.
  * This is done because there should not be any reference segments referencing reference segments.
  **/
  std::shared_ptr<PosList> _dereference_pos_list(const std::shared_ptr<const Table>& input_table, ColumnID column_id,
                                                 const std::shared_ptr<const PosList>& pos_list) {
    // Get all the input pos lists so that we only have to pointer cast the segments once
    auto input_pos_lists = std::vector<std::shared_ptr<const PosList>>();
    for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
      auto base_segment = input_table->get_chunk(chunk_id)->get_segment(column_id);
      auto reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(base_segment);
      input_pos_lists.push_back(reference_segment->pos_list());
    }

    // Get the row ids that are referenced
    auto new_pos_list = std::make_shared<PosList>();
    for (const auto& row : *pos_list) {
      if (row.is_null()) {
        new_pos_list->push_back(NULL_ROW_ID);
      } else {
        new_pos_list->push_back((*input_pos_lists[row.chunk_id])[row.chunk_offset]);
      }
    }

    return new_pos_list;
  }

 public:
  /**
  * Executes the SortMergeJoin operator.
  **/
  std::shared_ptr<const Table> _on_execute() override {
    bool include_null_left = (_mode == JoinMode::Left || _mode == JoinMode::FullOuter);
    bool include_null_right = (_mode == JoinMode::Right || _mode == JoinMode::FullOuter);
    auto radix_clusterer = RadixClusterSort<T>(
        _sort_merge_join.input_table_left(), _sort_merge_join.input_table_right(),
        _sort_merge_join._primary_predicate.column_ids, _primary_predicate_condition == PredicateCondition::Equals,
        include_null_left, include_null_right, _cluster_count);
    // Sort and cluster the input tables
    auto sort_output = radix_clusterer.execute();
    _sorted_left_table = std::move(sort_output.clusters_left);
    _sorted_right_table = std::move(sort_output.clusters_right);
    _null_rows_left = std::move(sort_output.null_rows_left);
    _null_rows_right = std::move(sort_output.null_rows_right);
    _end_of_left_table = _end_of_table(_sorted_left_table);
    _end_of_right_table = _end_of_table(_sorted_right_table);

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

    // Add the segments from both input tables to the output
    Segments output_segments;
    _add_output_segments(output_segments, _sort_merge_join.input_table_left(), output_left);
    _add_output_segments(output_segments, _sort_merge_join.input_table_right(), output_right);

    // Build the output_table with one Chunk
    return _sort_merge_join._build_output_table({std::make_shared<Chunk>(output_segments)});
  }
};

}  // namespace opossum
