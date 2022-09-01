#include "join_sort_merge.hpp"

#include <algorithm>
#include <memory>
#include <optional>
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

namespace hyrise {

// TODO(anyone) >> todos and nice-to-haves for the sort-merge join:
//    - outer not-equal join (outer !=)
//    - Bloom filters

bool JoinSortMerge::supports(const JoinConfiguration config) {
  return (config.predicate_condition != PredicateCondition::NotEquals || config.join_mode == JoinMode::Inner) &&
         config.left_data_type == config.right_data_type && config.join_mode != JoinMode::Semi &&
         config.join_mode != JoinMode::AntiNullAsTrue && config.join_mode != JoinMode::AntiNullAsFalse;
}

// The sort merge join performs a join on two input tables on specific join columns. For usage notes, see the
// join_sort_merge.hpp. This is how the join works:
// -> The input tables are materialized and clustered into a specified number of clusters.
//    /utils/radix_cluster_sort.hpp for more info on the clustering phase.
// -> The join is performed per cluster. For the joining phase, runs of entries with the same value are identified
//    and handled at once. If a join-match is identified, the corresponding row_ids are noted for the output.
// -> Using the join result, the output table is built using pos lists referencing the original tables.
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

void JoinSortMerge::_on_cleanup() {
  _impl.reset();
}

const std::string& JoinSortMerge::name() const {
  static const auto name = std::string{"JoinSortMerge"};
  return name;
}

template <typename T>
class JoinSortMerge::JoinSortMergeImpl : public AbstractReadOnlyOperatorImpl {
 public:
  JoinSortMergeImpl(JoinSortMerge& sort_merge_join, const std::shared_ptr<const Table>& left_input_table,
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
        _secondary_join_predicates{secondary_join_predicates},
        _cluster_count{_determine_number_of_clusters()} {
    _output_pos_lists_left.resize(_cluster_count);
    _output_pos_lists_right.resize(_cluster_count);
  }

 protected:
  JoinSortMerge& _sort_merge_join;
  const std::shared_ptr<const Table> _left_input_table, _right_input_table;

  OperatorPerformanceData<JoinSortMerge::OperatorSteps>& _performance;

  // Contains the materialized sorted input tables
  MaterializedSegmentList<T> _sorted_left_table;
  MaterializedSegmentList<T> _sorted_right_table;

  // Contains the null value row ids if a join column is an outer join column
  RowIDPosList _null_rows_left;
  RowIDPosList _null_rows_right;

  const ColumnID _primary_left_column_id;
  const ColumnID _primary_right_column_id;

  const PredicateCondition _primary_predicate_condition;
  const JoinMode _mode;

  const std::vector<OperatorJoinPredicate>& _secondary_join_predicates;

  // The cluster count must be a power of two, i.e. 1, 2, 4, 8, 16, ...
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

  // The TablePosition is a utility struct that is used to define a specific position in a sorted input table.
  struct TableRange;

  struct TablePosition {
    TablePosition() = default;

    TablePosition(size_t init_cluster, size_t init_index) : cluster{init_cluster}, index{init_index} {}

    size_t cluster;
    size_t index;

    TableRange to(TablePosition position) {
      return TableRange(*this, position);
    }
  };

  TablePosition _end_of_left_table;
  TablePosition _end_of_right_table;

  // The TableRange is a utility struct that is used to define ranges of rows in a sorted input table spanning from
  // a start position to an end position.
  struct TableRange {
    TableRange(TablePosition start_position, TablePosition end_position) : start(start_position), end(end_position) {}

    TableRange(size_t cluster, size_t start_index, size_t end_index)
        : start{TablePosition(cluster, start_index)}, end{TablePosition(cluster, end_index)} {}

    TablePosition start;
    TablePosition end;

    // Executes the given action for every row id of the table in this range.
    template <typename F>
    void for_every_row_id(const MaterializedSegmentList<T>& table, const F& action) {
// False positive with gcc and tsan (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=92194)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
      for (auto cluster = start.cluster; cluster <= end.cluster; ++cluster) {
        const auto start_index = (cluster == start.cluster) ? start.index : 0;
        const auto end_index = (cluster == end.cluster) ? end.index : table[cluster].size();
        for (auto index = start_index; index < end_index; ++index) {
          action(table[cluster][index].row_id);
        }
      }
#pragma GCC diagnostic pop
    }
  };

  // Determines the number of clusters to be used for the join. The number of clusters must be a power of two.
  size_t _determine_number_of_clusters() {
    // We try to have a partition size of roughly 256 KB to avoid out-of-L2 cache sorts. This value has been determined
    // by an array of benchmarks. Ideally, it would incorporate hardware knowledge such as the L2 cache size.
    const auto max_sort_items_count = 256'000 / sizeof(T);
    const size_t cluster_count_left = _sort_merge_join.left_input_table()->row_count() / max_sort_items_count;
    const size_t cluster_count_right = _sort_merge_join.right_input_table()->row_count() / max_sort_items_count;

    // Return the next larger power of two for the larger of the two cluster counts.
    return static_cast<size_t>(
        std::pow(2, std::floor(std::log2(std::max({size_t{1}, cluster_count_left, cluster_count_right})))));
  }

  // Gets the table position corresponding to the end of the table, i.e. the last entry of the last cluster.
  static TablePosition _end_of_table(const MaterializedSegmentList<T>& table) {
    DebugAssert(!table.empty(), "table has no chunks");
    auto last_cluster = table.size() - 1;
    return TablePosition(last_cluster, table[last_cluster].size());
  }

  // Represents the result of a value comparison.
  enum class CompareResult { Less, Greater, Equal };

  // Performs the join for two runs of a specified cluster.
  // A run is a series of rows in a cluster with the same value.
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

  // Emits a combination of a left row id and a right row id to the join output.
  void _emit_combination(size_t output_cluster, RowID left_row_id, RowID right_row_id) {
    _output_pos_lists_left[output_cluster].push_back(left_row_id);
    _output_pos_lists_right[output_cluster].push_back(right_row_id);
  }

  // Emits all the combinations of row ids from the left table range and the right table range to the join output
  // where also the secondary predicates are satisfied.
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

  // Only for multi predicated inner joins.
  // Emits all the combinations of row ids from the left table range and the right table range to the join output
  // where the secondary predicates are satisfied.
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

  // Only for multi predicated left outer joins.
  // Emits all the combinations of row ids from the left table range and the right table range to the join output
  // where the secondary predicates are satisfied.
  // For a left row id without a match, the combination [left row id|NULL row id] is emitted.
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

  // Only for multi predicated right outer joins.
  // Emits all the combinations of row ids from the left table range and the right table range to the join output
  // where the secondary predicates are satisfied.
  // For a right row id without a match, the combination [NULL row id|right row id] is emitted.
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

  // Only for multi-predicate full outer joins.
  // Emits all the combinations of row ids from the left table range and the right table range to the join output
  // where the secondary predicates are satisfied.
  // For a left row id without a match, the combination [right row id|NULL row id] is emitted.
  // For a right row id without a match, the combination [NULL row id|right row id] is emitted.
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
        if (!matched_right_row_ids.contains(right_row_id)) {
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

  // Emits all combinations of row ids from the left table range and a NULL value on the right side
  // (regarding the primary predicate) to the join output.
  void _emit_right_primary_null_combinations(size_t output_cluster, TableRange left_range) {
    left_range.for_every_row_id(
        _sorted_left_table, [&](RowID left_row_id) { _emit_combination(output_cluster, left_row_id, NULL_ROW_ID); });
  }

  // Emits all combinations of row ids from the right table range and a NULL value on the left side
  // (regarding the primary predicate) to the join output.
  void _emit_left_primary_null_combinations(size_t output_cluster, TableRange right_range) {
    right_range.for_every_row_id(
        _sorted_right_table, [&](RowID right_row_id) { _emit_combination(output_cluster, NULL_ROW_ID, right_row_id); });
  }

  // Determines the length of the run starting at start_index in the values vector. A run is a series of the same
  // value. Even though the input vector is sorted, we first start by linearly scanning for the end of the run. The
  // reason is that we know with a high probability that the item we are looking for is at the beginning of the input
  // vector (the run value is the first in the sequence and the sequence is sorted). If we do not find the run's end
  // within the linearly scanned part, we use a binary search. An unsuccessful linear search should come with
  // neglectable costs as we will iterate over the values anyways.
  size_t _run_length(size_t start_index, const MaterializedSegment<T>& values) {
    if (start_index >= values.size()) {
      return 0;
    }

    const auto begin = values.begin() + start_index;
    const auto run_value = begin->value;

    constexpr auto LINEAR_SEARCH_ITEMS = size_t{128};
    auto end = begin + LINEAR_SEARCH_ITEMS;
    if (start_index + LINEAR_SEARCH_ITEMS >= values.size()) {
      // Set end of linear search to end of input vector if we would overshoot otherwise.
      end = values.end();
    }

    const auto linear_search_result =
        std::find_if(begin, end, [&](const auto& mat_value) { return mat_value.value > run_value; });
    if (linear_search_result != end) {
      // Match found within the linearly scanned part.
      return std::distance(begin, linear_search_result);
    }

    if (linear_search_result == values.end()) {
      // We did not find a larger value in the linearly scanned part and it spanned until the end of the input vector.
      // That means all values up to the end are part of the run.
      return std::distance(begin, end);
    }

    // Binary search in case the run did not end within the linearly scanned part.
    const auto binary_search_result = std::upper_bound(
        end, values.end(), *end, [](const auto& lhs, const auto& rhs) { return lhs.value < rhs.value; });
    return std::distance(begin, binary_search_result);
  }

  // Compares two values and creates a comparison result.
  CompareResult _compare(const T& left, const T& right) {
    if (left < right) {
      return CompareResult::Less;
    }

    if (left == right) {
      return CompareResult::Equal;
    }

    return CompareResult::Greater;
  }

  // Performs the join on a single cluster. Runs of entries with the same value are identified and handled together.
  // This constitutes the merge phase of the join. The output combinations of row ids are determined by _join_runs.
  void _join_cluster(const size_t cluster_id,
                     std::optional<MultiPredicateJoinEvaluator>& multi_predicate_join_evaluator) {
    const auto& left_cluster = _sorted_left_table[cluster_id];
    const auto& right_cluster = _sorted_right_table[cluster_id];

    auto left_run_start = size_t{0};
    auto right_run_start = size_t{0};

    auto left_run_end = left_run_start + _run_length(left_run_start, left_cluster);
    auto right_run_end = right_run_start + _run_length(right_run_start, right_cluster);

    const auto left_size = left_cluster.size();
    const auto right_size = right_cluster.size();

    while (left_run_start < left_size && right_run_start < right_size) {
      const auto& left_value = left_cluster[left_run_start].value;
      const auto& right_value = right_cluster[right_run_start].value;

      const auto compare_result = _compare(left_value, right_value);

      TableRange left_run(cluster_id, left_run_start, left_run_end);
      TableRange right_run(cluster_id, right_run_start, right_run_end);
      _join_runs(left_run, right_run, compare_result, multi_predicate_join_evaluator, cluster_id);

      // Advance to the next run on the smaller side or both if equal
      switch (compare_result) {
        case CompareResult::Equal:
          // Advance both runs
          left_run_start = left_run_end;
          right_run_start = right_run_end;
          left_run_end = left_run_start + _run_length(left_run_start, left_cluster);
          right_run_end = right_run_start + _run_length(right_run_start, right_cluster);
          break;
        case CompareResult::Less:
          // Advance the left run
          left_run_start = left_run_end;
          left_run_end = left_run_start + _run_length(left_run_start, left_cluster);
          break;
        case CompareResult::Greater:
          // Advance the right run
          right_run_start = right_run_end;
          right_run_end = right_run_start + _run_length(right_run_start, right_cluster);
          break;
        default:
          throw std::logic_error("Unknown CompareResult");
      }
    }

    // Join the rest of the unfinished side, which is relevant for outer joins and non-equi joins
    const auto right_rest = TableRange(cluster_id, right_run_start, right_size);
    const auto left_rest = TableRange(cluster_id, left_run_start, left_size);
    if (left_run_start < left_size) {
      _join_runs(left_rest, right_rest, CompareResult::Less, multi_predicate_join_evaluator, cluster_id);
    } else if (right_run_start < right_size) {
      _join_runs(left_rest, right_rest, CompareResult::Greater, multi_predicate_join_evaluator, cluster_id);
    }
  }

  // Determines the smallest value in a sorted materialized table.
  const T& _table_min_value(const MaterializedSegmentList<T>& sorted_table) {
    DebugAssert(_primary_predicate_condition != PredicateCondition::Equals,
                "Complete table order is required for _table_min_value() which is only available in the non-equi case");
    DebugAssert(!sorted_table.empty(), "Sorted table has no partitions");

    for (const auto& partition : sorted_table) {
      if (!partition.empty()) {
        return partition[0].value;
      }
    }

    Fail("Every partition is empty");
  }

  // Determines the largest value in a sorted materialized table.
  const T& _table_max_value(const MaterializedSegmentList<T>& sorted_table) {
    DebugAssert(_primary_predicate_condition != PredicateCondition::Equals,
                "The table needs to be sorted for _table_max_value() which is only the case for non-equi joins");
    DebugAssert(!sorted_table.empty(), "Sorted table is empty");

    for (auto partition_iter = sorted_table.rbegin(); partition_iter != sorted_table.rend(); ++partition_iter) {
      if (!partition_iter->empty()) {
        return partition_iter->back().value;
      }
    }

    Fail("Every partition is empty");
  }

  // Looks for the first value in a sorted materialized table that fulfills the specified condition.
  // Returns the TablePosition of this element and whether a satisfying element has been found.
  template <typename Function>
  std::optional<TablePosition> _first_value_that_satisfies(const MaterializedSegmentList<T>& sorted_table,
                                                           const Function& condition) {
    const auto sorted_table_size = sorted_table.size();
    for (auto partition_id = size_t{0}; partition_id < sorted_table_size; ++partition_id) {
      const auto& partition = sorted_table[partition_id];
      if (!partition.empty() && condition(partition.back().value)) {
        const auto partition_size = partition.size();
        for (auto index = size_t{0}; index < partition_size; ++index) {
          if (condition(partition[index].value)) {
            return TablePosition(partition_id, index);
          }
        }
      }
    }

    return {};
  }

  // Looks for the first value in a sorted materialized table that fulfills the specified condition, but searches
  // the table in reverse order. Returns the TablePosition of this element, and a satisfying element has been found.
  template <typename Function>
  std::optional<TablePosition> _first_value_that_satisfies_reverse(const MaterializedSegmentList<T>& sorted_table,
                                                                   const Function& condition) {
    const auto sorted_table_size = sorted_table.size();
    for (auto partition_id = static_cast<int64_t>(sorted_table_size - 1); partition_id >= 0; --partition_id) {
      const auto& partition = sorted_table[partition_id];
      if (!partition.empty() && condition(partition[0].value)) {
        const auto partition_size = partition.size();
        for (auto index = static_cast<int64_t>(partition_size - 1); index >= 0; --index) {
          if (condition(partition[index].value)) {
            return TablePosition(partition_id, index + 1);
          }
        }
      }
    }

    return {};
  }

  // Adds the rows without matches for right outer joins for non-equi operators (<, <=, >, >=).
  // This method adds those rows from the right table to the output that do not find a join partner.
  // The outer join for the equality operator is handled in _join_runs instead.
  void _right_outer_non_equi_join() {
    auto end_of_right_table = _end_of_table(_sorted_right_table);

    if (_sort_merge_join.left_input_table()->row_count() == 0) {
      _emit_left_primary_null_combinations(0, TablePosition(0, 0).to(end_of_right_table));
      return;
    }

    const auto& left_min_value = _table_min_value(_sorted_left_table);
    const auto& left_max_value = _table_max_value(_sorted_left_table);

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
      for (const auto& cluster : _sorted_right_table) {
        for (const auto& row : cluster) {
          if (!_right_row_ids_emitted.contains(row.row_id)) {
            _emit_combination(0, NULL_ROW_ID, row.row_id);
          }
        }
      }
    }
  }

  // Adds the rows without matches for left outer joins for non-equi operators (<, <=, >, >=).
  // This method adds those rows from the left table to the output that do not find a join partner.
  // The outer join for the equality operator is handled in _join_runs instead.
  void _left_outer_non_equi_join() {
    auto end_of_left_table = _end_of_table(_sorted_left_table);

    if (_sort_merge_join.right_input_table()->row_count() == 0) {
      _emit_right_primary_null_combinations(0, TablePosition(0, 0).to(end_of_left_table));
      return;
    }

    const auto& right_min_value = _table_min_value(_sorted_right_table);
    const auto& right_max_value = _table_max_value(_sorted_right_table);

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
      for (const auto& cluster : _sorted_left_table) {
        for (const auto& row : cluster) {
          if (!_left_row_ids_emitted.contains(row.row_id)) {
            _emit_combination(0, row.row_id, NULL_ROW_ID);
          }
        }
      }
    }
  }

  // Performs the join on all clusters in parallel.
  void _perform_join() {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    _left_row_ids_emitted_per_chunk.resize(_cluster_count);
    _right_row_ids_emitted_per_chunk.resize(_cluster_count);

    // Parallel join for each cluster
    for (auto cluster_id = size_t{0}; cluster_id < _cluster_count; ++cluster_id) {
      // Create output position lists
      _output_pos_lists_left[cluster_id] = RowIDPosList{};
      _output_pos_lists_right[cluster_id] = RowIDPosList{};

      // Avoid empty jobs for inner equi joins
      if (_mode == JoinMode::Inner && _primary_predicate_condition == PredicateCondition::Equals) {
        if (_sorted_left_table[cluster_id].empty() || _sorted_right_table[cluster_id].empty()) {
          continue;
        }
      }

      _left_row_ids_emitted_per_chunk[cluster_id] = RowHashSet{};
      _right_row_ids_emitted_per_chunk[cluster_id] = RowHashSet{};

      const auto merge_row_count = _sorted_left_table[cluster_id].size() + _sorted_right_table[cluster_id].size();
      const auto join_cluster_task = [this, cluster_id] {
        // Accessors are not thread-safe, so we create one evaluator per job
        std::optional<MultiPredicateJoinEvaluator> multi_predicate_join_evaluator;
        if (!_secondary_join_predicates.empty()) {
          multi_predicate_join_evaluator.emplace(*_sort_merge_join._left_input->get_output(),
                                                 *_sort_merge_join.right_input()->get_output(), _mode,
                                                 _secondary_join_predicates);
        }

        this->_join_cluster(cluster_id, multi_predicate_join_evaluator);
      };

      if (merge_row_count > JOB_SPAWN_THRESHOLD) {
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

 public:
  std::shared_ptr<const Table> _on_execute() override {
    const auto include_null_left = (_mode == JoinMode::Left || _mode == JoinMode::FullOuter);
    const auto include_null_right = (_mode == JoinMode::Right || _mode == JoinMode::FullOuter);
    auto radix_clusterer = RadixClusterSort<T>(
        _sort_merge_join.left_input_table(), _sort_merge_join.right_input_table(),
        _sort_merge_join._primary_predicate.column_ids, _primary_predicate_condition == PredicateCondition::Equals,
        include_null_left, include_null_right, _cluster_count, _performance);
    // Sort and cluster the input tables
    auto sort_output = radix_clusterer.execute();
    _sorted_left_table = std::move(sort_output.clusters_left);
    _sorted_right_table = std::move(sort_output.clusters_right);
    _null_rows_left = std::move(sort_output.null_rows_left);
    _null_rows_right = std::move(sort_output.null_rows_right);
    _end_of_left_table = _end_of_table(_sorted_left_table);
    _end_of_right_table = _end_of_table(_sorted_right_table);

    Timer timer;

    _perform_join();

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
    auto output_chunks =
        write_output_chunks(_output_pos_lists_left, _output_pos_lists_right, _left_input_table, _right_input_table,
                            create_left_side_pos_lists_by_segment, create_right_side_pos_lists_by_segment,
                            OutputColumnOrder::LeftFirstRightSecond, ALLOW_PARTITION_MERGE);

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
        _sort_merge_join._primary_predicate.predicate_condition == PredicateCondition::Equals) {
      // Table clustering is not defined for columns storing NULL values. Additionally, clustering is not given for
      // non-equal predicates.
      result_table->set_value_clustered_by({left_join_column, right_join_column});
    }
    return result_table;
  }
};

}  // namespace hyrise
