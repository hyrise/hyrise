#include "join_hash.hpp"

#include <cmath>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bytell_hash_map.hpp"
#include "hyrise.hpp"
#include "join_hash/join_hash_steps.hpp"
#include "join_hash/join_hash_traits.hpp"
#include "join_helper/join_output_writing.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace hyrise {

bool JoinHash::supports(const JoinConfiguration config) {
  // JoinHash supports only equi joins and every join mode, except FullOuter.
  // Secondary predicates in AntiNullAsTrue are not supported, because implementing them is cumbersome and we couldn't
  // so far determine a case/query where we'd need them.
  return config.predicate_condition == PredicateCondition::Equals && config.join_mode != JoinMode::FullOuter &&
         (config.join_mode != JoinMode::AntiNullAsTrue || !config.secondary_predicates);
}

JoinHash::JoinHash(const std::shared_ptr<const AbstractOperator>& left,
                   const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                   const OperatorJoinPredicate& primary_predicate,
                   const std::vector<OperatorJoinPredicate>& secondary_predicates,
                   const std::optional<size_t>& radix_bits)
    : AbstractJoinOperator(OperatorType::JoinHash, left, right, mode, primary_predicate, secondary_predicates,
                           std::make_unique<PerformanceData>()),
      _radix_bits(radix_bits) {}

const std::string& JoinHash::name() const {
  static const auto name = std::string{"JoinHash"};
  return name;
}

std::shared_ptr<AbstractOperator> JoinHash::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<JoinHash>(copied_left_input, copied_right_input, _mode, _primary_predicate,
                                    _secondary_predicates, _radix_bits);
}

void JoinHash::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

size_t JoinHash::calculate_radix_bits(const size_t build_side_size, const size_t probe_side_size, const JoinMode mode) {
  /*
    The number of radix bits is used to determine the number of build partitions. The idea is to size the partitions in
    a way that keeps the whole hash map cache resident. We aim for the largest unshared cache (for most Intel systems
    that's the L2 cache, for Apple's M1 the L1 cache). This calculation should include hardware knowledge, once
    available in Hyrise. As of now, we assume a cache size of 1024 KB, of which we use 75 %.

    We estimate the size the following way:
      - we assume each key appears once (that is an overestimation space-wise, but we
        aim rather for a hash map that is slightly smaller than the cache than slightly larger)
      - each entry in the hash map is a pair of the actual hash key and the SmallPosList storing uint32_t offsets (see
        hash_join_steps.hpp)
  */
  if (build_side_size > probe_side_size) {
    /*
      Hash joins perform best when the build side is small. For inner joins, we can simply select the smaller input
      table as the build side. For other joins, such as semi or outer joins, the build side is fixed. In this case,
      other join operators might be more efficient. We emit performance warning in this case. In the future, the
      optimizer could identify these cases of potentially inefficient hash joins and switch to other join algorithms.
    */
    PerformanceWarning("Build side larger than probe side in hash join");
  }

  // We assume a cache of 1024 KB for an Intel Xeon Platinum 8180. For local deployments or other CPUs, this size might
  // be different (e.g., an AMD EPYC 7F72 CPU has an L2 cache size of 512 KB and Apple's M1 has 128 KB).
  constexpr auto L2_CACHE_SIZE = 1'024'000;                   // bytes
  constexpr auto L2_CACHE_MAX_USABLE = L2_CACHE_SIZE * 0.75;  // use 75% of the L2 cache size

  // For information about the sizing of the bytell hash map, see the comments:
  // https://probablydance.com/2018/05/28/a-new-fast-hash-table-in-response-to-googles-new-fast-hash-table/
  // Bytell hash map has a maximum fill factor of 0.9375. Since it's hard to estimate the number of distinct values in
  // a radix partition (and thus the size of each hash table), we accomodate a little bit extra space for
  // slightly skewed data distributions and aim for a fill level of 80%.
  const auto complete_hash_map_size =
      // number of items in map
      static_cast<double>(build_side_size) *
      // key + value (and one byte overhead, see link above)
      static_cast<double>(sizeof(uint32_t)) / 0.8;

  const auto cluster_count = std::max(1.0, complete_hash_map_size / L2_CACHE_MAX_USABLE);

  return static_cast<size_t>(std::ceil(std::log2(cluster_count)));
}

std::shared_ptr<const Table> JoinHash::_on_execute() {
  Assert(supports({_mode, _primary_predicate.predicate_condition,
                   left_input_table()->column_data_type(_primary_predicate.column_ids.first),
                   right_input_table()->column_data_type(_primary_predicate.column_ids.second),
                   !_secondary_predicates.empty(), left_input_table()->type(), right_input_table()->type()}),
         "JoinHash doesn't support these parameters");

  std::shared_ptr<const Table> build_input_table;
  std::shared_ptr<const Table> probe_input_table;
  auto build_column_id = ColumnID{};
  auto probe_column_id = ColumnID{};

  /**
   * The hash join works best when the table being probed is larger than the side for which the hash table is built
   * (i.e., the build side). As consequence, when we are to freely determine the build side, we chose the smaller
   * input table. For other cases, we cannot freely decide.
   *
   * Build and probe side are assigned as follows:
   *   JoinMode::Inner        The smaller table becomes the build side, the bigger the probe side
   *   JoinMode::Left/Right   The outer table becomes the probe side, the inner table becomes the build side
   *   JoinMode::FullOuter    Not supported by JoinHash
   *   JoinMode::Semi/Anti*   The left table becomes the probe side, the right table becomes the build side
   */
  const auto build_hash_table_for_right_input =
      _mode == JoinMode::Left || _mode == JoinMode::AntiNullAsTrue || _mode == JoinMode::AntiNullAsFalse ||
      _mode == JoinMode::Semi ||
      (_mode == JoinMode::Inner && _left_input->get_output()->row_count() > _right_input->get_output()->row_count());

  if (build_hash_table_for_right_input) {
    // We don't have to swap the operation itself here, because we only support the commutative Equi Join.
    build_input_table = _right_input->get_output();
    probe_input_table = _left_input->get_output();
    build_column_id = _primary_predicate.column_ids.second;
    probe_column_id = _primary_predicate.column_ids.first;
  } else {
    build_input_table = _left_input->get_output();
    probe_input_table = _right_input->get_output();
    build_column_id = _primary_predicate.column_ids.first;
    probe_column_id = _primary_predicate.column_ids.second;
  }

  // If the input operators are swapped, we also have to swap the column pairs and the predicate conditions
  // of the secondary join predicates.
  auto adjusted_secondary_predicates = _secondary_predicates;

  if (build_hash_table_for_right_input) {
    for (auto& predicate : adjusted_secondary_predicates) {
      predicate.flip();
    }
  }

  auto adjusted_column_ids = std::make_pair(build_column_id, probe_column_id);

  const auto build_column_type = build_input_table->column_data_type(build_column_id);
  const auto probe_column_type = probe_input_table->column_data_type(probe_column_id);

  // Depending on which input table became the build/probe table we have to order the columns of the output table.
  // Semi/Anti* Joins only emit tuples from the probe table, which is given by the right input table in Hyrise.
  // For other join modes, we need to check which side has been chosen as the probe side (see variable
  // `build_hash_table_for_right_input` for more details).
  auto output_column_order = OutputColumnOrder{};
  if (_mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsTrue || _mode == JoinMode::AntiNullAsFalse) {
    output_column_order = OutputColumnOrder::RightOnly;
  } else if (build_hash_table_for_right_input) {
    output_column_order = OutputColumnOrder::RightFirstLeftSecond;
  } else {
    output_column_order = OutputColumnOrder::LeftFirstRightSecond;
  }

  auto& join_hash_performance_data = dynamic_cast<PerformanceData&>(*performance_data);
  resolve_data_type(build_column_type, [&](const auto build_data_type_t) {
    using BuildColumnDataType = typename decltype(build_data_type_t)::type;
    resolve_data_type(probe_column_type, [&](const auto probe_data_type_t) {
      using ProbeColumnDataType = typename decltype(probe_data_type_t)::type;

      constexpr auto BOTH_ARE_STRING =
          std::is_same_v<pmr_string, BuildColumnDataType> && std::is_same_v<pmr_string, ProbeColumnDataType>;
      constexpr auto NEITHER_IS_STRING =
          !std::is_same_v<pmr_string, BuildColumnDataType> && !std::is_same_v<pmr_string, ProbeColumnDataType>;

      if constexpr (BOTH_ARE_STRING || NEITHER_IS_STRING) {
        if (!_radix_bits) {
          _radix_bits = calculate_radix_bits(build_input_table->row_count(), probe_input_table->row_count(), _mode);
        }

        // It needs to be ensured that the build partitions do not get too large, because the used offsets in the
        // hash maps might otherwise overflow. Since radix partitioning aims to avoid large build partitions, this
        // should never happen. Nonetheless, we better assert since the effects of overflows will probably be hard to
        // debug.
        const auto max_partition_size = std::numeric_limits<uint32_t>::max() * 0.5;
        Assert(static_cast<uint32_t>(static_cast<double>(build_input_table->row_count()) / std::pow(2, *_radix_bits)) <
                   max_partition_size,
               "Partition count too small (potential overflows in hash map offsetting).");

        _impl = std::make_unique<JoinHashImpl<BuildColumnDataType, ProbeColumnDataType>>(
            *this, build_input_table, probe_input_table, _mode, adjusted_column_ids,
            _primary_predicate.predicate_condition, output_column_order, *_radix_bits, join_hash_performance_data,
            adjusted_secondary_predicates);
      } else {
        Fail("Cannot join String with non-String column");
      }
    });
  });

  Assert(_radix_bits, "Radix bits are not set.");
  join_hash_performance_data.radix_bits = *_radix_bits;
  join_hash_performance_data.left_input_is_build_side = !build_hash_table_for_right_input;

  return _impl->_on_execute();
}

void JoinHash::_on_cleanup() {
  _impl.reset();
}

template <typename BuildColumnType, typename ProbeColumnType>
class JoinHash::JoinHashImpl : public AbstractReadOnlyOperatorImpl {
 public:
  JoinHashImpl(const JoinHash& join_hash, const std::shared_ptr<const Table>& build_input_table,
               const std::shared_ptr<const Table>& probe_input_table, const JoinMode mode,
               const ColumnIDPair& column_ids, const PredicateCondition predicate_condition,
               const OutputColumnOrder output_column_order, const size_t radix_bits,
               JoinHash::PerformanceData& performance_data, std::vector<OperatorJoinPredicate>& secondary_predicates)
      : _join_hash(join_hash),
        _build_input_table(build_input_table),
        _probe_input_table(probe_input_table),
        _mode(mode),
        _column_ids(column_ids),
        _predicate_condition(predicate_condition),
        _performance_data(performance_data),
        _output_column_order(output_column_order),
        _secondary_predicates(secondary_predicates),
        _radix_bits(radix_bits) {}

 protected:
  const JoinHash& _join_hash;
  const std::shared_ptr<const Table> _build_input_table, _probe_input_table;
  const JoinMode _mode;
  const ColumnIDPair _column_ids;
  const PredicateCondition _predicate_condition;
  JoinHash::PerformanceData& _performance_data;

  OutputColumnOrder _output_column_order;

  const std::vector<OperatorJoinPredicate>& _secondary_predicates;

  std::shared_ptr<Table> _output_table;

  const size_t _radix_bits;

  // Determine correct type for hashing
  using HashedType = typename JoinHashTraits<BuildColumnType, ProbeColumnType>::HashType;

  std::shared_ptr<const Table> _on_execute() override {
    /**
     * Keep/Discard NULLs from build and probe columns as follows
     *
     * JoinMode::Inner              Discard NULLs from both columns
     * JoinMode::Left/Right         Discard NULLs from the build column (the inner table), but keep them on the probe
     *                              column (the outer table)
     * JoinMode::FullOuter          Not supported by JoinHash
     * JoinMode::Semi               Discard NULLs from both columns
     * JoinMode::AntiNullAsFalse    Discard NULLs from the build column (the right table), but keep them on the probe
     *                              column (the left table)
     * JoinMode::AntiNullAsTrue     Keep NULLs from both columns
     */

    const auto keep_nulls_build_column = _mode == JoinMode::AntiNullAsTrue;
    const auto keep_nulls_probe_column = _mode == JoinMode::Left || _mode == JoinMode::Right ||
                                         _mode == JoinMode::AntiNullAsTrue || _mode == JoinMode::AntiNullAsFalse;

    // Containers used to store histograms for (potentially subsequent) radix partitioning step (in cases
    // _radix_bits > 0). Created during materialization step.
    std::vector<std::vector<size_t>> histograms_build_column;
    std::vector<std::vector<size_t>> histograms_probe_column;

    // Output containers of materialization step. Uses the same output type as the radix partitioning step to allow
    // shortcut for _radix_bits == 0 (in this case, we can skip the partitioning altogether).
    RadixContainer<BuildColumnType> materialized_build_column;
    RadixContainer<ProbeColumnType> materialized_probe_column;

    // Containers for potential (skipped when build side small) radix partitioning step
    RadixContainer<BuildColumnType> radix_build_column;
    RadixContainer<ProbeColumnType> radix_probe_column;

    // HashTables for the build column, one for each partition
    std::vector<std::optional<PosHashTable<HashedType>>> hash_tables;

    /**
     * Depiction of the hash join parallelization (radix partitioning can be skipped when radix_bits = 0)
     * ===============================================================================================
     * We have two data paths, one for build side and one for probe input side. All tasks might spawn concurrent tasks
     * tasks themselves. For example, materialize parallelizes over the input chunks and the following steps over the
     * radix clusters.
     *
     * Bloom filters can be used to skip rows that will not find a join partner. They are not shown here.
     *
     *            Build Table                          Probe Table
     *                 |                                    |
     *        materialize_input()                  materialize_input()
     *                 |                                    |
     *      ( partition_by_radix() )            ( partition_by_radix() )
     *                 |                                    |
     *               build()                                |
     *                   \_                               _/
     *                     \_                           _/
     *                       \_                       _/
     *                         \_                   _/
     *                           \                 /
     *                          Probing (actual Join)
     */

    /**
     * 1.1. Materialize the build partition, which is expected to be smaller. Create a Bloom filter.
     */

    auto build_side_bloom_filter = BloomFilter{};
    auto probe_side_bloom_filter = BloomFilter{};

    const auto materialize_build_side = [&](const auto& input_bloom_filter) {
      if (keep_nulls_build_column) {
        materialized_build_column = materialize_input<BuildColumnType, HashedType, true>(
            _build_input_table, _column_ids.first, histograms_build_column, _radix_bits, build_side_bloom_filter,
            input_bloom_filter);
      } else {
        materialized_build_column = materialize_input<BuildColumnType, HashedType, false>(
            _build_input_table, _column_ids.first, histograms_build_column, _radix_bits, build_side_bloom_filter,
            input_bloom_filter);
      }
    };

    /**
     * 1.2. Materialize the larger probe partition. Use the Bloom filter from the probe partition to skip rows that
     *       will not find a join partner.
     */
    const auto materialize_probe_side = [&](const auto& input_bloom_filter) {
      if (keep_nulls_probe_column) {
        materialized_probe_column = materialize_input<ProbeColumnType, HashedType, true>(
            _probe_input_table, _column_ids.second, histograms_probe_column, _radix_bits, probe_side_bloom_filter,
            input_bloom_filter);
      } else {
        materialized_probe_column = materialize_input<ProbeColumnType, HashedType, false>(
            _probe_input_table, _column_ids.second, histograms_probe_column, _radix_bits, probe_side_bloom_filter,
            input_bloom_filter);
      }
    };

    Timer timer_materialization;
    if (_build_input_table->row_count() < _probe_input_table->row_count()) {
      // When materializing the first side (here: the build side), we do not yet have a Bloom filter. To keep the number
      // of code paths low, materialize_*_side always expects a Bloom filter. For the first step, we thus pass in a
      // Bloom filter that returns true for every probe.
      materialize_build_side(ALL_TRUE_BLOOM_FILTER);
      _performance_data.set_step_runtime(OperatorSteps::BuildSideMaterializing, timer_materialization.lap());
      materialize_probe_side(build_side_bloom_filter);
      _performance_data.set_step_runtime(OperatorSteps::ProbeSideMaterializing, timer_materialization.lap());
    } else {
      // Here, we first materialize the probe side and use the resulting Bloom filter in the materialization of the
      // build side. Consequently, the Bloom filter later passed into build() will have no effect as it has already
      // been used here to filter non-matching values.
      materialize_probe_side(ALL_TRUE_BLOOM_FILTER);
      _performance_data.set_step_runtime(OperatorSteps::ProbeSideMaterializing, timer_materialization.lap());
      materialize_build_side(probe_side_bloom_filter);
      _performance_data.set_step_runtime(OperatorSteps::BuildSideMaterializing, timer_materialization.lap());
    }

    // Store the number of materialized values. Depending on the order of materialization (which depends on the input
    // sizes), each side might or might not be filtered by the Bloom filter.
    for (const auto& partition : materialized_build_column) {
      _performance_data.build_side_materialized_value_count += partition.elements.size();
    }
    for (const auto& partition : materialized_probe_column) {
      _performance_data.probe_side_materialized_value_count += partition.elements.size();
    }

    /**
     * 2. Perform radix partitioning for build and probe sides. The Bloom filters are not used in this step. Future work
     *    could use them on the build side to exclude them for values that are not seen on the probe side. That would
     *    reduce the size of the intermediary results, but would require an adapted calculation of the output offsets
     *    within partition_by_radix.
     */
    if (_radix_bits > 0) {
      Timer timer_clustering;
      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};

      jobs.emplace_back(std::make_shared<JobTask>([&]() {
        // radix partition the build table
        if (keep_nulls_build_column) {
          radix_build_column = partition_by_radix<BuildColumnType, HashedType, true>(
              materialized_build_column, histograms_build_column, _radix_bits);
        } else {
          radix_build_column = partition_by_radix<BuildColumnType, HashedType, false>(
              materialized_build_column, histograms_build_column, _radix_bits);
        }

        // After the data in materialized_build_column has been partitioned, it is not needed anymore.
        materialized_build_column.clear();
      }));

      jobs.emplace_back(std::make_shared<JobTask>([&]() {
        // radix partition the probe column.
        if (keep_nulls_probe_column) {
          radix_probe_column = partition_by_radix<ProbeColumnType, HashedType, true>(
              materialized_probe_column, histograms_probe_column, _radix_bits);
        } else {
          radix_probe_column = partition_by_radix<ProbeColumnType, HashedType, false>(
              materialized_probe_column, histograms_probe_column, _radix_bits);
        }

        // After the data in materialized_probe_column has been partitioned, it is not needed anymore.
        materialized_probe_column.clear();
      }));

      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

      histograms_build_column.clear();
      histograms_probe_column.clear();

      _performance_data.set_step_runtime(OperatorSteps::Clustering, timer_clustering.lap());
    } else {
      // short cut: skip radix partitioning and use materialized data directly
      radix_build_column = std::move(materialized_build_column);
      radix_probe_column = std::move(materialized_probe_column);
    }

    /**
     * 3. Build hash tables.
     *    In the case of semi or anti joins, we do not need to track all rows on the hashed side, just one per value.
     *    value. However, if we have secondary predicates, those might fail on that single row. In that case, we DO need
     *    all rows.
     *    We use the probe side's Bloom filter to exclude values from the hash table that will not be accessed in the
     *    probe step.
     */
    Timer timer_hash_map_building;
    if (_secondary_predicates.empty() &&
        (_mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsTrue || _mode == JoinMode::AntiNullAsFalse)) {
      hash_tables = build<BuildColumnType, HashedType>(radix_build_column, JoinHashBuildMode::ExistenceOnly,
                                                       _radix_bits, probe_side_bloom_filter);
    } else {
      hash_tables = build<BuildColumnType, HashedType>(radix_build_column, JoinHashBuildMode::AllPositions, _radix_bits,
                                                       probe_side_bloom_filter);
    }
    _performance_data.set_step_runtime(OperatorSteps::Building, timer_hash_map_building.lap());

    // Store the element counts of the built hash tables. Depending on the Bloom filter, we might have significantly
    // less values stored than in the initial input table.
    for (const auto& hash_table : hash_tables) {
      if (!hash_table) {
        continue;
      }

      _performance_data.hash_tables_distinct_value_count += hash_table->distinct_value_count();
      const auto position_count = hash_table->position_count();
      if (position_count) {
        // Update or set hash_tables_position_count if hash table stores positions.
        _performance_data.hash_tables_position_count =
            _performance_data.hash_tables_position_count.value_or(0) + *position_count;
      }
    }

    /**
     * Short cut for AntiNullAsTrue:
     *   If there is any NULL value on the build side, do not bother probing as no tuples can be emitted anyway (as
     *   long as JoinHash/AntiNullAsTrue doesn't support secondary predicates). Doing this early out right here is
     *   hacky, but during probing we assume NULL values on the build side do not matter, so we'd have no chance
     *   detecting a NULL value on the build side there.
     */
    if (_mode == JoinMode::AntiNullAsTrue) {
      for (const auto& build_side_partition : radix_build_column) {
        for (const auto null_value : build_side_partition.null_values) {
          if (null_value) {
            Timer timer_output_writing;
            const auto result = _join_hash._build_output_table({});
            _performance_data.set_step_runtime(OperatorSteps::OutputWriting, timer_output_writing.lap());
            return result;
          }
        }
      }
    }

    radix_build_column.clear();

    /**
     * 4. Probe step
     */
    std::vector<RowIDPosList> build_side_pos_lists;
    std::vector<RowIDPosList> probe_side_pos_lists;
    const size_t partition_count = radix_probe_column.size();
    build_side_pos_lists.resize(partition_count);
    probe_side_pos_lists.resize(partition_count);

    // simple heuristic: half of the rows of the probe side will match
    const size_t result_rows_per_partition =
        _probe_input_table->row_count() > 0 ? _probe_input_table->row_count() / partition_count / 2 : 0;
    for (auto partition_index = size_t{0}; partition_index < partition_count; ++partition_index) {
      build_side_pos_lists[partition_index].reserve(result_rows_per_partition);
      probe_side_pos_lists[partition_index].reserve(result_rows_per_partition);
    }

    Timer timer_probing;
    switch (_mode) {
      case JoinMode::Inner:
        probe<ProbeColumnType, HashedType, false>(radix_probe_column, hash_tables, build_side_pos_lists,
                                                  probe_side_pos_lists, _mode, *_build_input_table, *_probe_input_table,
                                                  _secondary_predicates);
        break;

      case JoinMode::Left:
      case JoinMode::Right:
        probe<ProbeColumnType, HashedType, true>(radix_probe_column, hash_tables, build_side_pos_lists,
                                                 probe_side_pos_lists, _mode, *_build_input_table, *_probe_input_table,
                                                 _secondary_predicates);
        break;

      case JoinMode::Semi:
        probe_semi_anti<ProbeColumnType, HashedType, JoinMode::Semi>(radix_probe_column, hash_tables,
                                                                     probe_side_pos_lists, *_build_input_table,
                                                                     *_probe_input_table, _secondary_predicates);
        break;

      case JoinMode::AntiNullAsTrue:
        probe_semi_anti<ProbeColumnType, HashedType, JoinMode::AntiNullAsTrue>(
            radix_probe_column, hash_tables, probe_side_pos_lists, *_build_input_table, *_probe_input_table,
            _secondary_predicates);
        break;

      case JoinMode::AntiNullAsFalse:
        probe_semi_anti<ProbeColumnType, HashedType, JoinMode::AntiNullAsFalse>(
            radix_probe_column, hash_tables, probe_side_pos_lists, *_build_input_table, *_probe_input_table,
            _secondary_predicates);
        break;

      default:
        Fail("JoinMode not supported by JoinHash");
    }
    _performance_data.set_step_runtime(OperatorSteps::Probing, timer_probing.lap());

    radix_probe_column.clear();
    hash_tables.clear();

    /**
     * 5. Write output Table
     */

    /**
     * After the probe step build_side_pos_lists and probe_side_pos_lists contain all pairs of joined rows grouped by
     * partition. Let p be a partition index and r a row index. The value of build_side_pos_lists[p][r] will match
     * probe_side_pos_lists[p][r].
     */

    Timer timer_output_writing;

    const auto create_left_side_pos_lists_by_segment =
        (_build_input_table->type() == TableType::References && _output_column_order != OutputColumnOrder::RightOnly);
    const auto create_right_side_pos_lists_by_segment = (_probe_input_table->type() == TableType::References);

    // A hash join's input can be heavily pre-filtered or the join results in very few matches. To counteract this the
    // partitions can be merged (#2202).
    constexpr auto ALLOW_PARTITION_MERGE = true;
    auto output_chunks =
        write_output_chunks(build_side_pos_lists, probe_side_pos_lists, _build_input_table, _probe_input_table,
                            create_left_side_pos_lists_by_segment, create_right_side_pos_lists_by_segment,
                            _output_column_order, ALLOW_PARTITION_MERGE);

    _performance_data.set_step_runtime(OperatorSteps::OutputWriting, timer_output_writing.lap());

    return _join_hash._build_output_table(std::move(output_chunks));
  }
};

void JoinHash::PerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  OperatorPerformanceData<OperatorSteps>::output_to_stream(stream, description_mode);

  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
  stream << separator << "Radix bits: " << radix_bits << ".";
  stream << separator << "Build side is " << (left_input_is_build_side ? "left." : "right.");
}

}  // namespace hyrise
