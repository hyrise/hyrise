#include "probe.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "join_helper/join_output_writing.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/build.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

bool Probe::supports(const JoinConfiguration& config) {
  // JoinHash supports only equi joins and every join mode, except FullOuter.
  // Secondary predicates in AntiNullAsTrue are not supported, because implementing them is cumbersome and we couldn't
  // so far determine a case/query where we'd need them.
  const auto is_integer = [](const auto type) {
    return type == DataType::Int || type == DataType::Long;
  };
  return config.predicate_condition == PredicateCondition::Equals && config.join_mode == JoinMode::Semi &&
         !config.secondary_predicates && is_integer(config.left_data_type) && is_integer(config.right_data_type);
}

Probe::Probe(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
             const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
             const std::vector<OperatorJoinPredicate>& secondary_predicates)
    : AbstractJoinOperator(OperatorType::Probe, left, right, mode, primary_predicate, secondary_predicates,
                           std::make_unique<JoinHash::PerformanceData>()) {
  Assert(mode == JoinMode::Semi && secondary_predicates.empty(), "Invalid probe configuration.");
}

const std::string& Probe::name() const {
  static const auto name = std::string{"Probe"};
  return name;
}

std::shared_ptr<AbstractOperator> Probe::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<Probe>(copied_left_input, copied_right_input, _mode, _primary_predicate,
                                 _secondary_predicates);
}

std::shared_ptr<const Table> Probe::_on_execute() {
  Assert(_right_input->type() == OperatorType::Build, "Wrong operator type.");
  const auto& build = static_cast<const Build&>(*_right_input);

  auto& join_hash_performance_data = dynamic_cast<JoinHash::PerformanceData&>(*performance_data);
  resolve_data_type(build.build_data_type(), [&](const auto build_data_type_t) {
    using BuildColumnType = typename decltype(build_data_type_t)::type;
    resolve_data_type(build.probe_data_type(), [&](const auto probe_data_type_t) {
      using ProbeColumnType = typename decltype(probe_data_type_t)::type;
      using HashedType = typename JoinHashTraits<BuildColumnType, ProbeColumnType>::HashType;

      if constexpr (std::is_integral_v<BuildColumnType> && std::is_integral_v<HashedType> &&
                    std::is_integral_v<ProbeColumnType>) {
        const auto& build_statistics =
            static_cast<const BuildStatistics<BuildColumnType, HashedType>&>(build.build_statistics());

        if (build_statistics.row_count == 1 || build_statistics.is_continuous) {
          auto predicate = std::shared_ptr<AbstractExpression>{};
          const auto column = PQPColumnExpression::from_table(*left_input_table(), _primary_predicate.column_ids.first);
          if (build_statistics.row_count == 1) {
            Assert(build_statistics.min == build_statistics.max, "Values should be equal.");
            predicate = equals_(column, value_(build_statistics.min));
          } else {
            predicate = between_inclusive_(column, value_(build_statistics.min), value_(build_statistics.max));
          }

          const auto table_scan = std::make_shared<TableScan>(_left_input, predicate);
          table_scan->execute();
          _output = table_scan->get_output();
          return;
        }
        auto histograms_probe_column = std::vector<std::vector<size_t>>{};
        auto materialized_probe_column = RadixContainer<ProbeColumnType>{};
        auto radix_probe_column = RadixContainer<ProbeColumnType>{};
        auto probe_side_bloom_filter = BloomFilter{};

        auto timer_materialization = Timer{};

        materialized_probe_column = materialize_input<ProbeColumnType, HashedType, false>(
            left_input_table(), _primary_predicate.column_ids.first, histograms_probe_column, build.radix_bits(),
            probe_side_bloom_filter, build.bloom_filter());
        join_hash_performance_data.set_step_runtime(JoinHash::OperatorSteps::ProbeSideMaterializing,
                                                    timer_materialization.lap());

        // Store the number of materialized values. Depending on the order of materialization (which depends on the input
        // sizes), each side might or might not be filtered by the Bloom filter.
        for (const auto& partition : materialized_probe_column) {
          join_hash_performance_data.probe_side_materialized_value_count += partition.elements.size();
        }

        if (build.radix_bits() > 0) {
          auto timer_clustering = Timer{};

          radix_probe_column = partition_by_radix<ProbeColumnType, HashedType, false>(
              materialized_probe_column, histograms_probe_column, build.radix_bits());

          // After the data in materialized_probe_column has been partitioned, it is not needed anymore.
          materialized_probe_column.clear();
          histograms_probe_column.clear();

          join_hash_performance_data.set_step_runtime(JoinHash::OperatorSteps::Clustering, timer_clustering.lap());
        } else {
          // short cut: skip radix partitioning and use materialized data directly
          radix_probe_column = std::move(materialized_probe_column);
        }

        auto probe_side_pos_lists = std::vector<RowIDPosList>{};
        auto build_side_pos_lists = std::vector<RowIDPosList>{};

        auto timer_probing = Timer{};
        probe_semi_anti<ProbeColumnType, HashedType, JoinMode::Semi>(radix_probe_column, build_statistics.hash_tables,
                                                                     probe_side_pos_lists, *right_input_table(),
                                                                     *left_input_table(), {});
        join_hash_performance_data.set_step_runtime(JoinHash::OperatorSteps::Probing, timer_probing.lap());

        radix_probe_column.clear();
        build.clear_statistics();

        auto timer_output_writing = Timer{};
        const auto create_right_side_pos_lists_by_segment = (left_input_table()->type() == TableType::References);

        // A hash join's input can be heavily pre-filtered or the join results in very few matches. To counteract this the
        // partitions can be merged (#2202).
        constexpr auto ALLOW_PARTITION_MERGE = true;
        auto output_chunks = write_output_chunks(build_side_pos_lists, probe_side_pos_lists, right_input_table(),
                                                 left_input_table(), false, create_right_side_pos_lists_by_segment,
                                                 OutputColumnOrder::RightOnly, ALLOW_PARTITION_MERGE);

        join_hash_performance_data.set_step_runtime(JoinHash::OperatorSteps::OutputWriting, timer_output_writing.lap());

        _output = _build_output_table(std::move(output_chunks));
      } else {
        Fail("Probe only supports integral types.");
      }
    });
  });

  return _output;
}

}  // namespace hyrise
