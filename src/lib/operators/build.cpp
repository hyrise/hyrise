#include "build.hpp"

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
#include "expression/expression_utils.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/join_hash.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace hyrise {

Build::Build(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID column_id,
             const DataType build_type, const DataType probe_type)
    : AbstractReadOnlyOperator(OperatorType::Build, input_operator, nullptr,
                               std::make_unique<JoinHash::PerformanceData>()),
      _column_id{column_id},
      _build_type{build_type},
      _probe_type{probe_type},
      _bloom_filter{} {}

const std::string& Build::name() const {
  static const auto name = std::string{"Build"};
  return name;
}

ColumnID Build::column_id() const {
  return _column_id;
}

const BloomFilter& Build::bloom_filter() const {
  return _bloom_filter;
}

size_t Build::radix_bits() const {
  Assert(_radix_bits, "Radix bits have not been set.");
  return *_radix_bits;
}

DataType Build::build_data_type() const {
  return _build_type;
}

DataType Build::probe_data_type() const {
  return _probe_type;
}

const BaseBuildStatistics& Build::build_statistics() const {
  Assert(_build_statistics, "Statistics not set.");
  return *_build_statistics;
}

void Build::clear_statistics() const {
  _build_statistics.reset();
}

std::shared_ptr<AbstractOperator> Build::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<Build>(copied_left_input, _column_id, _build_type, _probe_type);
}

std::shared_ptr<const Table> Build::_on_execute() {
  const auto build_input_table = _left_input->get_output();
  const auto _data_type = build_input_table->column_data_type(_column_id);

  auto& join_hash_performance_data = dynamic_cast<JoinHash::PerformanceData&>(*performance_data);
  resolve_data_type(_data_type, [&](const auto build_data_type_t) {
    using BuildColumnType = typename decltype(build_data_type_t)::type;
    resolve_data_type(_probe_type, [&](const auto probe_data_type_t) {
      using ProbeColumnType = typename decltype(probe_data_type_t)::type;
      using HashedType = typename JoinHashTraits<BuildColumnType, ProbeColumnType>::HashType;

      if constexpr (std::is_integral_v<BuildColumnType> && std::is_integral_v<HashedType>) {
        const auto row_count = build_input_table->row_count();
        _radix_bits = JoinHash::calculate_radix_bits(row_count, row_count);
        join_hash_performance_data.radix_bits = *_radix_bits;

        auto histograms_build_column = std::vector<std::vector<size_t>>{};
        auto timer_materialization = Timer{};

        auto materialized = materialize_input<BuildColumnType, HashedType, false, true>(
            build_input_table, _column_id, histograms_build_column, *_radix_bits, _bloom_filter, ALL_TRUE_BLOOM_FILTER);
        join_hash_performance_data.set_step_runtime(JoinHash::OperatorSteps::BuildSideMaterializing,
                                                    timer_materialization.lap());

        auto statistics = std::make_unique<BuildStatistics<BuildColumnType, HashedType>>();

        for (const auto& partition : materialized.container) {
          join_hash_performance_data.build_side_materialized_value_count += partition.elements.size();
        }

        statistics->min = *materialized.min;
        statistics->max = *materialized.max;
        statistics->is_continuous = materialized.is_continuous;
        statistics->row_count = materialized.row_count;

        if (materialized.row_count == 1 || materialized.is_continuous) {
          _build_statistics = std::move(statistics);
          return;
        }

        auto radix_build_column = RadixContainer<BuildColumnType>{};

        if (*_radix_bits > 0) {
          auto timer_clustering = Timer{};

          radix_build_column = partition_by_radix<BuildColumnType, HashedType, false>(
              materialized.container, histograms_build_column, *_radix_bits);

          // After the data in materialized_build_column has been partitioned, it is not needed anymore.
          materialized.container.clear();

          histograms_build_column.clear();

          join_hash_performance_data.set_step_runtime(JoinHash::OperatorSteps::Clustering, timer_clustering.lap());
        } else {
          // short cut: skip radix partitioning and use materialized data directly
          radix_build_column = std::move(materialized.container);
        }

        auto timer_hash_map_building = Timer{};

        statistics->hash_tables = build<BuildColumnType, HashedType>(
            radix_build_column, JoinHashBuildMode::ExistenceOnly, *_radix_bits, ALL_TRUE_BLOOM_FILTER);

        join_hash_performance_data.set_step_runtime(JoinHash::OperatorSteps::Building, timer_hash_map_building.lap());

        // Store the element counts of the built hash tables. Depending on the Bloom filter, we might have significantly
        // less values stored than in the initial input table.
        for (const auto& hash_table : statistics->hash_tables) {
          if (!hash_table) {
            continue;
          }

          join_hash_performance_data.hash_tables_distinct_value_count += hash_table->distinct_value_count();
          const auto position_count = hash_table->position_count();
          if (position_count) {
            // Update or set hash_tables_position_count if hash table stores positions.
            join_hash_performance_data.hash_tables_position_count =
                join_hash_performance_data.hash_tables_position_count.value_or(0) + *position_count;
          }
        }

        _build_statistics = std::move(statistics);
      } else {
        Fail("Build only supports integral types.");
      }
    });
  });

  return left_input_table();
}

void Build::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

template class BuildStatistics<int32_t, int32_t>;
template class BuildStatistics<int32_t, int64_t>;
template class BuildStatistics<int64_t, int64_t>;

}  // namespace hyrise
