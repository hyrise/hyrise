#include "aggregate_hashsort.hpp"

#include "boost/functional/hash.hpp"

#include "operators/aggregate/aggregate_hashsort_steps.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

using namespace aggregate_hashsort;  // NOLINT

AggregateHashSort::AggregateHashSort(const std::shared_ptr<AbstractOperator>& in,
                                     const std::vector<AggregateColumnDefinition>& aggregates,
                                     const std::vector<ColumnID>& groupby_column_ids,
                                     const AggregateHashSortConfig& config)
    : AbstractAggregateOperator(in, aggregates, groupby_column_ids), _config(config) {}

const std::string AggregateHashSort::name() const { return "AggregateHashSort"; }

std::shared_ptr<AbstractOperator> AggregateHashSort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<AggregateHashSort>(copied_input_left, _aggregates, _groupby_column_ids);
}

void AggregateHashSort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateHashSort::_on_cleanup() {}

std::shared_ptr<const Table> AggregateHashSort::_on_execute() {
  auto& input_table = *input_table_left();

  const auto fixed_size_groups = std::all_of(
      _groupby_column_ids.begin(), _groupby_column_ids.end(),
      [&](const ColumnID column_id) { return input_table.column_data_type(column_id) != DataType::String; });

  if (fixed_size_groups) {
    auto input_groups = produce_initial_groups<FixedSizeGroupRun>(input_table, _groupby_column_ids);
    auto input_aggregates = produce_initial_aggregates(input_table, _aggregates);

    auto input_run = Run{std::move(input_groups), std::move(input_aggregates)};
    std::vector<Run<FixedSizeGroupRun>> input_runs;
    input_runs.emplace_back(std::move(input_run));

    auto result_runs = aggregate<FixedSizeGroupRun>(_config, std::move(input_runs), 1u);

    /**
     * Build output Table
     */
    const auto output_column_definitions = _get_output_column_defintions();
    auto output_chunks = std::vector<std::shared_ptr<Chunk>>(result_runs.size());

    for (auto run_idx = size_t{0}; run_idx < result_runs.size(); ++run_idx) {
      auto& run = result_runs[run_idx];
      auto output_segments = Segments{_aggregates.size() + _groupby_column_ids.size()};

      for (auto output_group_by_column_id = ColumnID{0}; output_group_by_column_id < _groupby_column_ids.size();
           ++output_group_by_column_id) {
        const auto& output_column_definition = output_column_definitions[output_group_by_column_id];
        resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          output_segments[output_group_by_column_id] = run.groups.materialize_output<ColumnDataType>(output_group_by_column_id, output_column_definition.nullable);
        });
      }

      for (auto aggregate_idx = ColumnID{0}; aggregate_idx < _aggregates.size(); ++aggregate_idx) {
        output_segments[_groupby_column_ids.size() + aggregate_idx] = run.aggregates[aggregate_idx]->materialize_output();
      }

      output_chunks[run_idx] = std::make_shared<Chunk>(output_segments);
    }

    return std::make_shared<Table>(output_column_definitions, TableType::Data, std::move(output_chunks));
  } else {
    Fail("Nope");
  }
}

}  // namespace opossum
