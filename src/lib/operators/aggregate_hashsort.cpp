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
    return _on_execute_with_group_run<FixedSizeGroupRun>();
  } else {
    return _on_execute_with_group_run<VariablySizedGroupRun>();
  }
}

template <typename GroupRun>
std::shared_ptr<const Table> AggregateHashSort::_on_execute_with_group_run() {
  auto input_table = input_table_left();

  auto layout = produce_initial_groups_layout<typename GroupRun::LayoutType>(*input_table, _groupby_column_ids);

  auto run_source = std::make_unique<TableRunSource<GroupRun>>(input_table, &layout, _config, _aggregates, _groupby_column_ids);

  auto output_runs = aggregate<GroupRun>(_config, std::move(run_source), 1u);

  /**
   * Build output Table
   */
  const auto output_column_definitions = _get_output_column_defintions();
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>(output_runs.size());

  for (auto run_idx = size_t{0}; run_idx < output_runs.size(); ++run_idx) {
    auto& run = output_runs[run_idx];
    auto output_segments = Segments{_aggregates.size() + _groupby_column_ids.size()};

    for (auto output_group_by_column_id = ColumnID{0}; output_group_by_column_id < _groupby_column_ids.size();
         ++output_group_by_column_id) {
      const auto& output_column_definition = output_column_definitions[output_group_by_column_id];
      resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        output_segments[output_group_by_column_id] = run.groups.template materialize_output<ColumnDataType>(
            output_group_by_column_id, output_column_definition.nullable);
      });
    }

    for (auto aggregate_idx = ColumnID{0}; aggregate_idx < _aggregates.size(); ++aggregate_idx) {
      output_segments[_groupby_column_ids.size() + aggregate_idx] = run.aggregates[aggregate_idx]->materialize_output(run.size());
    }

    output_chunks[run_idx] = std::make_shared<Chunk>(output_segments);
  }

  return std::make_shared<Table>(output_column_definitions, TableType::Data, std::move(output_chunks));
}

}  // namespace opossum
