#include "aggregate_hashsort.hpp"

#include "boost/functional/hash.hpp"
#include "boost/hana.hpp"

#include "operators/aggregate/aggregate_hashsort_steps.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/segment_iterate.hpp"

#include "dbg.h"

using namespace opossum::aggregate_hashsort;  // NOLINT
namespace hana = boost::hana;

namespace {

template<typename Functor>
void resolve_group_size_policy(const AggregateHashSortDefinition& definition, const Functor& functor) {
  constexpr auto MAX_STATIC_GROUP_SIZE = 4;

  if (definition.variably_sized_column_ids.empty()) {
    if (definition.fixed_group_size > MAX_STATIC_GROUP_SIZE) {
      dbg("DynamicFixedGroupSizePolicy");
      functor(hana::type_c<DynamicFixedGroupSizePolicy>);
    } else {
      hana::for_each(hana::make_range(hana::size_c<1>, hana::size_c<5>), [&](const auto value_t) {
        if (definition.fixed_group_size == +value_t) {
          dbg("StaticFixedGroupSizePolicy");
          dbg(+value_t);
          functor(hana::type_c<StaticFixedGroupSizePolicy<+value_t>>);
        }
      });
    }
  } else {
    dbg("VariableGroupSizePolicy");
    functor(hana::type_c<VariableGroupSizePolicy>);
  }
}

}  // namespace

namespace opossum {

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
  const auto input_table = input_table_left();

  const auto definition = AggregateHashSortDefinition::create(input_table, _aggregates, _groupby_column_ids);

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>();
  const auto output_column_definitions = _get_output_column_defintions();

  resolve_group_size_policy(definition, [&](const auto group_size_policy_t) {
//    using GroupSizePolicy = typename decltype(group_size_policy_t)::type;
//    const auto run_source = std::make_shared<TableRunSource<GroupSizePolicy>>(input_table, &layout, _config, _aggregates, _groupby_column_ids);
//    const auto output_runs = aggregate(_config, run_source, 0u);
//
//    /**
//     * Materialize aggregate/group runs into segments
//     */
//#if VERBOSE
//  Timer t;
//#endif
//    output_chunks.resize(output_runs.size());
//
//    for (auto run_idx = size_t{0}; run_idx < output_runs.size(); ++run_idx) {
//      auto& run = output_runs[run_idx];
//      auto output_segments = Segments{_aggregates.size() + _groupby_column_ids.size()};
//
//      // Materialize the group-by columns
//      for (auto column_id = ColumnID{0}; column_id < _groupby_column_ids.size(); ++column_id) {
//        const auto& output_column_definition = output_column_definitions[column_id];
//        resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
//          using ColumnDataType = typename decltype(data_type_t)::type;
//          output_segments[column_id] = run.groups.template materialize_output<ColumnDataType>(
//              column_id, output_column_definition.nullable);
//        });
//      }
//
//      // Materialize the aggregate columns
//      for (auto aggregate_idx = ColumnID{0}; aggregate_idx < _aggregates.size(); ++aggregate_idx) {
//        output_segments[_groupby_column_ids.size() + aggregate_idx] =
//            run.aggregates[aggregate_idx]->materialize_output(run.size());
//      }
//
//      output_chunks[run_idx] = std::make_shared<Chunk>(output_segments);
//    }
//#if VERBOSE
//  std::cout << "Building output table with " << output_table->row_count() << " rows and " << output_table->chunk_count()
//            << " chunks in " << t.lap_formatted() << std::endl;
//#endif
  });

  const auto output_table =
      std::make_shared<Table>(output_column_definitions, TableType::Data, std::move(output_chunks));

  return output_table;
}

//template <typename GroupRun>
//std::shared_ptr<const Table> AggregateHashSort::_on_execute_with_group_run(
//    const typename GroupRun::LayoutType& layout) {
//  auto input_table = input_table_left();
//
//  auto run_source =
//      std::make_shared<TableRunSource<GroupRun>>(input_table, &layout, _config, _aggregates, _groupby_column_ids);
//
//  auto output_runs = aggregate<GroupRun>(_config, std::move(run_source), 0u);
//
//  /**
//   * Build output Table
//   */
//#if VERBOSE
//  Timer t;
//#endif
//
//  const auto output_column_definitions = _get_output_column_defintions();
//  auto output_chunks = std::vector<std::shared_ptr<Chunk>>(output_runs.size());
//
//  for (auto run_idx = size_t{0}; run_idx < output_runs.size(); ++run_idx) {
//    auto& run = output_runs[run_idx];
//    auto output_segments = Segments{_aggregates.size() + _groupby_column_ids.size()};
//
//    for (auto output_group_by_column_id = ColumnID{0}; output_group_by_column_id < _groupby_column_ids.size();
//         ++output_group_by_column_id) {
//      const auto& output_column_definition = output_column_definitions[output_group_by_column_id];
//      resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
//        using ColumnDataType = typename decltype(data_type_t)::type;
//        output_segments[output_group_by_column_id] = run.groups.template materialize_output<ColumnDataType>(
//            output_group_by_column_id, output_column_definition.nullable);
//      });
//    }
//
//    for (auto aggregate_idx = ColumnID{0}; aggregate_idx < _aggregates.size(); ++aggregate_idx) {
//      output_segments[_groupby_column_ids.size() + aggregate_idx] =
//          run.aggregates[aggregate_idx]->materialize_output(run.size());
//    }
//
//    output_chunks[run_idx] = std::make_shared<Chunk>(output_segments);
//  }
//
//  const auto output_table =
//      std::make_shared<Table>(output_column_definitions, TableType::Data, std::move(output_chunks));
//
//#if VERBOSE
//  std::cout << "Building output table with " << output_table->row_count() << " rows and " << output_table->chunk_count()
//            << " chunks in " << t.lap_formatted() << std::endl;
//#endif
//
//  return output_table;
//}

}  // namespace opossum
