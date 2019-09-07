#include "aggregate_hashsort.hpp"

#include "boost/functional/hash.hpp"
#include "boost/hana.hpp"

#include "operators/aggregate/aggregate_hashsort_algorithm.hpp"
#include "operators/aggregate/aggregate_hashsort_utils.hpp"
#include "operators/aggregate/aggregate_hashsort_mt.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/segment_iterate.hpp"

#if !VERBOSE
#define DBG_MACRO_DISABLE
#endif
#include "dbg.h"

using namespace opossum::aggregate_hashsort;  // NOLINT
namespace hana = boost::hana;

namespace {

template <typename Functor>
void resolve_group_size_policy(const AggregateHashSortSetup& definition, const Functor& functor) {
  constexpr auto MAX_STATIC_GROUP_SIZE = 4;

  if (definition.variably_sized_column_ids.empty()) {
    if (definition.fixed_group_size > MAX_STATIC_GROUP_SIZE) {
      dbg("DynamicFixedGroupSizePolicy");
      functor(hana::type_c<DynamicFixedGroupSizePolicy>);
    } else {
      hana::for_each(hana::make_range(hana::size_c<1>, hana::size_c<MAX_STATIC_GROUP_SIZE + 1>), [&](const auto value_t) {
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

AggregateHashSortConfig AggregateHashSort::create_config() {
  return {};
}

AggregateHashSort::AggregateHashSort(const std::shared_ptr<AbstractOperator>& in,
                                     const std::vector<AggregateColumnDefinition>& aggregates,
                                     const std::vector<ColumnID>& groupby_column_ids,
                                     const std::optional<AggregateHashSortConfig>& config)
    : AbstractAggregateOperator(in, aggregates, groupby_column_ids), _config(config ? *config : create_config()) {}

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

  const auto setup = AggregateHashSortSetup::create(_config, input_table, _aggregates, _groupby_column_ids);

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>();
  const auto output_column_definitions = _get_output_column_defintions();

#if VERBOSE
  Timer t;
#endif

  resolve_group_size_policy(setup, [&](const auto group_size_policy_t) {
    using GroupSizePolicy = typename decltype(group_size_policy_t)::type;
    using Run = BasicRun<GroupSizePolicy>;

    auto current_chunk_id = ChunkID{0};
    auto current_chunk_offset = ChunkOffset{0};
    auto slice_begin_chunk_id = ChunkID{0};
    auto slice_row_count = size_t{0};
    auto tasks = std::vector<std::shared_ptr<mt::AggregateHashSortTask<Run>>>{};

    const auto chunk_count = input_table->chunk_count();

    while (current_chunk_id < chunk_count) {
      const auto& chunk = input_table->get_chunk(current_chunk_id);
      const auto remaining_rows_in_chunk = chunk->size() - current_chunk_offset;

      if (slice_row_count + remaining_rows_in_chunk >= _config.initial_run_size) {
        slice_row_count += std::min(remaining_rows_in_chunk, _config.initial_run_size - slice_row_count);
        const auto run_source = std::make_shared<TableRunSource<Run>>(setup, input_table);
        const auto task = std::make_shared<mt::AggregateHashSortTask<Run>>(setup, input_table, slice_begin_chunk_id);

      }
    }

    auto first

    const auto run_source = std::make_shared<TableRunSource<Run>>(setup, input_table);
    const auto abstract_run_source = std::static_pointer_cast<AbstractRunSource<BasicRun<GroupSizePolicy>>>(run_source);

    const auto output_runs = aggregate(setup, abstract_run_source, 0u);

    /**
     * Materialize aggregate/group runs into segments
     */
    output_chunks.resize(output_runs.size());

    for (auto run_idx = size_t{0}; run_idx < output_runs.size(); ++run_idx) {
      auto& run = output_runs[run_idx];
      auto output_segments = Segments{_aggregates.size() + _groupby_column_ids.size()};

      // Materialize the group-by columns
      for (auto column_id = ColumnID{0}; column_id < _groupby_column_ids.size(); ++column_id) {
        const auto& output_column_definition = output_column_definitions[column_id];
        resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          output_segments[column_id] = run.template materialize_group_column<ColumnDataType>(setup,
              column_id, output_column_definition.nullable);
        });
      }

      // Materialize the aggregate columns
      for (auto aggregate_idx = ColumnID{0}; aggregate_idx < _aggregates.size(); ++aggregate_idx) {
        output_segments[_groupby_column_ids.size() + aggregate_idx] =
            run.aggregates[aggregate_idx]->materialize_output(run.size);
      }

      output_chunks[run_idx] = std::make_shared<Chunk>(output_segments);
    }
  });

  const auto output_table =
      std::make_shared<Table>(output_column_definitions, TableType::Data, std::move(output_chunks));
#if VERBOSE
  std::cout << "Building output table with " << output_table->row_count() << " rows and " << output_table->chunk_count()
            << " chunks in " << t.lap_formatted() << std::endl;
#endif

  return output_table;
}


}  // namespace opossum
