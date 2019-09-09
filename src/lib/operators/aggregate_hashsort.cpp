#include "aggregate_hashsort.hpp"

#include "boost/functional/hash.hpp"
#include "boost/hana.hpp"

#include "operators/aggregate/aggregate_hashsort_algorithm.hpp"
#include "operators/aggregate/aggregate_hashsort_utils.hpp"
#include "operators/aggregate/aggregate_hashsort_mt.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "scheduler/current_scheduler.hpp"
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

  auto setup = AggregateHashSortSetup::create(_config, input_table, _aggregates, _groupby_column_ids);
  setup->output_column_definitions = _get_output_column_defintions();

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>();

#if VERBOSE
  Timer t;
#endif

  resolve_group_size_policy(*setup, [&](const auto group_size_policy_t) {
    using GroupSizePolicy = typename decltype(group_size_policy_t)::type;
    using Run = BasicRun<GroupSizePolicy>;

    const auto task_set = std::make_shared<mt::AggregateHashSortTaskSet<Run>>(setup);
    auto tasks = std::vector<std::shared_ptr<mt::AggregateHashSortTask<Run>>>{};
    for (auto chunk_id = ChunkID{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
      const auto input_chunk = input_table->get_chunk(chunk_id);
      const auto run_source = std::make_shared<TableRunSource<Run>>(setup, input_chunk);
      task_set->add_task(std::make_shared<mt::AggregateHashSortTask<Run>>(task_set, setup, run_source));
    }

    task_set->schedule();

    if (CurrentScheduler::is_set()) {
      auto lock = std::unique_lock{setup->output_chunks_mutex};
      setup->done_condition.wait(lock, [&]() { return setup->global_task_counter.load() == 0; });
    }
  });

  const auto output_table =
      std::make_shared<Table>(output_column_definitions, TableType::Data, std::move(setup->output_chunks));

#if VERBOSE
  std::cout << "Building output table with " << output_table->row_count() << " rows and " << output_table->chunk_count()
            << " chunks in " << t.lap_formatted() << std::endl;
#endif

  return output_table;
}


}  // namespace opossum
