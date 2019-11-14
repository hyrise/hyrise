#include "aggregate_hashsort.hpp"

#include "boost/functional/hash.hpp"
#include "boost/hana.hpp"

#include "operators/aggregate/aggregate_hashsort_algorithm.hpp"
#include "operators/aggregate/aggregate_hashsort_mt.hpp"
#include "operators/aggregate/aggregate_hashsort_utils.hpp"
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

/**
 * Pick the GroupSizePolicy, aka the way group keys are stored.
 * Chooses between StaticFixedGroupSizePolicy<>, DynamicFixedGroupSizePolicy and VariableGroupSizePolicy
 */
template <typename Functor>
void resolve_group_size_policy(const AggregateHashSortEnvironment& environment, const Functor& functor) {
  constexpr auto MAX_STATIC_GROUP_SIZE = 4;

  if (environment.variably_sized_column_ids.empty()) {
    if (environment.fixed_group_size > MAX_STATIC_GROUP_SIZE) {
      functor(hana::type_c<DynamicFixedGroupSizePolicy>);
    } else {
      hana::for_each(hana::make_range(hana::size_c<1>, hana::size_c<MAX_STATIC_GROUP_SIZE + 1>),
                     [&](const auto value_t) {
                       if (environment.fixed_group_size == +value_t) {
                         functor(hana::type_c<StaticFixedGroupSizePolicy<+value_t>>);
                       }
                     });
    }
  } else {
    functor(hana::type_c<VariableGroupSizePolicy>);
  }
}

}  // namespace

namespace opossum {

AggregateHashSortConfig AggregateHashSort::create_config() { return {}; }

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

  auto environment = AggregateHashSortEnvironment::create(_config, input_table, _aggregates, _groupby_column_ids,
                                                    _get_output_column_defintions());

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>();

  resolve_group_size_policy(*environment, [&](const auto group_size_policy_t) {
    using GroupSizePolicy = typename decltype(group_size_policy_t)::type;
    using Run = BasicRun<GroupSizePolicy>;

    /**
     * Create an AggregateHashSortTask for each Chunk in the input table and schedule it
     */
    const auto task_set = std::make_shared<AggregateHashSortTaskSet<Run>>(environment, 0);
    auto tasks = std::vector<std::shared_ptr<AggregateHashSortTask<Run>>>{};
    for (auto chunk_id = ChunkID{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
      const auto run_source = std::make_shared<ChunkRunSource<Run>>(environment, input_table, chunk_id);
      const auto task = std::make_shared<AggregateHashSortTask<Run>>(environment, task_set, run_source);
      task_set->add_task(task);
      tasks.emplace_back(task);
    }

    CurrentScheduler::schedule_tasks(tasks);
  });

  /**
   * Wait for all Tasks to finish
   */
  {
    auto lock = std::unique_lock{environment->output_chunks_mutex};
    environment->done_condition.wait(lock, [&]() { return environment->global_task_counter == 0; });
  }

  const auto output_table =
      std::make_shared<Table>(environment->output_column_definitions, TableType::Data, std::move(environment->output_chunks));

  return output_table;
}

}  // namespace opossum
