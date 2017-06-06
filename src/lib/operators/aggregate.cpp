#include "aggregate.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

Aggregate::Aggregate(const std::shared_ptr<AbstractOperator> in,
                     const std::vector<std::pair<std::string, AggregateFunction>> aggregates,
                     const std::vector<std::string> groupby_columns)
    : AbstractReadOnlyOperator(in), _aggregates(aggregates), _groupby_columns(groupby_columns) {
  Assert(!(aggregates.empty() && groupby_columns.empty()), "Neither aggregate nor groupby columns have been specified");
}

const std::string Aggregate::name() const { return "Aggregate"; }

uint8_t Aggregate::num_in_tables() const { return 1; }

uint8_t Aggregate::num_out_tables() const { return 1; }

std::shared_ptr<const Table> Aggregate::on_execute() {
  auto input_table = input_table_left();

  // find group by column IDs
  std::vector<ColumnID> groupby_column_ids;
  std::transform(_groupby_columns.begin(), _groupby_columns.end(), std::back_inserter(groupby_column_ids),
                 [&](std::string name) { return input_table->column_id_by_name(name); });

  // find aggregated column IDs
  std::transform(
      _aggregates.begin(), _aggregates.end(), std::back_inserter(_aggregate_column_ids),
      [&](std::pair<std::string, AggregateFunction> pair) { return input_table->column_id_by_name(pair.first); });

  // check for invalid aggregates
  for (uint aggregate_index = 0; aggregate_index < _aggregates.size(); ++aggregate_index) {
    auto column_id = _aggregate_column_ids[aggregate_index];
    auto aggregate = _aggregates[aggregate_index].second;

    if (input_table->column_type(column_id) == "string" && (aggregate == Sum || aggregate == Avg)) {
      throw std::runtime_error("Aggregate: Cannot calculate SUM or AVG on string column");
    }
  }

  /*
  PARTITIONING PHASE
  First we partition the input chunks by the given group key(s).
  This is done by creating a vector that contains the AggregateKey for each row.
  It is gradually built by visitors, one for each group column.
  */
  auto keys_per_chunk = std::vector<std::shared_ptr<std::vector<AggregateKey>>>(input_table->chunk_count());

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(input_table->chunk_count());

  for (ChunkID chunk_id = 0; chunk_id < input_table->chunk_count(); ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id, groupby_column_ids]() {
      const Chunk &chunk_in = input_table->get_chunk(chunk_id);

      auto hash_keys = std::make_shared<std::vector<AggregateKey>>(chunk_in.size());

      // Partition by group columns
      for (auto column_id : groupby_column_ids) {
        auto base_column = chunk_in.get_column(column_id);
        auto column_type = input_table->column_type(column_id);

        auto builder = make_shared_by_column_type<ColumnVisitable, PartitionBuilder>(column_type);
        auto ctx = std::make_shared<GroupByContext>(input_table, chunk_id, column_id, hash_keys);
        base_column->visit(*builder, ctx);
      }

      keys_per_chunk[chunk_id] = hash_keys;
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  /*
  AGGREGATION PHASE
  */
  _contexts_per_column = std::vector<std::shared_ptr<ColumnVisitableContext>>(_aggregates.size());

  // pre-insert empty maps for each aggregate column
  for (ColumnID column_index = 0; column_index < _contexts_per_column.size(); ++column_index) {
    auto type_string = input_table->column_type(_aggregate_column_ids[column_index]);

    call_functor_by_column_type<AggregateContextCreator>(type_string, _contexts_per_column, column_index,
                                                         _aggregates[column_index].second);
  }

  /*
  Insert a dummy context for the DISTINCT implementation.
  That way, _contexts_per_column will always have atleast one context with results.
  This is important later on when we write the group keys into the table.
  */
  if (_aggregate_column_ids.empty()) {
    auto ctx = std::make_shared<AggregateContext<int32_t, int64_t>>();
    ctx->results = std::make_shared<std::map<AggregateKey, AggregateResult<int64_t>>>();

    _contexts_per_column.push_back(ctx);
  }

  for (ChunkID chunk_id = 0; chunk_id < input_table->chunk_count(); ++chunk_id) {
    const Chunk &chunk_in = input_table->get_chunk(chunk_id);

    auto hash_keys = keys_per_chunk[chunk_id];

    if (_aggregate_column_ids.empty()) {
      /**
       * DISTINCT implementation
       *
       * In Opossum we handle the SQL keyword DISTINCT by grouping without aggregation.
       *
       * For a query like "SELECT DISTINCT * FROM A;"
       * we would assume that all columns from A are part of 'groupby_columns',
       * respectively any columns that were specified in the projection.
       * The optimizer is responsible to take care of passing in the correct columns.
       *
       * How does this operation work?
       * Distinct rows are retrieved by grouping by vectors of values. Similar as for the usual aggregation
       * these vectors are used as keys in the 'column_results' map.
       *
       * At this point we've got all the different keys from the chunks and accumulate them in 'column_results'.
       * In order to reuse the aggregation implementation, we add a dummy AggregateResult.
       * One could optimize here in the future.
       *
       * Obviously this implementation is also used for plain GroupBy's.
       */

      auto ctx = std::static_pointer_cast<AggregateContext<int32_t, int64_t>>(_contexts_per_column[0]);
      auto &results = *ctx->results;
      for (auto &chunk : keys_per_chunk) {
        for (auto &keys : *chunk) {
          // insert dummy value to make sure we have the key in our map
          results[keys] = AggregateResult<int64_t>();
        }
      }
    } else {
      ColumnID column_index = 0;
      for (auto column_id : _aggregate_column_ids) {
        auto base_column = chunk_in.get_column(column_id);
        auto type_string = input_table->column_type(column_id);

        /*
        Invoke the AggregateVisitor for each aggregate column
        */
        auto groupby_ctx = std::make_shared<GroupByContext>(input_table, chunk_id, column_id, hash_keys);
        std::shared_ptr<ColumnVisitable> builder;
        auto ctx = _contexts_per_column[column_index];

        call_functor_by_column_type<AggregateVisitorCreator>(type_string, builder, ctx, groupby_ctx,
                                                             _aggregates[column_index].second);

        base_column->visit(*builder, ctx);
        column_index++;
      }
    }
  }

  // Write the output
  _output = std::make_shared<Table>();

  if (_groupby_columns.size()) {
    // add group by columns
    for (ColumnID column_index = 0; column_index < _groupby_columns.size(); ++column_index) {
      _output->add_column(_groupby_columns[column_index], input_table->column_type(groupby_column_ids[column_index]),
                          false);

      _group_columns.emplace_back(make_shared_by_column_type<BaseColumn, ValueColumn>(
          input_table->column_type(groupby_column_ids[column_index])));

      _out_chunk.add_column(_group_columns.back());
    }
  }

  /**
   * Write group-by columns.
   *
   * 'results_per_column' always contains at least one element, since there are either GroupBy or Aggregate columns.
   * However, we need to look only at the first element, because the keys for all columns are the same.
   *
   * The following loop is used for both, actual GroupBy columns and DISTINCT columns.
   **/
  if (_aggregates.empty()) {
    auto ctx = std::static_pointer_cast<AggregateContext<int32_t, int64_t>>(_contexts_per_column[0]);
    for (auto &map : *ctx->results) {
      for (size_t group_column_index = 0; group_column_index < map.first.size(); ++group_column_index) {
        _group_columns[group_column_index]->append(map.first[group_column_index]);
      }
    }
  }

  /*
  Write the aggregated columns to the output
  */
  ColumnID column_index = 0;
  for (auto aggregate : _aggregates) {
    auto column_id = _aggregate_column_ids[column_index];
    auto type_string = input_table_left()->column_type(column_id);

    call_functor_by_column_type<AggregateWriter>(type_string, *this, column_index, _aggregates[column_index].second);

    column_index++;
  }

  _output->add_chunk(std::move(_out_chunk));

  return _output;
}

template <typename ColumnType, AggregateFunction function>
void Aggregate::write_aggregate_output(ColumnID column_index) {
  auto &column_name = _aggregates[column_index].first;

  // retrieve type information from the aggregation traits
  typename aggregate_traits<ColumnType, function>::aggregate_type aggregate_type;
  std::string aggregate_type_name = std::string(aggregate_traits<ColumnType, function>::aggregate_type_name);

  if (aggregate_type_name.empty()) {
    // if not specified, it's the input column's type
    aggregate_type_name = input_table_left()->column_type(_aggregate_column_ids[column_index]);
  }

  // generate the name, e.g. MAX(column_a)
  std::vector<std::string> names{"MIN", "MAX", "SUM", "AVG", "COUNT"};
  _output->add_column(names[function] + "(" + column_name + ")", aggregate_type_name, false);

  auto col = std::make_shared<ValueColumn<decltype(aggregate_type)>>();
  auto &values = col->values();

  auto ctx = std::static_pointer_cast<AggregateContext<ColumnType, decltype(aggregate_type)>>(
      _contexts_per_column[column_index]);

  // write all group keys into the respective columns
  if (column_index == 0) {
    for (auto &map : *ctx->results) {
      for (size_t group_column_index = 0; group_column_index < map.first.size(); ++group_column_index) {
        _group_columns[group_column_index]->append(map.first[group_column_index]);
      }
    }
  }

  // write aggregated values into the column
  _write_aggregate_values<decltype(aggregate_type), function>(values, ctx->results);
  _out_chunk.add_column(col);
}

}  // namespace opossum
