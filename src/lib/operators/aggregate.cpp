#include "aggregate.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace opossum {

/*
Disable impossible ColumnType/AggregateType combinations to avoid compiler errors.
Unfortunately, I cannot use type_traits on constructors, hence the long list.
*/
template <>
AggregateBuilder<std::string, double>::AggregateBuilder(const AggregateFunction) {
  throw std::runtime_error("AggregateBuilder: string columns cannot have arithmetic aggregates");
}
template <>
AggregateBuilder<int, std::string>::AggregateBuilder(const AggregateFunction) {
  throw std::runtime_error("AggregateBuilder: number columns cannot have string aggregates");
}
template <>
AggregateBuilder<int64_t, std::string>::AggregateBuilder(const AggregateFunction) {
  throw std::runtime_error("AggregateBuilder: number columns cannot have string aggregates");
}
template <>
AggregateBuilder<double, std::string>::AggregateBuilder(const AggregateFunction) {
  throw std::runtime_error("AggregateBuilder: number columns cannot have string aggregates");
}
template <>
AggregateBuilder<float, std::string>::AggregateBuilder(const AggregateFunction) {
  throw std::runtime_error("AggregateBuilder: number columns cannot have string aggregates");
}

Aggregate::Aggregate(const std::shared_ptr<AbstractOperator> in,
                     const std::vector<std::pair<std::string, AggregateFunction>> aggregates,
                     const std::vector<std::string> groupby_columns)
    : AbstractReadOnlyOperator(in), _aggregates(aggregates), _groupby_columns(groupby_columns) {}

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
    auto column_type = input_table->column_type(_aggregate_column_ids[column_index]);
    if (column_type == "string") {
      _contexts_per_column[column_index] = std::make_shared<AggregateContext<std::string>>();
    } else {
      _contexts_per_column[column_index] = std::make_shared<AggregateContext<double>>();
    }
  }

  for (ChunkID chunk_id = 0; chunk_id < input_table->chunk_count(); ++chunk_id) {
    const Chunk &chunk_in = input_table->get_chunk(chunk_id);

    auto hash_keys = keys_per_chunk[chunk_id];

    ColumnID column_index = 0;
    for (auto column_id : _aggregate_column_ids) {
      auto base_column = chunk_in.get_column(column_id);
      auto column_type = input_table->column_type(column_id);

      /*
      Invoke the AggregateBuilder for each aggregate column
      */
      auto groupby_ctx = std::make_shared<GroupByContext>(input_table, chunk_id, column_id, hash_keys);

      std::shared_ptr<ColumnVisitable> builder;
      auto ctx = _contexts_per_column[column_index];

      if (column_type == "string") {
        builder = make_shared_by_column_type<ColumnVisitable, AggregateBuilder, std::string>(
            column_type, _aggregates[column_index].second);
        std::static_pointer_cast<AggregateContext<std::string>>(ctx)->groupby_context = groupby_ctx;
      } else {
        builder = make_shared_by_column_type<ColumnVisitable, AggregateBuilder, double>(
            column_type, _aggregates[column_index].second);
        std::static_pointer_cast<AggregateContext<double>>(ctx)->groupby_context = groupby_ctx;
      }

      base_column->visit(*builder, ctx);
      column_index++;
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

  /*
  Write the aggregated columns to the output
  */
  ColumnID column_index = 0;
  for (auto aggregate : _aggregates) {
    auto column_type = input_table_left()->column_type(_aggregate_column_ids[column_index]);

    if (column_type == "string") {
      _write_aggregate_output<std::string>(aggregate, column_index);
    } else {
      _write_aggregate_output<double>(aggregate, column_index);
    }

    column_index++;
  }

  _output->add_chunk(std::move(_out_chunk));

  return _output;
}

template <typename AggregateType>
void Aggregate::_write_aggregate_output(std::pair<std::string, AggregateFunction> aggregate, ColumnID column_index) {
  auto column_type = input_table_left()->column_type(_aggregate_column_ids[column_index]);
  auto &column_name = aggregate.first;
  auto &func = aggregate.second;

  std::string aggregate_type_name = "double";
  if (column_type == "string") {
    aggregate_type_name = "string";
  }

  // generate the name, e.g. MAX(column_a)
  std::vector<std::string> names{"MIN", "MAX", "SUM", "AVG"};
  _output->add_column(names[func] + "(" + column_name + ")", aggregate_type_name, false);

  auto col = std::make_shared<ValueColumn<AggregateType>>();
  auto &values = col->values();

  auto ctx = std::static_pointer_cast<AggregateContext<AggregateType>>(_contexts_per_column[column_index]);

  for (auto &kv : *ctx->results) {
    if (column_index == 0) {
      // in first iteration, also add the group key values
      for (size_t group_column_index = 0; group_column_index < kv.first.size(); ++group_column_index) {
        _group_columns[group_column_index]->append(kv.first[group_column_index]);
      }
    }

    if (!kv.second.current_aggregate) {
      // this needs to be NULL, as soon as that is implemented!
      values.push_back(0);
      continue;
    }

    if (func == Avg) {
      // finally calculate the average from the sum
      values.push_back(calc_average(*kv.second.current_aggregate, kv.second.aggregate_count));
    } else {
      values.push_back(*kv.second.current_aggregate);
    }
  }
  _out_chunk.add_column(col);
}

}  // namespace opossum
