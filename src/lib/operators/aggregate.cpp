#include "aggregate.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"

namespace opossum {

// disable string columns for aggregate functions
template <>
AggregateBuilder<std::string>::AggregateBuilder(const AggregateFunction) {
  throw std::runtime_error("Cannot use string columns in aggregates");
}

Aggregate::Aggregate(const std::shared_ptr<AbstractOperator> in,
                     const std::vector<std::pair<std::string, AggregateFunction>> aggregates,
                     const std::vector<std::string> groupby_columns)
    : AbstractReadOnlyOperator(in), _aggregates(aggregates), _groupby_columns(groupby_columns) {
  if (aggregates.empty() && groupby_columns.empty()) {
    throw std::runtime_error("Neither aggregate nor groupby columns have been specified");
  }
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
  std::vector<ColumnID> aggregate_column_ids;
  std::transform(
      _aggregates.begin(), _aggregates.end(), std::back_inserter(aggregate_column_ids),
      [&](std::pair<std::string, AggregateFunction> pair) { return input_table->column_id_by_name(pair.first); });

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
        auto ctx = std::make_shared<AggregateContext>(input_table, chunk_id, column_id, hash_keys);
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
  auto results_per_column = std::vector<std::shared_ptr<std::map<AggregateKey, AggregateResult>>>(_aggregates.size());

  // pre-insert empty maps for each aggregate column
  for (ColumnID column_index = 0; column_index < results_per_column.size(); ++column_index) {
    results_per_column[column_index] = std::make_shared<std::map<AggregateKey, AggregateResult>>();
  }

  /*
  define the functions that are later used to combine the aggregation results
  from the different chunks.
  */
  std::vector<std::function<double(double, double)>> combine_functions;
  for (auto &kv : _aggregates) {
    switch (kv.second) {
      case Min:
        combine_functions.emplace_back([](double new_value, double current_aggregate) {
          if (std::isnan(current_aggregate)) {
            return new_value;
          }
          return (new_value < current_aggregate) ? new_value : current_aggregate;
        });
        break;

      case Max:
        combine_functions.emplace_back([](double new_value, double current_aggregate) {
          if (std::isnan(current_aggregate)) {
            return new_value;
          }
          return (new_value > current_aggregate) ? new_value : current_aggregate;
        });
        break;

      case Sum:
      case Avg:
      case Count:
        combine_functions.emplace_back([](double new_value, double current_aggregate) {
          if (std::isnan(current_aggregate)) {
            return new_value;
          }
          return current_aggregate + new_value;
        });
        break;

      default:
        throw std::runtime_error("Aggregate: invalid aggregate function");
    }
  }

  for (ChunkID chunk_id = 0; chunk_id < input_table->chunk_count(); ++chunk_id) {
    const Chunk &chunk_in = input_table->get_chunk(chunk_id);

    auto hash_keys = keys_per_chunk[chunk_id];

    if (aggregate_column_ids.empty()) {
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

      std::map<AggregateKey, AggregateResult> column_results;
      for (auto &chunk : keys_per_chunk) {
        for (auto &keys : *chunk) {
          // insert dummy value to make sure we have the key in our map
          column_results[keys] = std::make_pair(0, 0);
        }
      }
      results_per_column.emplace_back(std::make_shared<std::map<AggregateKey, AggregateResult>>(column_results));
    } else {
      ColumnID column_index = 0;
      for (auto column_id : aggregate_column_ids) {
        auto base_column = chunk_in.get_column(column_id);
        auto column_type = input_table->column_type(column_id);

        auto results = std::make_shared<std::map<AggregateKey, AggregateResult>>();

        /*
        Invoke the AggregateBuilder for each aggregate column
        */
        auto builder = make_shared_by_column_type<ColumnVisitable, AggregateBuilder>(column_type,
                                                                                     _aggregates[column_index].second);
        auto ctx = std::make_shared<AggregateContext>(input_table, chunk_id, column_id, hash_keys, results);
        base_column->visit(*builder, ctx);

        auto &column_results =
            static_cast<std::map<AggregateKey, AggregateResult> &>(*results_per_column[column_index]);

        /*
        Combine the results from this chunk with previous results for this column
        */
        for (auto &kv : *results) {
          auto hash = kv.first;
          auto result_value = kv.second.first;
          auto result_counter = kv.second.second;

          column_results[hash].first = combine_functions[column_index](result_value, column_results[hash].first);
          column_results[hash].second += result_counter;
        }

        column_index++;
      }
    }
  }

  // Write the output
  auto output = std::make_shared<Table>();
  Chunk out_chunk;
  std::vector<std::shared_ptr<BaseColumn>> group_columns;

  if (_groupby_columns.size()) {
    // add group by columns
    for (ColumnID column_index = 0; column_index < _groupby_columns.size(); ++column_index) {
      output->add_column(_groupby_columns[column_index], input_table->column_type(groupby_column_ids[column_index]),
                         false);

      group_columns.emplace_back(make_shared_by_column_type<BaseColumn, ValueColumn>(
          input_table->column_type(groupby_column_ids[column_index])));

      out_chunk.add_column(group_columns.back());
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
  for (auto &map : *results_per_column[0]) {
    for (size_t group_column_index = 0; group_column_index < map.first.size(); ++group_column_index) {
      group_columns[group_column_index]->append(map.first[group_column_index]);
    }
  }

  /*
  Write the aggregated columns to the output
  */
  ColumnID column_index = 0;
  for (auto aggregate : _aggregates) {
    auto &column_name = aggregate.first;
    auto &func = aggregate.second;

    // generate the name, e.g. MAX(column_a)
    std::vector<std::string> names{"MIN", "MAX", "SUM", "AVG", "COUNT"};
    output->add_column(names[func] + "(" + column_name + ")", "double", false);

    auto col = std::make_shared<ValueColumn<double>>();
    auto &values = col->values();

    for (auto &kv : *results_per_column[column_index]) {
      if (func == Avg) {
        // finally calculate the average from the sum
        values.push_back(kv.second.first / kv.second.second);
      } else if (func == Count) {
        values.push_back(kv.second.second);
      } else {
        values.push_back(kv.second.first);
      }
    }
    out_chunk.add_column(col);

    column_index++;
  }

  output->add_chunk(std::move(out_chunk));

  return output;
}

}  // namespace opossum
