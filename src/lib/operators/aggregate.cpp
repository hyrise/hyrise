#include "aggregate.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

AggregateDefinition::AggregateDefinition(const ColumnID column_id, const AggregateFunction function,
                                         const optional<std::string> &alias)
    : column_id(column_id), function(function), alias(alias) {}

Aggregate::Aggregate(const std::shared_ptr<AbstractOperator> in, const std::vector<AggregateDefinition> aggregates,
                     const std::vector<ColumnID> groupby_column_ids)
    : AbstractReadOnlyOperator(in), _aggregates(aggregates), _groupby_column_ids(groupby_column_ids) {
  Assert(!(aggregates.empty() && groupby_column_ids.empty()),
         "Neither aggregate nor groupby columns have been specified");
}

const std::vector<AggregateDefinition> &Aggregate::aggregates() const { return _aggregates; }

const std::vector<ColumnID> &Aggregate::groupby_column_ids() const { return _groupby_column_ids; }

const std::string Aggregate::name() const { return "Aggregate"; }

uint8_t Aggregate::num_in_tables() const { return 1; }

uint8_t Aggregate::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> Aggregate::recreate(const std::vector<AllParameterVariant> &args) const {
  return std::make_shared<Aggregate>(_input_left->recreate(args), _aggregates, _groupby_column_ids);
}

std::shared_ptr<const Table> Aggregate::_on_execute() {
  auto input_table = _input_table_left();

  // check for invalid aggregates
  for (const auto &aggregate : _aggregates) {
    if (aggregate.column_id == CountStarID) {
      if (aggregate.function != AggregateFunction::Count) {
        Fail("Aggregate: Asterisk is only valid with COUNT");
      }
    } else if (input_table->column_type(aggregate.column_id) == "string" &&
               (aggregate.function == AggregateFunction::Sum || aggregate.function == AggregateFunction::Avg)) {
      Fail("Aggregate: Cannot calculate SUM or AVG on string column");
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

  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id, this]() {
      const Chunk &chunk_in = input_table->get_chunk(chunk_id);

      auto hash_keys = std::make_shared<std::vector<AggregateKey>>(chunk_in.size());

      // Partition by group columns
      for (const auto column_id : _groupby_column_ids) {
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
  for (ColumnID column_index{0}; column_index < _contexts_per_column.size(); ++column_index) {
    auto column_id = _aggregates[column_index].column_id;
    auto function = _aggregates[column_index].function;

    const auto is_count_star_context = (column_id == CountStarID && function == AggregateFunction::Count);

    // Special COUNT(*) contexts. "int" is chosen arbitrarily.
    const auto type_string = is_count_star_context ? std::string{"int"} : input_table->column_type(column_id);

    resolve_data_type(type_string, [&, column_index, function](auto type) {
      this->_create_aggregate_context(type, _contexts_per_column[column_index], function);
    });
  }

  if (_aggregates.empty()) {
    /*
    Insert a dummy context for the DISTINCT implementation.
    That way, _contexts_per_column will always have at least one context with results.
    This is important later on when we write the group keys into the table.

    We choose int8_t for column type and aggregate type because it's small.
    */
    auto ctx = std::make_shared<AggregateContext<DistinctColumnType, DistinctAggregateType>>();
    ctx->results = std::make_shared<std::map<AggregateKey, AggregateResult<DistinctAggregateType>>>();

    _contexts_per_column.push_back(ctx);
  }

  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    const Chunk &chunk_in = input_table->get_chunk(chunk_id);

    auto hash_keys = keys_per_chunk[chunk_id];

    if (_aggregates.empty()) {
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

      auto ctx = std::static_pointer_cast<AggregateContext<DistinctColumnType, DistinctAggregateType>>(
          _contexts_per_column[0]);
      auto &results = *ctx->results;
      for (auto &chunk : keys_per_chunk) {
        for (auto &keys : *chunk) {
          // insert dummy value to make sure we have the key in our map
          results[keys] = AggregateResult<DistinctAggregateType>();
        }
      }
    } else {
      ColumnID column_index{0};
      for (const auto &aggregate : _aggregates) {
        /**
         * Special COUNT(*) implementation.
         * Because COUNT(*) does not have a specific target column, we use the maximum ColumnID.
         * We then basically go through the keys_per_chunk map and count the occurences of each group key.
         * The results are saved in the regular aggregate_count variable so that we don't need a
         * specific output logic for COUNT(*).
         */
        if (aggregate.column_id == CountStarID && aggregate.function == AggregateFunction::Count) {
          // We know the template arguments, so we don't need a visitor
          auto ctx = std::static_pointer_cast<AggregateContext<CountColumnType, CountAggregateType>>(
              _contexts_per_column[column_index]);

          if (!ctx->results) {
            // create result map for the first time if necessary
            ctx->results = std::make_shared<std::map<AggregateKey, AggregateResult<CountAggregateType>>>();
          }

          auto &results = *ctx->results;

          // count occurences for each group key
          for (const auto &hash_key : *hash_keys) {
            ++results[hash_key].aggregate_count;
          }

          ++column_index;
          continue;
        }

        auto base_column = chunk_in.get_column(aggregate.column_id);
        auto type_string = input_table->column_type(aggregate.column_id);

        /*
        Invoke the AggregateVisitor for each aggregate column
        */
        auto groupby_ctx = std::make_shared<GroupByContext>(input_table, chunk_id, aggregate.column_id, hash_keys);
        std::shared_ptr<ColumnVisitable> builder;
        auto ctx = _contexts_per_column[column_index];

        resolve_data_type(type_string, [&](auto type) {
          _create_aggregate_visitor(type, builder, ctx, groupby_ctx, aggregate.function);
        });

        base_column->visit(*builder, ctx);
        ++column_index;
      }
    }
  }

  // Write the output
  _output = std::make_shared<Table>();

  // add group by columns
  for (const auto column_id : _groupby_column_ids) {
    const auto &column_type = input_table->column_type(column_id);
    const auto &column_name = input_table->column_name(column_id);

    _output->add_column_definition(column_name, column_type, true);

    _groupby_columns.emplace_back(make_shared_by_column_type<BaseColumn, ValueColumn>(column_type, true));
    _out_chunk.add_column(_groupby_columns.back());
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
    auto ctx =
        std::static_pointer_cast<AggregateContext<DistinctColumnType, DistinctAggregateType>>(_contexts_per_column[0]);
    for (auto &map : *ctx->results) {
      for (size_t group_column_index = 0; group_column_index < map.first.size(); ++group_column_index) {
        _groupby_columns[group_column_index]->append(map.first[group_column_index]);
      }
    }
  }

  /*
  Write the aggregated columns to the output
  */
  ColumnID column_index{0};
  for (const auto &aggregate : _aggregates) {
    auto column_id = aggregate.column_id;

    // Output column for COUNT(*). "int" type is chosen arbitrarily.
    const auto type_string = (column_id == CountStarID) ? std::string{"int"} : input_table->column_type(column_id);

    resolve_data_type(type_string, [&, column_index](auto type) {
      this->_write_aggregate_output(type, column_index, aggregate.function);
    });

    ++column_index;
  }

  _output->add_chunk(std::move(_out_chunk));

  return _output;
}

void Aggregate::_on_cleanup() { _impl.reset(); }

template <typename ColumnType>
void Aggregate::_create_aggregate_context(boost::hana::basic_type<ColumnType> type,
                                          std::shared_ptr<ColumnVisitableContext> &aggregate_context,
                                          AggregateFunction function) {
  switch (function) {
    case AggregateFunction::Min:
      aggregate_context = make_aggregate_context<ColumnType, AggregateFunction::Min>();
      break;
    case AggregateFunction::Max:
      aggregate_context = make_aggregate_context<ColumnType, AggregateFunction::Max>();
      break;
    case AggregateFunction::Sum:
      aggregate_context = make_aggregate_context<ColumnType, AggregateFunction::Sum>();
      break;
    case AggregateFunction::Avg:
      aggregate_context = make_aggregate_context<ColumnType, AggregateFunction::Avg>();
      break;
    case AggregateFunction::Count:
      aggregate_context = make_aggregate_context<ColumnType, AggregateFunction::Count>();
      break;
  }
}

template <typename ColumnType>
void Aggregate::_create_aggregate_visitor(boost::hana::basic_type<ColumnType> type,
                                          std::shared_ptr<ColumnVisitable> &builder,
                                          std::shared_ptr<ColumnVisitableContext> ctx,
                                          std::shared_ptr<GroupByContext> groupby_ctx, AggregateFunction function) {
  switch (function) {
    case AggregateFunction::Min:
      builder = make_aggregate_visitor<ColumnType, AggregateFunction::Min>(ctx, groupby_ctx);
      break;
    case AggregateFunction::Max:
      builder = make_aggregate_visitor<ColumnType, AggregateFunction::Max>(ctx, groupby_ctx);
      break;
    case AggregateFunction::Sum:
      builder = make_aggregate_visitor<ColumnType, AggregateFunction::Sum>(ctx, groupby_ctx);
      break;
    case AggregateFunction::Avg:
      builder = make_aggregate_visitor<ColumnType, AggregateFunction::Avg>(ctx, groupby_ctx);
      break;
    case AggregateFunction::Count:
      builder = make_aggregate_visitor<ColumnType, AggregateFunction::Count>(ctx, groupby_ctx);
      break;
  }
}

template <typename ColumnType>
void Aggregate::_write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                                        AggregateFunction function) {
  switch (function) {
    case AggregateFunction::Min:
      write_aggregate_output<ColumnType, AggregateFunction::Min>(column_index);
      break;
    case AggregateFunction::Max:
      write_aggregate_output<ColumnType, AggregateFunction::Max>(column_index);
      break;
    case AggregateFunction::Sum:
      write_aggregate_output<ColumnType, AggregateFunction::Sum>(column_index);
      break;
    case AggregateFunction::Avg:
      write_aggregate_output<ColumnType, AggregateFunction::Avg>(column_index);
      break;
    case AggregateFunction::Count:
      write_aggregate_output<ColumnType, AggregateFunction::Count>(column_index);
      break;
  }
}

template <typename ColumnType, AggregateFunction function>
void Aggregate::write_aggregate_output(ColumnID column_index) {
  // retrieve type information from the aggregation traits
  typename AggregateTraits<ColumnType, function>::aggregate_type aggregate_type;
  std::string aggregate_type_name = std::string(AggregateTraits<ColumnType, function>::aggregate_type_name);

  const auto &aggregate = _aggregates[column_index];

  if (aggregate_type_name.empty()) {
    // if not specified, it's the input column's type
    aggregate_type_name = _input_table_left()->column_type(aggregate.column_id);
  }

  // use the alias or generate the name, e.g. MAX(column_a)
  std::string output_column_name;
  if (aggregate.alias) {
    output_column_name = *aggregate.alias;
  } else if (aggregate.column_id == CountStarID) {
    output_column_name = "COUNT(*)";
  } else {
    const auto &column_name = _input_table_left()->column_name(aggregate.column_id);
    output_column_name = aggregate_function_to_string.left.at(function) + "(" + column_name + ")";
  }

  constexpr bool needs_null = (function != AggregateFunction::Count);
  _output->add_column_definition(output_column_name, aggregate_type_name, needs_null);

  auto col = std::make_shared<ValueColumn<decltype(aggregate_type)>>(needs_null);

  auto ctx = std::static_pointer_cast<AggregateContext<ColumnType, decltype(aggregate_type)>>(
      _contexts_per_column[column_index]);

  // write all group keys into the respective columns
  if (column_index == 0) {
    for (auto &map : *ctx->results) {
      for (size_t group_column_index = 0; group_column_index < map.first.size(); ++group_column_index) {
        _groupby_columns[group_column_index]->append(map.first[group_column_index]);
      }
    }
  }

  // write aggregated values into the column
  _write_aggregate_values<decltype(aggregate_type), function>(col, ctx->results);
  _out_chunk.add_column(col);
}

}  // namespace opossum
