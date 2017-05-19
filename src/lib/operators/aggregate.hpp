#pragma once

#include <algorithm>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "type_comparison.hpp"
#include "types.hpp"

namespace opossum {

enum AggregateFunction { Min, Max, Sum, Avg };

/*
Operator to aggregate columns by certain functions, such as min, max, sum, or average. The output is a table with
reference columns. As with most operators we do not guarantee a stable operation with regards to positions - i.e. your
sorting order.
Current Limitations (due to lack of time)
 - we cannot aggregate on string columns (they work for GROUP BY, though)
 - aggregated columns are always type double (connected with the point above)
*/

class Aggregate : public AbstractReadOnlyOperator {
 public:
  Aggregate(const std::shared_ptr<AbstractOperator> in,
            const std::vector<std::pair<std::string, AggregateFunction>> aggregates,
            const std::vector<std::string> groupby_columns);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  template <typename AggregateType>
  void _write_aggregate_output(std::pair<std::string, AggregateFunction> aggregate, ColumnID column_index);

  const std::vector<std::pair<std::string, AggregateFunction>> _aggregates;
  const std::vector<std::string> _groupby_columns;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;

  std::shared_ptr<Table> _output;
  Chunk _out_chunk;
  std::vector<std::shared_ptr<BaseColumn>> _group_columns;
  std::vector<std::shared_ptr<ColumnVisitableContext>> _contexts_per_column;
  std::vector<ColumnID> _aggregate_column_ids;
};

// this value indicates that no aggregate was calculated yet
constexpr double invalid_aggregate = std::numeric_limits<double>::quiet_NaN();

/*
Essentially, this is a `double` that has `invalid_aggregate` as
its default value. This is necessary to support MIN and MAX.
The implicit constructor allows automatic conversion with double.
*/
class AggregateDouble {
 public:
  AggregateDouble() {}
  AggregateDouble(const double &rhs) : _value(rhs) {}  // NOLINT
  operator double() const { return _value; }

 protected:
  double _value = invalid_aggregate;
};

/*
Current aggregated value and the number of rows that were used.
The latter is used for AVG.
*/
template <typename T>
class AggregateResult {
 public:
  AggregateResult() {}

  optional<T> current_aggregate;
  size_t aggregate_count = 0;
};

/*
Type-based average calculation.
*/
template <typename T>
typename std::enable_if<std::is_arithmetic<T>::value, double>::type calc_average(T sum, size_t count) {
  return sum / static_cast<double>(count);
}

/*
There's no average for strings. Just return an empty string for now.
This should be NULL, when it is implemented!
*/
template <typename T>
typename std::enable_if<!std::is_arithmetic<T>::value, std::string>::type calc_average(T, size_t) {
  return "";
}

/*
The key type that is used for the aggregation map.
*/
using AggregateKey = std::vector<AllTypeVariant>;

/*
Visitor context for the partitioning/grouping visitor
*/
struct GroupByContext : ColumnVisitableContext {
  GroupByContext(std::shared_ptr<const Table> t, ChunkID chunk, ColumnID column,
                 std::shared_ptr<std::vector<AggregateKey>> keys)
      : table_in(t), chunk_id(chunk), column_id(column), hash_keys(keys) {}

  // constructor for use in ReferenceColumn::visit_dereferenced
  GroupByContext(std::shared_ptr<BaseColumn>, const std::shared_ptr<const Table> referenced_table,
                 std::shared_ptr<ColumnVisitableContext> base_context, ChunkID chunk_id,
                 std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets)
      : table_in(referenced_table),
        chunk_id(chunk_id),
        column_id(std::static_pointer_cast<GroupByContext>(base_context)->column_id),
        hash_keys(std::static_pointer_cast<GroupByContext>(base_context)->hash_keys),
        chunk_offsets_in(chunk_offsets) {}

  std::shared_ptr<const Table> table_in;
  const ChunkID chunk_id;
  const ColumnID column_id;
  std::shared_ptr<std::vector<AggregateKey>> hash_keys;
  std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets_in;
};

template <typename AggregateType>
struct AggregateContext : ColumnVisitableContext {
  AggregateContext() {}
  explicit AggregateContext(std::shared_ptr<GroupByContext> base_context) : groupby_context(base_context) {}

  // constructor for use in ReferenceColumn::visit_dereferenced
  AggregateContext(std::shared_ptr<BaseColumn>, const std::shared_ptr<const Table>,
                   std::shared_ptr<ColumnVisitableContext> base_context, ChunkID,
                   std::shared_ptr<std::vector<ChunkOffset>>)
      : groupby_context(std::static_pointer_cast<AggregateContext>(base_context)->groupby_context),
        results(std::static_pointer_cast<AggregateContext>(base_context)->results) {}

  std::shared_ptr<GroupByContext> groupby_context;
  std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType>>> results;
};

/*
Visitor for the partitioning phase.
It is used to partition the input by the given group key(s)
*/
template <typename T>
struct PartitionBuilder : public ColumnVisitable {
  std::vector<std::function<double(T, double)>> aggregators;

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<GroupByContext>(base_context);
    const auto &column = static_cast<ValueColumn<T> &>(base_column);
    const auto &values = column.values();

    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position
      for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
        (*context->hash_keys)[offset_in_value_column].emplace_back(values[offset_in_value_column]);
      }
    } else {
      ChunkOffset chunk_offset = 0;
      for (const auto &value : values) {
        (*context->hash_keys)[chunk_offset].emplace_back(value);
        chunk_offset++;
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) {
    column.visit_dereferenced<GroupByContext>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<GroupByContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<T> &>(base_column);
    const BaseAttributeVector &attribute_vector = *(column.attribute_vector());
    const std::vector<T> &dictionary = *(column.dictionary());

    if (context->chunk_offsets_in) {
      for (const ChunkOffset &offset_in_dictionary_column : *(context->chunk_offsets_in)) {
        (*context->hash_keys)[offset_in_dictionary_column].emplace_back(
            dictionary[attribute_vector.get(offset_in_dictionary_column)]);
      }
    } else {
      // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching
      // rows.
      for (ChunkOffset chunk_offset = 0; chunk_offset < column.size(); ++chunk_offset) {
        (*context->hash_keys)[chunk_offset].emplace_back(dictionary[attribute_vector.get(chunk_offset)]);
      }
    }
  }
};

/*
Visitor for the aggregation phase.
It is used to gradually build the given aggregate over one column.
*/
template <typename ColumnType, typename AggregateType>
struct AggregateBuilder : public ColumnVisitable {
  std::function<optional<AggregateType>(ColumnType, optional<AggregateType>)> aggregate_func;

  explicit AggregateBuilder(const AggregateFunction aggregate) {
    switch (aggregate) {
      case Min:
        aggregate_func = [](ColumnType new_value, optional<AggregateType> current_aggregate) {
          return (!current_aggregate || value_smaller(new_value, *current_aggregate)) ? new_value : *current_aggregate;
        };
        break;

      case Max:
        aggregate_func = [](ColumnType new_value, optional<AggregateType> current_aggregate) {
          return (!current_aggregate || value_greater(new_value, *current_aggregate)) ? new_value : *current_aggregate;
        };
        break;

      case Sum:
        aggregate_func = [](ColumnType new_value, optional<AggregateType> current_aggregate) {
          return new_value + (!current_aggregate ? 0 : *current_aggregate);
        };
        break;

      case Avg:
        aggregate_func = [](ColumnType new_value, optional<AggregateType> current_aggregate) {
          return new_value + (!current_aggregate ? 0 : *current_aggregate);
        };
        break;

      default:
        throw std::runtime_error("AggregateBuilder: invalid aggregate function");
    }
  }

  /*
  This will check if the results map has been created yet.
  If not, it will be created with the correct AggregateType.
  */
  void check_and_init_context(std::shared_ptr<AggregateContext<AggregateType>> context) {
    if (!context->results) {
      context->results = std::make_shared<std::map<AggregateKey, AggregateResult<AggregateType>>>();
    }
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext<AggregateType>>(base_context);
    check_and_init_context(context);
    const auto &column = static_cast<ValueColumn<ColumnType> &>(base_column);
    const auto &values = column.values();

    auto &hash_keys = static_cast<std::vector<AggregateKey> &>(*context->groupby_context->hash_keys);
    auto &results = static_cast<std::map<AggregateKey, AggregateResult<AggregateType>> &>(*context->results);

    if (context->groupby_context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position
      for (const ChunkOffset &offset_in_value_column : *(context->groupby_context->chunk_offsets_in)) {
        results[hash_keys[offset_in_value_column]].current_aggregate = aggregate_func(
            values[offset_in_value_column], results[hash_keys[offset_in_value_column]].current_aggregate);

        // increase value counter
        results[hash_keys[offset_in_value_column]].aggregate_count++;
      }
    } else {
      ChunkOffset chunk_offset = 0;
      for (const auto &value : values) {
        results[hash_keys[chunk_offset]].current_aggregate =
            aggregate_func(value, results[hash_keys[chunk_offset]].current_aggregate);

        // increase value counter
        results[hash_keys[chunk_offset]].aggregate_count++;
        chunk_offset++;
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext<AggregateType>>(base_context);
    check_and_init_context(context);
    column.visit_dereferenced<AggregateContext<AggregateType>>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext<AggregateType>>(base_context);
    check_and_init_context(context);
    const auto &column = static_cast<DictionaryColumn<ColumnType> &>(base_column);
    const BaseAttributeVector &attribute_vector = *(column.attribute_vector());
    const std::vector<ColumnType> &dictionary = *(column.dictionary());

    auto &hash_keys = static_cast<std::vector<AggregateKey> &>(*context->groupby_context->hash_keys);
    auto &results = static_cast<std::map<AggregateKey, AggregateResult<AggregateType>> &>(*context->results);

    if (context->groupby_context->chunk_offsets_in) {
      for (const ChunkOffset &offset_in_dictionary_column : *(context->groupby_context->chunk_offsets_in)) {
        results[hash_keys[offset_in_dictionary_column]].current_aggregate =
            aggregate_func(dictionary[attribute_vector.get(offset_in_dictionary_column)],
                           results[hash_keys[offset_in_dictionary_column]].current_aggregate);

        // increase value counter
        results[hash_keys[offset_in_dictionary_column]].aggregate_count++;
      }
    } else {
      // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching
      // rows.
      for (ChunkOffset chunk_offset = 0; chunk_offset < column.size(); ++chunk_offset) {
        results[hash_keys[chunk_offset]].current_aggregate = aggregate_func(
            dictionary[attribute_vector.get(chunk_offset)], results[hash_keys[chunk_offset]].current_aggregate);

        // increase value counter
        results[hash_keys[chunk_offset]].aggregate_count++;
      }
    }
  }
};

}  // namespace opossum
