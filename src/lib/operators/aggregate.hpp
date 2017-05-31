#pragma once

#include <algorithm>
#include <cmath>
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
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

enum AggregateFunction { Min, Max, Sum, Avg, Count };

/*
Operator to aggregate columns by certain functions, such as min, max, sum, average, and count. The output is a table
 with reference columns. As with most operators we do not guarantee a stable operation with regards to positions -
 i.e. your sorting order.
Current Limitations (due to lack of time)
 - we cannot aggregate on string columns (they work for GROUP BY, though)
 - aggregated columns are always type double (connected with the point above)
*/

class Aggregate : public AbstractReadOnlyOperator {
 public:
  Aggregate(const std::shared_ptr<AbstractOperator> in,
            const alloc_vector<std::pair<std::string, AggregateFunction>> aggregates,
            const alloc_vector<std::string> groupby_columns);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  const alloc_vector<std::pair<std::string, AggregateFunction>> _aggregates;
  const alloc_vector<std::string> _groupby_columns;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
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
The latter is used for AVG and COUNT.
*/
using AggregateResult = std::pair<AggregateDouble, size_t>;

/*
The key type that is used for the aggregation map.
*/
using AggregateKey = alloc_vector<AllTypeVariant>;

/*
Visitor context for both visitors that are used
*/
struct AggregateContext : ColumnVisitableContext {
  AggregateContext(std::shared_ptr<const Table> t, ChunkID chunk, ColumnID column,
                   std::shared_ptr<alloc_vector<AggregateKey>> keys,
                   std::shared_ptr<std::map<AggregateKey, AggregateResult>> res = nullptr)
      : table_in(t), chunk_id(chunk), column_id(column), hash_keys(keys), results(res) {}

  // constructor for use in ReferenceColumn::visit_dereferenced
  AggregateContext(std::shared_ptr<BaseColumn>, const std::shared_ptr<const Table> referenced_table,
                   std::shared_ptr<ColumnVisitableContext> base_context, ChunkID chunk_id,
                   std::shared_ptr<alloc_vector<ChunkOffset>> chunk_offsets)
      : table_in(referenced_table),
        chunk_id(chunk_id),
        column_id(std::static_pointer_cast<AggregateContext>(base_context)->column_id),
        hash_keys(std::static_pointer_cast<AggregateContext>(base_context)->hash_keys),
        results(std::static_pointer_cast<AggregateContext>(base_context)->results),
        chunk_offsets_in(chunk_offsets) {}

  std::shared_ptr<const Table> table_in;
  const ChunkID chunk_id;
  const ColumnID column_id;
  std::shared_ptr<alloc_vector<AggregateKey>> hash_keys;
  std::shared_ptr<std::map<AggregateKey, AggregateResult>> results;
  std::shared_ptr<alloc_vector<ChunkOffset>> chunk_offsets_in;
};

/*
Visitor for the partitioning phase.
It is used to partition the input by the given group key(s)
*/
template <typename T>
struct PartitionBuilder : public ColumnVisitable {
  alloc_vector<std::function<double(T, double)>> aggregators;

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext>(base_context);
    const auto &column = static_cast<ValueColumn<T> &>(base_column);
    const auto &values = column.values();

    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position
      ChunkOffset chunk_offset = 0;
      for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
        (*context->hash_keys)[chunk_offset].emplace_back(values[offset_in_value_column]);
        chunk_offset++;
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
    column.visit_dereferenced<AggregateContext>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<T> &>(base_column);
    const BaseAttributeVector &attribute_vector = *(column.attribute_vector());
    const alloc_vector<T> &dictionary = *(column.dictionary());

    if (context->chunk_offsets_in) {
      ChunkOffset chunk_offset = 0;
      for (const ChunkOffset &offset_in_dictionary_column : *(context->chunk_offsets_in)) {
        (*context->hash_keys)[chunk_offset].emplace_back(dictionary[attribute_vector.get(offset_in_dictionary_column)]);
        chunk_offset++;
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
template <typename T>
struct AggregateBuilder : public ColumnVisitable {
  std::function<double(T, double)> aggregate_func;

  explicit AggregateBuilder(const AggregateFunction aggregate) {
    switch (aggregate) {
      case Min:
        aggregate_func = [](T new_value, double current_aggregate) {
          return (new_value < current_aggregate || std::isnan(current_aggregate)) ? new_value : current_aggregate;
        };
        break;

      case Max:
        aggregate_func = [](T new_value, double current_aggregate) {
          return (new_value > current_aggregate || std::isnan(current_aggregate)) ? new_value : current_aggregate;
        };
        break;

      case Sum:
      case Avg:
      case Count:
        aggregate_func = [](T new_value, double current_aggregate) {
          return new_value + (std::isnan(current_aggregate) ? 0 : current_aggregate);
        };
        break;

      default:
        Fail("AggregateBuilder: invalid aggregate function");
    }
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext>(base_context);
    const auto &column = static_cast<ValueColumn<T> &>(base_column);
    const auto &values = column.values();

    auto &hash_keys = static_cast<alloc_vector<AggregateKey> &>(*context->hash_keys);
    auto &results = static_cast<std::map<AggregateKey, AggregateResult> &>(*context->results);

    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position
      ChunkOffset chunk_offset = 0;
      for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
        results[hash_keys[chunk_offset]].first =
            aggregate_func(values[offset_in_value_column], results[hash_keys[chunk_offset]].first);

        // increase value counter
        results[hash_keys[chunk_offset]].second++;
        chunk_offset++;
      }
    } else {
      ChunkOffset chunk_offset = 0;
      for (const auto &value : values) {
        results[hash_keys[chunk_offset]].first = aggregate_func(value, results[hash_keys[chunk_offset]].first);

        // increase value counter
        results[hash_keys[chunk_offset]].second++;
        chunk_offset++;
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) {
    column.visit_dereferenced<AggregateContext>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<T> &>(base_column);
    const BaseAttributeVector &attribute_vector = *(column.attribute_vector());
    const alloc_vector<T> &dictionary = *(column.dictionary());

    auto &hash_keys = static_cast<alloc_vector<AggregateKey> &>(*context->hash_keys);
    auto &results = static_cast<std::map<AggregateKey, AggregateResult> &>(*context->results);

    if (context->chunk_offsets_in) {
      ChunkOffset chunk_offset = 0;
      for (const ChunkOffset &offset_in_dictionary_column : *(context->chunk_offsets_in)) {
        results[hash_keys[chunk_offset]].first = aggregate_func(
            dictionary[attribute_vector.get(offset_in_dictionary_column)], results[hash_keys[chunk_offset]].first);

        // increase value counter
        results[hash_keys[chunk_offset]].second++;
        chunk_offset++;
      }
    } else {
      // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching
      // rows.
      for (ChunkOffset chunk_offset = 0; chunk_offset < column.size(); ++chunk_offset) {
        results[hash_keys[chunk_offset]].first =
            aggregate_func(dictionary[attribute_vector.get(chunk_offset)], results[hash_keys[chunk_offset]].first);

        // increase value counter
        results[hash_keys[chunk_offset]].second++;
      }
    }
  }
};

}  // namespace opossum
