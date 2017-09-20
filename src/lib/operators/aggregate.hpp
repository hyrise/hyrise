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
#include "storage/base_attribute_vector.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "resolve_type.hpp"
#include "type_comparison.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

struct GroupByContext;

/*
Operator to aggregate columns by certain functions, such as min, max, sum, average, and count. The output is a table
 with reference columns. As with most operators we do not guarantee a stable operation with regards to positions -
 i.e. your sorting order.

For implementation details, please check the wiki: https://github.com/hyrise/zweirise/wiki/Aggregate-Operator
*/

/*
Current aggregated value and the number of rows that were used.
The latter is used for AVG and COUNT.
*/
template <typename T>
class AggregateResult {
 public:
  AggregateResult() {}

  optional<T> current_aggregate;
  size_t aggregate_count = 0;
};

/*
The key type that is used for the aggregation map.
*/
using AggregateKey = std::vector<AllTypeVariant>;

// ColumnID representing the '*' when using COUNT(*)
constexpr ColumnID CountStarID{std::numeric_limits<ColumnID::base_type>::max()};

/**
 * Struct to specify aggregates.
 * Aggregates are defined by the column_id they operate on and the aggregate function they use.
 * Optionally, an alias can be specified to use as the output name.
 */
struct AggregateDefinition {
  AggregateDefinition(const ColumnID column_id, const AggregateFunction function,
                      const optional<std::string> &alias = nullopt);

  ColumnID column_id;
  AggregateFunction function;
  optional<std::string> alias;
};

/**
 * Types that are used for the special COUNT(*) and DISTINCT implementations
 */
using CountColumnType = int32_t;
using CountAggregateType = int64_t;
using DistinctColumnType = int8_t;
using DistinctAggregateType = int8_t;

/**
 * Note: Aggregate does not support null values at the moment
 */
class Aggregate : public AbstractReadOnlyOperator {
 public:
  Aggregate(const std::shared_ptr<AbstractOperator> in, const std::vector<AggregateDefinition> aggregates,
            const std::vector<ColumnID> groupby_column_ids);

  const std::vector<AggregateDefinition> &aggregates() const;
  const std::vector<ColumnID> &groupby_column_ids() const;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override;

  // write the aggregated output for a given aggregate column
  template <typename ColumnType, AggregateFunction function>
  void write_aggregate_output(ColumnID column_index);

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;

  template <typename ColumnType>
  static void _create_aggregate_context(boost::hana::basic_type<ColumnType> type,
                                        std::shared_ptr<ColumnVisitableContext> &aggregate_context,
                                        AggregateFunction function);

  template <typename ColumnType>
  static void _create_aggregate_visitor(boost::hana::basic_type<ColumnType> type,
                                        std::shared_ptr<ColumnVisitable> &builder,
                                        std::shared_ptr<ColumnVisitableContext> ctx,
                                        std::shared_ptr<GroupByContext> groupby_ctx, AggregateFunction function);

  template <typename ColumnType>
  void _write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                               AggregateFunction function);

  /*
  The following template functions write the aggregated values for the different aggregate functions.
  They are separate and templated to avoid compiler errors for invalid type/function combinations.
  */
  // MIN, MAX, SUM write the current aggregated value
  template <typename AggregateType, AggregateFunction func>
  typename std::enable_if<
      func == AggregateFunction::Min || func == AggregateFunction::Max || func == AggregateFunction::Sum, void>::type
  _write_aggregate_values(std::shared_ptr<ValueColumn<AggregateType>> column,
                          std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType>>> results) {
    DebugAssert(column->is_nullable(), "Aggregate: Output column needs to be nullable");

    auto &values = column->values();
    auto &null_values = column->null_values();

    for (auto &kv : *results) {
      null_values.push_back(!kv.second.current_aggregate);

      if (!kv.second.current_aggregate) {
        values.push_back(AggregateType());
      } else {
        values.push_back(*kv.second.current_aggregate);
      }
    }
  }

  // COUNT writes the aggregate counter
  template <typename AggregateType, AggregateFunction func>
  typename std::enable_if<func == AggregateFunction::Count, void>::type _write_aggregate_values(
      std::shared_ptr<ValueColumn<AggregateType>> column,
      std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType>>> results) {
    DebugAssert(!column->is_nullable(), "Aggregate: Output column for COUNT shouldn't be nullable");

    auto &values = column->values();

    for (auto &kv : *results) {
      values.push_back(kv.second.aggregate_count);
    }
  }

  // AVG writes the calculated average from current aggregate and the aggregate counter
  template <typename AggregateType, AggregateFunction func>
  typename std::enable_if<func == AggregateFunction::Avg && std::is_arithmetic<AggregateType>::value, void>::type
  _write_aggregate_values(std::shared_ptr<ValueColumn<AggregateType>> column,
                          std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType>>> results) {
    DebugAssert(column->is_nullable(), "Aggregate: Output column needs to be nullable");

    auto &values = column->values();
    auto &null_values = column->null_values();

    for (auto &kv : *results) {
      null_values.push_back(!kv.second.current_aggregate);

      if (!kv.second.current_aggregate) {
        values.push_back(AggregateType());
      } else {
        values.push_back(*kv.second.current_aggregate / static_cast<AggregateType>(kv.second.aggregate_count));
      }
    }
  }

  // AVG is not defined for non-arithmetic types. Avoiding compiler errors.
  template <typename AggregateType, AggregateFunction func>
  typename std::enable_if<func == AggregateFunction::Avg && !std::is_arithmetic<AggregateType>::value, void>::type
  _write_aggregate_values(std::shared_ptr<ValueColumn<AggregateType>>,
                          std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType>>>) {
    Fail("Invalid aggregate");
  }

  const std::vector<AggregateDefinition> _aggregates;
  const std::vector<ColumnID> _groupby_column_ids;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;

  std::shared_ptr<Table> _output;
  Chunk _out_chunk;
  std::vector<std::shared_ptr<BaseColumn>> _groupby_columns;
  std::vector<std::shared_ptr<ColumnVisitableContext>> _contexts_per_column;
};

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
  ChunkID chunk_id;
  const ColumnID column_id;
  std::shared_ptr<std::vector<AggregateKey>> hash_keys;
  std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets_in;
};

/*
Visitor for the partitioning phase.
It is used to partition the input by the given group key(s)
*/
template <typename T>
struct PartitionBuilder : public ColumnVisitable {
  PartitionBuilder() : chunk_offset(0) {}

  /*
  The builder saves the current position in its hash_keys vector.
  This is crucial to support ReferenceColumns with multiple chunks.
  */
  ChunkOffset chunk_offset;

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<GroupByContext>(base_context);
    const auto &column = static_cast<ValueColumn<T> &>(base_column);
    const auto &values = column.values();

    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position

      if (column.is_nullable()) {
        const auto &null_values = column.null_values();

        for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
          if (null_values[offset_in_value_column]) {
            (*context->hash_keys)[chunk_offset].emplace_back(NULL_VALUE);
          } else {
            (*context->hash_keys)[chunk_offset].emplace_back(values[offset_in_value_column]);
          }

          ++chunk_offset;
        }
      } else {
        for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
          if (offset_in_value_column == INVALID_CHUNK_OFFSET) {
            (*context->hash_keys)[chunk_offset].emplace_back(NULL_VALUE);
          } else {
            (*context->hash_keys)[chunk_offset].emplace_back(values[offset_in_value_column]);
          }
          ++chunk_offset;
        }
      }
    } else {
      if (column.is_nullable()) {
        const auto &null_values = column.null_values();

        auto value_it = values.cbegin();
        auto null_value_it = null_values.cbegin();

        for (; value_it != values.cend(); ++value_it, ++null_value_it, ++chunk_offset) {
          if (*null_value_it) {
            (*context->hash_keys)[chunk_offset].emplace_back(NULL_VALUE);
          } else {
            (*context->hash_keys)[chunk_offset].emplace_back(*value_it);
          }
        }
      } else {
        for (const auto &value : values) {
          (*context->hash_keys)[chunk_offset].emplace_back(value);
          ++chunk_offset;
        }
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
    const pmr_vector<T> &dictionary = *(column.dictionary());

    if (context->chunk_offsets_in) {
      for (const ChunkOffset &offset_in_dictionary_column : *(context->chunk_offsets_in)) {
        if (offset_in_dictionary_column == INVALID_CHUNK_OFFSET) {
          (*context->hash_keys)[chunk_offset].emplace_back(NULL_VALUE);
          continue;
        }

        const auto value_id = attribute_vector.get(offset_in_dictionary_column);

        if (value_id == NULL_VALUE_ID) {
          (*context->hash_keys)[chunk_offset].emplace_back(NULL_VALUE);
        } else {
          (*context->hash_keys)[chunk_offset].emplace_back(dictionary[value_id]);
        }

        ++chunk_offset;
      }
    } else {
      // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching
      // rows.
      for (ChunkOffset av_offset{0}; av_offset < column.size(); ++av_offset, ++chunk_offset) {
        const auto value_id = attribute_vector.get(av_offset);

        if (value_id == NULL_VALUE_ID) {
          (*context->hash_keys)[chunk_offset].emplace_back(NULL_VALUE);
        } else {
          (*context->hash_keys)[chunk_offset].emplace_back(dictionary[value_id]);
        }
      }
    }
  }
};

/*
The AggregateFunctionBuilder is used to create the lambda function that will be used by
the AggregateVisitor. It is a separate class because methods cannot be partially specialized.
Therefore, we partially specialize the whole class and define the get_aggregate_function anew every time.
*/
template <typename ColumnType, typename AggregateType>
using AggregateFunctor = std::function<optional<AggregateType>(ColumnType, optional<AggregateType>)>;

template <typename ColumnType, typename AggregateType, AggregateFunction function>
struct AggregateFunctionBuilder {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() { Fail("Invalid aggregate function"); }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Min> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](ColumnType new_value, optional<AggregateType> current_aggregate) {
      if (!current_aggregate || value_smaller(new_value, *current_aggregate)) {
        // New minimum found
        return new_value;
      }
      return *current_aggregate;
    };
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Max> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](ColumnType new_value, optional<AggregateType> current_aggregate) {
      if (!current_aggregate || value_greater(new_value, *current_aggregate)) {
        // New maximum found
        return new_value;
      }
      return *current_aggregate;
    };
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Sum> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](ColumnType new_value, optional<AggregateType> current_aggregate) {
      // add new value to sum
      return new_value + (!current_aggregate ? 0 : *current_aggregate);
    };
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Avg> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](ColumnType new_value, optional<AggregateType> current_aggregate) {
      // add new value to sum
      return new_value + (!current_aggregate ? 0 : *current_aggregate);
    };
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Count> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](ColumnType, optional<AggregateType> current_aggregate) { return nullopt; };
  }
};

/*
Visitor context for the AggregateVisitor.
*/
template <typename ColumnType, typename AggregateType>
struct AggregateContext : ColumnVisitableContext {
  AggregateContext() {}
  explicit AggregateContext(std::shared_ptr<GroupByContext> base_context) : groupby_context(base_context) {}

  // constructor for use in ReferenceColumn::visit_dereferenced
  AggregateContext(std::shared_ptr<BaseColumn>, const std::shared_ptr<const Table>,
                   std::shared_ptr<ColumnVisitableContext> base_context, ChunkID chunk_id,
                   std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets)
      : groupby_context(std::static_pointer_cast<AggregateContext>(base_context)->groupby_context),
        results(std::static_pointer_cast<AggregateContext>(base_context)->results) {
    groupby_context->chunk_id = chunk_id;
    groupby_context->chunk_offsets_in = chunk_offsets;
  }

  std::shared_ptr<GroupByContext> groupby_context;
  std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType>>> results;
};

/*
Visitor for the aggregation phase.
It is used to gradually build the given aggregate over one column.
*/
template <typename ColumnType, typename AggregateType, AggregateFunction function>
struct AggregateVisitor : public ColumnVisitable {
  AggregateFunctor<ColumnType, AggregateType> aggregate_func;
  ChunkOffset chunk_offset = 0;

  AggregateVisitor() {
    // retrieve the correct lambda for the given types and aggregate function
    aggregate_func = AggregateFunctionBuilder<ColumnType, AggregateType, function>().get_aggregate_function();
  }

  /*
  This will check if the results map has been created yet.
  If not, it will be created with the correct AggregateType.
  */
  void check_and_init_context(std::shared_ptr<AggregateContext<ColumnType, AggregateType>> context) {
    if (!context->results) {
      context->results = std::make_shared<std::map<AggregateKey, AggregateResult<AggregateType>>>();
    }
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext<ColumnType, AggregateType>>(base_context);
    check_and_init_context(context);
    const auto &column = static_cast<ValueColumn<ColumnType> &>(base_column);
    const auto &values = column.values();

    auto &hash_keys = static_cast<std::vector<AggregateKey> &>(*context->groupby_context->hash_keys);
    auto &results = static_cast<std::map<AggregateKey, AggregateResult<AggregateType>> &>(*context->results);

    if (context->groupby_context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position

      if (column.is_nullable()) {
        const auto &null_values = column.null_values();

        for (const ChunkOffset &offset_in_value_column : *(context->groupby_context->chunk_offsets_in)) {
          if (null_values[offset_in_value_column]) {
            // Keep it unchanged or initialize
            results.try_emplace(hash_keys[chunk_offset]);
          } else {
            results[hash_keys[chunk_offset]].current_aggregate =
                aggregate_func(values[offset_in_value_column], results[hash_keys[chunk_offset]].current_aggregate);

            // increase value counter
            ++results[hash_keys[chunk_offset]].aggregate_count;
          }

          ++chunk_offset;
        }
      } else {
        for (const ChunkOffset &offset_in_value_column : *(context->groupby_context->chunk_offsets_in)) {
          if (offset_in_value_column == INVALID_CHUNK_OFFSET) {
            results.try_emplace(hash_keys[chunk_offset]);
          } else {
            results[hash_keys[chunk_offset]].current_aggregate =
                aggregate_func(values[offset_in_value_column], results[hash_keys[chunk_offset]].current_aggregate);

            // increase value counter
            ++results[hash_keys[chunk_offset]].aggregate_count;
          }
          ++chunk_offset;
        }
      }
    } else {
      if (column.is_nullable()) {
        const auto &null_values = column.null_values();

        auto value_it = values.cbegin();
        auto null_value_it = null_values.cbegin();

        for (; value_it != values.cend(); ++value_it, ++null_value_it, ++chunk_offset) {
          if (*null_value_it) {
            // Keep it unchanged or initialize
            results.try_emplace(hash_keys[chunk_offset]);
          } else {
            results[hash_keys[chunk_offset]].current_aggregate =
                aggregate_func(*value_it, results[hash_keys[chunk_offset]].current_aggregate);

            // increase value counter
            ++results[hash_keys[chunk_offset]].aggregate_count;
          }
        }
      } else {
        for (const auto &value : values) {
          results[hash_keys[chunk_offset]].current_aggregate =
              aggregate_func(value, results[hash_keys[chunk_offset]].current_aggregate);

          // increase value counter
          ++results[hash_keys[chunk_offset]].aggregate_count;
          ++chunk_offset;
        }
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext<ColumnType, AggregateType>>(base_context);
    check_and_init_context(context);
    column.visit_dereferenced<AggregateContext<ColumnType, AggregateType>>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
    auto context = std::static_pointer_cast<AggregateContext<ColumnType, AggregateType>>(base_context);
    check_and_init_context(context);
    const auto &column = static_cast<DictionaryColumn<ColumnType> &>(base_column);
    const BaseAttributeVector &attribute_vector = *(column.attribute_vector());
    const auto &dictionary = *(column.dictionary());

    auto &hash_keys = static_cast<std::vector<AggregateKey> &>(*context->groupby_context->hash_keys);
    auto &results = static_cast<std::map<AggregateKey, AggregateResult<AggregateType>> &>(*context->results);

    if (context->groupby_context->chunk_offsets_in) {
      for (const ChunkOffset &offset_in_dictionary_column : *(context->groupby_context->chunk_offsets_in)) {
        ValueID value_id;

        if (offset_in_dictionary_column == INVALID_CHUNK_OFFSET) {
          value_id = NULL_VALUE_ID;
        } else {
          value_id = attribute_vector.get(offset_in_dictionary_column);
        }

        if (value_id == NULL_VALUE_ID) {
          // Keep it unchanged or initialize
          results.try_emplace(hash_keys[chunk_offset]);
        } else {
          results[hash_keys[chunk_offset]].current_aggregate =
              aggregate_func(dictionary[value_id], results[hash_keys[chunk_offset]].current_aggregate);

          // increase value counter
          ++results[hash_keys[chunk_offset]].aggregate_count;
        }

        ++chunk_offset;
      }
    } else {
      // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching
      // rows.
      for (ChunkOffset av_offset{0}; av_offset < column.size(); ++av_offset, ++chunk_offset) {
        const auto value_id = attribute_vector.get(av_offset);

        if (value_id == NULL_VALUE_ID) {
          // Keep it unchanged or initialize
          results.try_emplace(hash_keys[chunk_offset]);
        } else {
          results[hash_keys[chunk_offset]].current_aggregate =
              aggregate_func(dictionary[value_id], results[hash_keys[chunk_offset]].current_aggregate);

          // increase value counter
          ++results[hash_keys[chunk_offset]].aggregate_count;
        }
      }
    }
  }
};

/*
The following structs describe the different aggregate traits.
Given a ColumnType and AggregateFunction, certain traits like the aggregate type
can be deduced.
*/
template <typename ColumnType, AggregateFunction function, class Enable = void>
struct AggregateTraits {};

// COUNT on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, AggregateFunction::Count> {
  typedef ColumnType column_type;
  typedef int64_t aggregate_type;
  static constexpr const char *aggregate_type_name = "long";
};

// MIN/MAX on all types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Min || function == AggregateFunction::Max, void>> {
  typedef ColumnType column_type;
  typedef ColumnType aggregate_type;
  static constexpr const char *aggregate_type_name = "";
};

// AVG on arithmetic types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Avg && std::is_arithmetic<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef double aggregate_type;
  static constexpr const char *aggregate_type_name = "double";
};

// SUM on integers
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Sum && std::is_integral<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef int64_t aggregate_type;
  static constexpr const char *aggregate_type_name = "long";
};

// SUM on floating point numbers
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Sum && std::is_floating_point<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef double aggregate_type;
  static constexpr const char *aggregate_type_name = "double";
};

// invalid: AVG on non-arithmetic types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<ColumnType, function, typename std::enable_if_t<!std::is_arithmetic<ColumnType>::value &&
                                                                           (function == AggregateFunction::Avg ||
                                                                            function == AggregateFunction::Sum),
                                                                       void>> {
  typedef ColumnType column_type;
  typedef ColumnType aggregate_type;
  static constexpr const char *aggregate_type_name = "";
};

/*
Creates an appropriate AggregateContext based on the ColumnType and AggregateFunction
*/
template <typename ColumnType, AggregateFunction function>
std::shared_ptr<ColumnVisitableContext> make_aggregate_context() {
  typename AggregateTraits<ColumnType, function>::aggregate_type aggregate_type;

  return std::make_shared<AggregateContext<ColumnType, decltype(aggregate_type)>>();
}

/*
Creates an appropriate AggregateVisitor based on the ColumnType and AggregateFunction
*/
template <typename ColumnType, AggregateFunction function>
std::shared_ptr<ColumnVisitable> make_aggregate_visitor(std::shared_ptr<ColumnVisitableContext> new_ctx,
                                                        std::shared_ptr<GroupByContext> ctx) {
  typename AggregateTraits<ColumnType, function>::aggregate_type aggregate_type;

  auto visitor = std::make_shared<AggregateVisitor<ColumnType, decltype(aggregate_type), function>>();
  std::static_pointer_cast<AggregateContext<ColumnType, decltype(aggregate_type)>>(new_ctx)->groupby_context = ctx;
  return visitor;
}

}  // namespace opossum
