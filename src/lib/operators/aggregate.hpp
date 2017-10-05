
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <set>
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
template <typename AggregateType, typename DataType>
class AggregateResult {
 public:
  AggregateResult() {}

  optional<AggregateType> current_aggregate;
  size_t aggregate_count = 0;
  std::set<DataType> distinct_values;
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
                      const optional<std::string>& alias = nullopt);

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

  const std::vector<AggregateDefinition>& aggregates() const;
  const std::vector<ColumnID>& groupby_column_ids() const;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

  // write the aggregated output for a given aggregate column
  template <typename ColumnType, AggregateFunction function>
  void write_aggregate_output(ColumnID column_index);

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;

  template <typename ColumnType>
  static void _create_aggregate_context(boost::hana::basic_type<ColumnType> type,
                                        std::shared_ptr<ColumnVisitableContext>& aggregate_context,
                                        AggregateFunction function);

  template <typename ColumnType>
  static void _create_aggregate_visitor(boost::hana::basic_type<ColumnType> type,
                                        std::shared_ptr<ColumnVisitable>& builder,
                                        std::shared_ptr<ColumnVisitableContext> ctx,
                                        std::shared_ptr<GroupByContext> groupby_ctx, AggregateFunction function);

  template <typename ColumnType>
  void _write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                               AggregateFunction function);

  template <typename DataType, AggregateFunction function>
  void _aggregate_column(ChunkID chunk_id, ColumnID column_index, BaseColumn& base_column);

  /*
  The following template functions write the aggregated values for the different aggregate functions.
  They are separate and templated to avoid compiler errors for invalid type/function combinations.
  */
  // MIN, MAX, SUM write the current aggregated value
  template <typename ColumnType, typename AggregateType, AggregateFunction func>
  typename std::enable_if<
      func == AggregateFunction::Min || func == AggregateFunction::Max || func == AggregateFunction::Sum, void>::type
  _write_aggregate_values(std::shared_ptr<ValueColumn<AggregateType>> column,
                          std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType, ColumnType>>> results) {
    DebugAssert(column->is_nullable(), "Aggregate: Output column needs to be nullable");

    auto& values = column->values();
    auto& null_values = column->null_values();

    for (auto& kv : *results) {
      null_values.push_back(!kv.second.current_aggregate);

      if (!kv.second.current_aggregate) {
        values.push_back(AggregateType());
      } else {
        values.push_back(*kv.second.current_aggregate);
      }
    }
  }

  // COUNT writes the aggregate counter
  template <typename ColumnType, typename AggregateType, AggregateFunction func>
  typename std::enable_if<func == AggregateFunction::Count, void>::type _write_aggregate_values(
      std::shared_ptr<ValueColumn<AggregateType>> column,
      std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType, ColumnType>>> results) {
    DebugAssert(!column->is_nullable(), "Aggregate: Output column for COUNT shouldn't be nullable");

    auto& values = column->values();

    for (auto& kv : *results) {
      values.push_back(kv.second.aggregate_count);
    }
  }

  // COUNT(DISTINCT) writes the number of distinct values
  template <typename ColumnType, typename AggregateType, AggregateFunction func>
  typename std::enable_if<func == AggregateFunction::CountDistinct, void>::type _write_aggregate_values(
      std::shared_ptr<ValueColumn<AggregateType>> column,
      std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType, ColumnType>>> results) {
    DebugAssert(!column->is_nullable(), "Aggregate: Output column for COUNT shouldn't be nullable");

    auto& values = column->values();

    for (auto& kv : *results) {
      values.push_back(kv.second.distinct_values.size());
    }
  }

  // AVG writes the calculated average from current aggregate and the aggregate counter
  template <typename ColumnType, typename AggregateType, AggregateFunction func>
  typename std::enable_if<func == AggregateFunction::Avg && std::is_arithmetic<AggregateType>::value, void>::type
  _write_aggregate_values(std::shared_ptr<ValueColumn<AggregateType>> column,
                          std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType, ColumnType>>> results) {
    DebugAssert(column->is_nullable(), "Aggregate: Output column needs to be nullable");

    auto& values = column->values();
    auto& null_values = column->null_values();

    for (auto& kv : *results) {
      null_values.push_back(!kv.second.current_aggregate);

      if (!kv.second.current_aggregate) {
        values.push_back(AggregateType());
      } else {
        values.push_back(*kv.second.current_aggregate / static_cast<AggregateType>(kv.second.aggregate_count));
      }
    }
  }

  // AVG is not defined for non-arithmetic types. Avoiding compiler errors.
  template <typename ColumnType, typename AggregateType, AggregateFunction func>
  typename std::enable_if<func == AggregateFunction::Avg && !std::is_arithmetic<AggregateType>::value, void>::type
  _write_aggregate_values(std::shared_ptr<ValueColumn<AggregateType>>,
                          std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType, ColumnType>>>) {
    Fail("Invalid aggregate");
  }

  const std::vector<AggregateDefinition> _aggregates;
  const std::vector<ColumnID> _groupby_column_ids;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;

  std::shared_ptr<Table> _output;
  Chunk _out_chunk;
  std::vector<std::shared_ptr<BaseColumn>> _groupby_columns;
  std::vector<std::shared_ptr<ColumnVisitableContext>> _contexts_per_column;
  std::vector<std::shared_ptr<std::vector<AggregateKey>>> _keys_per_chunk;
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
  std::shared_ptr<std::map<AggregateKey, AggregateResult<AggregateType, ColumnType>>> results;
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
  static constexpr const char* aggregate_type_name = "long";
};

// COUNT(DISTINCT) on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, AggregateFunction::CountDistinct> {
  typedef ColumnType column_type;
  typedef int64_t aggregate_type;
  static constexpr const char* aggregate_type_name = "long";
};

// MIN/MAX on all types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Min || function == AggregateFunction::Max, void>> {
  typedef ColumnType column_type;
  typedef ColumnType aggregate_type;
  static constexpr const char* aggregate_type_name = "";
};

// AVG on arithmetic types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Avg && std::is_arithmetic<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef double aggregate_type;
  static constexpr const char* aggregate_type_name = "double";
};

// SUM on integers
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Sum && std::is_integral<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef int64_t aggregate_type;
  static constexpr const char* aggregate_type_name = "long";
};

// SUM on floating point numbers
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Sum && std::is_floating_point<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef double aggregate_type;
  static constexpr const char* aggregate_type_name = "double";
};

// invalid: AVG on non-arithmetic types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<ColumnType, function, typename std::enable_if_t<!std::is_arithmetic<ColumnType>::value &&
                                                                           (function == AggregateFunction::Avg ||
                                                                            function == AggregateFunction::Sum),
                                                                       void>> {
  typedef ColumnType column_type;
  typedef ColumnType aggregate_type;
  static constexpr const char* aggregate_type_name = "";
};

/*
Creates an appropriate AggregateContext based on the ColumnType and AggregateFunction
*/
template <typename ColumnType, AggregateFunction function>
std::shared_ptr<ColumnVisitableContext> make_aggregate_context() {
  typename AggregateTraits<ColumnType, function>::aggregate_type aggregate_type;

  return std::make_shared<AggregateContext<ColumnType, decltype(aggregate_type)>>();
}

}  // namespace opossum
