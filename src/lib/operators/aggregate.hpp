#pragma once

#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "resolve_type.hpp"
#include "storage/base_attribute_vector.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

struct GroupByContext;

/*
Operator to aggregate columns by certain functions, such as min, max, sum, average, and count. The output is a table
 with reference columns. As with most operators we do not guarantee a stable operation with regards to positions -
 i.e. your sorting order.

For implementation details, please check the wiki: https://github.com/hyrise/hyrise/wiki/Aggregate-Operator
*/

/*
Current aggregated value and the number of rows that were used.
The latter is used for AVG and COUNT.
*/
template <typename AggregateType, typename ColumnDataType>
struct AggregateResult {
  std::optional<AggregateType> current_aggregate;
  size_t aggregate_count = 0;
  std::set<ColumnDataType> distinct_values;
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
                      const std::optional<std::string>& alias = std::nullopt);

  ColumnID column_id;
  AggregateFunction function;
  std::optional<std::string> alias;
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
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

  // write the aggregated output for a given aggregate column
  template <typename ColumnType, AggregateFunction function>
  void write_aggregate_output(ColumnID column_index);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template <typename ColumnType>
  static void _create_aggregate_context(boost::hana::basic_type<ColumnType> type,
                                        std::shared_ptr<ColumnVisitableContext>& aggregate_context,
                                        AggregateFunction function);

  template <typename ColumnType>
  static void _create_aggregate_visitor(boost::hana::basic_type<ColumnType> type,
                                        std::shared_ptr<ColumnVisitable>& builder,
                                        std::shared_ptr<ColumnVisitableContext> context,
                                        std::shared_ptr<GroupByContext> groupby_context, AggregateFunction function);

  template <typename ColumnType>
  void _write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                               AggregateFunction function);

  template <typename ColumnDataType, AggregateFunction function>
  void _aggregate_column(ChunkID chunk_id, ColumnID column_index, const BaseColumn& base_column);

  const std::vector<AggregateDefinition> _aggregates;
  const std::vector<ColumnID> _groupby_column_ids;

  std::shared_ptr<Table> _output;
  std::shared_ptr<Chunk> _out_chunk;
  std::vector<std::shared_ptr<BaseColumn>> _groupby_columns;
  std::vector<std::shared_ptr<ColumnVisitableContext>> _contexts_per_column;
  std::vector<std::shared_ptr<std::vector<AggregateKey>>> _keys_per_chunk;
};

}  // namespace opossum
