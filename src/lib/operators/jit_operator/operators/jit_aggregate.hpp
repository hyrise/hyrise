#pragma once

#include "abstract_jittable_sink.hpp"
#include "expression/aggregate_expression.hpp"

namespace opossum {

// Represents an aggregate expression computed by the operator.
// The tuple_value provides the values that are to be aggregated.
// The resulting aggregates for each group of tuples are stored in the hashmap_value.
// The optional hashmap_count_for_avg is used for computing averages. In this case the aggregate operator computes
// two aggregates (a SUM and a COUNT) and divides the two in a post-processing step.
// Each aggregate and groupby column stores its position in the output table. This allows the operator to maintain
// arbitrary orders of groupby and aggregate columns.
struct JitAggregateColumn {
  std::string column_name;
  uint64_t position_in_table;
  AggregateFunction function;
  JitTupleValue tuple_value;
  JitHashmapValue hashmap_value;
  std::optional<JitHashmapValue> hashmap_count_for_avg = std::nullopt;
};

// Represents a column that is used as a GROUP BY column.
// The tuple_value provides the values to group by.
// The value for each group are stored in the hashmap_value.
// Each aggregate and groupby column stores its position in the output table. This allows the operator to maintain
// arbitrary orders of groupby and aggregate columns.
struct JitGroupByColumn {
  std::string column_name;
  uint64_t position_in_table;
  JitTupleValue tuple_value;
  JitHashmapValue hashmap_value;
};

/* The JitAggregate operator performs materialization and must thus be the last operator in an operator chain.
 * The operator groups tuples by a number of columns and computes a set of aggregates for each group of tuples.
 * The operator uses two internal data structures to perform its work:
 * - A set of output vectors - one for each groupby column and one for each aggregate. The output table is later build
 *   from these vectors.
 * - A hashmap that maps hashes (uint64_t values) to a set of indices into these vectors.
 *
 * The operator consumes tuples in a single pass. Each incoming tuple is processed in the following way:
 * - A hash across all groupby columns is computed.
 * - The computed hash is looked up in the hashmap.
 * - In case of no match (i.e., this hash was encountered for the first time), a new output tuple is created by
 *   appending a value to each output vector and adding a mapping from the hash to the index in the output vectors.
 * - In case of a hash match, all rows referenced in the hashmap for that particular hash are compared to the current
 *   tuple. If any row matches across ALL groupby columns, the aggregates in that row are updated with the values from
 *   the current tuple. Otherwise, a new output tuple is created (see above).
 *   These are (roughly) the same operations a std::unordered_map would perform internally.
 * - After all tuples have been processed, the output table is created from the output vectors.
 *
 * Averages can not easily be updated on the fly. Instead, each average aggregate triggers the computation of two
 * aggregates on the same value (a SUM and a COUNT). After all tuples have been consumed, the quotient of these
 * aggregates is computed in a post-processing step to produce the requested averages.
 */
class JitAggregate : public AbstractJittableSink {
 public:
  std::string description() const final;

  // Is called by the JitOperatorWrapper.
  // Creates an empty output table with appropriate column definitions.
  std::shared_ptr<Table> create_output_table(const Table& in_table) const final;

  // Is called by the JitOperatorWrapper before any tuple is consumed.
  // This is used to initialize the internal hashmap data structure to the correct size.
  void before_query(Table& out_table, JitRuntimeContext& context) const final;

  // Is called by the JitOperatorWrapper after all tuples have been consumed.
  // This is used to perform the post-processing for average aggregates and to build the final output table.
  void after_query(Table& out_table, JitRuntimeContext& context) const final;

  // Adds an aggregate to the operator that is to be computed on tuple groups.
  void add_aggregate_column(const std::string& column_name, const JitTupleValue& value,
                            const AggregateFunction function);

  // Adds a column to the operator that is to be considered when grouping tuples.
  void add_groupby_column(const std::string& column_name, const JitTupleValue& value);

  const std::vector<JitAggregateColumn> aggregate_columns() const;
  const std::vector<JitGroupByColumn> groupby_columns() const;

 private:
  void _consume(JitRuntimeContext& context) const final;

  uint32_t _num_hashmap_columns{0};
  std::vector<JitAggregateColumn> _aggregate_columns;
  std::vector<JitGroupByColumn> _groupby_columns;
};

}  // namespace opossum
