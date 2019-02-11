#include <random>

#include "base_test.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"

namespace opossum {

// Mock JitOperator that passes individual tuples into the chain.
// This operator is used as the tuple source in this test.
class MockSource : public AbstractJittable {
 public:
  std::string description() const final { return "MockOperator"; }

  void emit(JitRuntimeContext& context) { _emit(context); }

 private:
  void _consume(JitRuntimeContext& context) const final {}
};

class JitAggregateTest : public BaseTest {
 protected:
  void SetUp() override {
    // Create a chain of two operators.
    _source = std::make_shared<MockSource>();
    _aggregate = std::make_shared<JitAggregate>();
    _source->set_next_operator(_aggregate);
  }

  std::shared_ptr<MockSource> _source;
  std::shared_ptr<JitAggregate> _aggregate;
};

// Make sure that groupby columns are properly added to the output table
TEST_F(JitAggregateTest, AddsGroupByColumnsToOutputTable) {
  const auto column_definitions = TableColumnDefinitions({{"a", DataType::Int, false},
                                                          {"b", DataType::Long, true},
                                                          {"c", DataType::Float, false},
                                                          {"d", DataType::Double, false},
                                                          {"e", DataType::String, true}});

  for (const auto& column_definition : column_definitions) {
    _aggregate->add_groupby_column(column_definition.name,
                                   JitTupleValue(column_definition.data_type, column_definition.nullable, 0));
  }

  auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});
  EXPECT_EQ(output_table->column_definitions(), column_definitions);
}

// Make sure that aggregates are added to the output table with correct data type and nullability (e.g., count
// aggregates should be non-nullable and of type long independent of the type and nullability of the input value).
TEST_F(JitAggregateTest, AddsAggregateColumnsToOutputTable) {
  _aggregate->add_aggregate_column("count", JitTupleValue(DataType::String, false, 0), AggregateFunction::Count);
  _aggregate->add_aggregate_column("count_nullable", JitTupleValue(DataType::Int, true, 0), AggregateFunction::Count);
  _aggregate->add_aggregate_column("max", JitTupleValue(DataType::Float, false, 0), AggregateFunction::Max);
  _aggregate->add_aggregate_column("max_nullable", JitTupleValue(DataType::Double, true, 0), AggregateFunction::Max);
  _aggregate->add_aggregate_column("min", JitTupleValue(DataType::Long, false, 0), AggregateFunction::Min);
  _aggregate->add_aggregate_column("min_nullable", JitTupleValue(DataType::Int, true, 0), AggregateFunction::Min);
  _aggregate->add_aggregate_column("avg", JitTupleValue(DataType::Float, false, 0), AggregateFunction::Avg);
  _aggregate->add_aggregate_column("avg_nullable", JitTupleValue(DataType::Double, true, 0), AggregateFunction::Avg);
  _aggregate->add_aggregate_column("sum", JitTupleValue(DataType::Long, false, 0), AggregateFunction::Sum);
  _aggregate->add_aggregate_column("sum_nullable", JitTupleValue(DataType::Int, true, 0), AggregateFunction::Sum);

  const auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});

  const auto expected_column_definitions = TableColumnDefinitions({{"count", DataType::Long, false},
                                                                   {"count_nullable", DataType::Long, false},
                                                                   {"max", DataType::Float, true},
                                                                   {"max_nullable", DataType::Double, true},
                                                                   {"min", DataType::Long, true},
                                                                   {"min_nullable", DataType::Int, true},
                                                                   {"avg", DataType::Double, true},
                                                                   {"avg_nullable", DataType::Double, true},
                                                                   {"sum", DataType::Long, true},
                                                                   {"sum_nullable", DataType::Long, true}});

  EXPECT_EQ(output_table->column_definitions(), expected_column_definitions);
}

// Check, that aggregates on invalid data types are rejected.
TEST_F(JitAggregateTest, InvalidAggregatesAreRejected) {
  // Test case is only run in debug mode as checks are DebugAsserts, which are not present in release mode.
  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(
        _aggregate->add_aggregate_column("invalid", JitTupleValue(DataType::String, false, 0), AggregateFunction::Avg),
        std::logic_error);
    EXPECT_THROW(
        _aggregate->add_aggregate_column("invalid", JitTupleValue(DataType::String, true, 0), AggregateFunction::Sum),
        std::logic_error);
    EXPECT_THROW(
        _aggregate->add_aggregate_column("invalid", JitTupleValue(DataType::Null, false, 0), AggregateFunction::Min),
        std::logic_error);
    EXPECT_THROW(
        _aggregate->add_aggregate_column("invalid", JitTupleValue(DataType::Null, true, 0), AggregateFunction::Max),
        std::logic_error);
    EXPECT_THROW(_aggregate->add_aggregate_column("invalid", JitTupleValue(DataType::Int, false, 0),
                                                  AggregateFunction::CountDistinct),
                 std::logic_error);
  }
}

// Check, that any order of groupby and aggregates columns is reflected in the output table.
TEST_F(JitAggregateTest, MaintainsColumnOrderInOutputTable) {
  _aggregate->add_aggregate_column("a", JitTupleValue(DataType::String, false, 0), AggregateFunction::Count);
  _aggregate->add_groupby_column("b", JitTupleValue(DataType::Double, false, 0));
  _aggregate->add_aggregate_column("c", JitTupleValue(DataType::Long, true, 0), AggregateFunction::Min);
  _aggregate->add_groupby_column("d", JitTupleValue(DataType::Int, true, 0));

  const auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});
  const auto expected_column_names = std::vector<std::string>({"a", "b", "c", "d"});
  EXPECT_EQ(output_table->column_names(), expected_column_names);
}

// Check, that the aggregate operator combines multiple columns when grouping tuples.
TEST_F(JitAggregateTest, GroupsByMultipleColumns) {
  JitRuntimeContext context;
  context.tuple.resize(2);

  const auto value_a = JitTupleValue(DataType::Int, false, 0);
  const auto value_b = JitTupleValue(DataType::Int, false, 1);

  _aggregate->add_groupby_column("a", value_a);
  _aggregate->add_groupby_column("b", value_b);

  auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});
  _aggregate->before_query(*output_table, context);

  // We pass tuples with three value-combinations through the operator chain.
  // Each value combination should create one row in the output table.
  // Some tuples are repeated multiple times to make sure repeated value combinations only produce one row in the output
  // table.

  // Emit (1, 1) tuple
  value_a.set<int32_t>(1, context);
  value_b.set<int32_t>(1, context);
  _source->emit(context);
  _source->emit(context);

  // Emit (1, 2) tuple
  value_b.set<int32_t>(2, context);
  _source->emit(context);

  // Emit (2, 2) tuple
  value_a.set<int32_t>(2, context);
  _source->emit(context);
  _source->emit(context);
  _source->emit(context);

  _aggregate->after_query(*output_table, context);

  EXPECT_EQ(output_table->row_count(), 3u);
}

// Check NULL == NULL semantics.
TEST_F(JitAggregateTest, GroupsNullValues) {
  JitRuntimeContext context;
  context.tuple.resize(2);

  const auto value_a = JitTupleValue(DataType::Int, true, 0);
  const auto value_b = JitTupleValue(DataType::Int, true, 1);

  _aggregate->add_groupby_column("a", value_a);
  _aggregate->add_groupby_column("b", value_b);

  auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});
  _aggregate->before_query(*output_table, context);

  value_a.set<int32_t>(1, context);
  value_b.set<int32_t>(1, context);

  // Emit (NULL, 1) tuple
  value_a.set_is_null(true, context);
  _source->emit(context);
  _source->emit(context);

  // Emit (NULL, NULL) tuple
  value_b.set_is_null(true, context);
  _source->emit(context);
  _source->emit(context);

  _aggregate->after_query(*output_table, context);
  EXPECT_EQ(output_table->row_count(), 2u);
}

// Check the computation of aggregate values.
TEST_F(JitAggregateTest, CorrectlyComputesAggregates) {
  JitRuntimeContext context;
  context.tuple.resize(2);

  const auto value_a = JitTupleValue(DataType::Int, false, 0);
  const auto value_b = JitTupleValue(DataType::Int, true, 1);

  // We compute an aggregate of each type on the same input value.
  _aggregate->add_groupby_column("groupby", value_a);
  _aggregate->add_aggregate_column("count", value_b, AggregateFunction::Count);
  _aggregate->add_aggregate_column("sum", value_b, AggregateFunction::Sum);
  _aggregate->add_aggregate_column("max", value_b, AggregateFunction::Max);
  _aggregate->add_aggregate_column("min", value_b, AggregateFunction::Min);
  _aggregate->add_aggregate_column("avg", value_b, AggregateFunction::Avg);

  auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});
  _aggregate->before_query(*output_table, context);

  // Group 1
  value_a.set<int32_t>(1, context);

  // We pass a NULL value as input to the aggregate functions. This value should be ignored by all aggregates.
  value_b.set_is_null(true, context);
  _source->emit(context);

  // Now pass some "real" inputs to the aggregate functions.
  value_b.set_is_null(false, context);
  for (auto i = 0; i < 10; ++i) {
    value_b.set<int32_t>(i, context);
    _source->emit(context);
  }

  // Group 2
  value_a.set<int32_t>(2, context);

  // Again, we pass a NULL value as input to the aggregate functions. This value should be ignored by all aggregates.
  value_b.set_is_null(true, context);
  _source->emit(context);

  // Now pass some "real" inputs to the aggregate functions.
  value_b.set_is_null(false, context);
  for (auto i = 20; i > 10; --i) {
    value_b.set<int32_t>(i, context);
    _source->emit(context);
  }

  _aggregate->after_query(*output_table, context);

  const auto expected_column_definitions = TableColumnDefinitions({{"groupby", DataType::Int, false},
                                                                   {"count", DataType::Long, false},
                                                                   {"sum", DataType::Long, true},
                                                                   {"max", DataType::Int, true},
                                                                   {"min", DataType::Int, true},
                                                                   {"avg", DataType::Double, true}});

  auto expected_output_table = std::make_shared<Table>(expected_column_definitions, TableType::Data);
  expected_output_table->append({1, 10, 45, 9, 0, 4.5});
  expected_output_table->append({2, 10, 155, 20, 11, 15.5});

  EXPECT_TRUE(check_table_equal(output_table, expected_output_table, OrderSensitivity::No, TypeCmpMode::Strict,
                                FloatComparisonMode::AbsoluteDifference));
}

// Check the computation of aggregate values when there are no groupby columns.
TEST_F(JitAggregateTest, NoGroupByColumns) {
  JitRuntimeContext context;
  context.tuple.resize(1);

  const auto value = JitTupleValue(DataType::Int, false, 0);

  // We compute an aggregate of each type.
  _aggregate->add_aggregate_column("count", value, AggregateFunction::Count);
  _aggregate->add_aggregate_column("sum", value, AggregateFunction::Sum);
  _aggregate->add_aggregate_column("max", value, AggregateFunction::Max);
  _aggregate->add_aggregate_column("min", value, AggregateFunction::Min);
  _aggregate->add_aggregate_column("avg", value, AggregateFunction::Avg);

  auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});
  _aggregate->before_query(*output_table, context);

  value.set<int32_t>(1, context);
  _source->emit(context);
  value.set<int32_t>(5, context);
  _source->emit(context);

  _aggregate->after_query(*output_table, context);

  const auto expected_column_definitions = TableColumnDefinitions({{"count", DataType::Long, false},
                                                                   {"sum", DataType::Long, true},
                                                                   {"max", DataType::Int, true},
                                                                   {"min", DataType::Int, true},
                                                                   {"avg", DataType::Double, true}});

  auto expected_output_table = std::make_shared<Table>(expected_column_definitions, TableType::Data);
  expected_output_table->append({2, 6, 5, 1, 3});

  EXPECT_TRUE(check_table_equal(output_table, expected_output_table, OrderSensitivity::No, TypeCmpMode::Strict,
                                FloatComparisonMode::AbsoluteDifference));
}

// Check the computation of aggregate values on an empty table.
TEST_F(JitAggregateTest, EmptyInputTable) {
  JitRuntimeContext context;
  context.tuple.resize(2);

  const auto value_a = JitTupleValue(DataType::Int, false, 0);
  const auto value_b = JitTupleValue(DataType::Int, true, 1);

  // We compute an aggregate of each type on the same input value.
  _aggregate->add_groupby_column("groupby", value_a);
  _aggregate->add_aggregate_column("count", value_b, AggregateFunction::Count);
  _aggregate->add_aggregate_column("sum", value_b, AggregateFunction::Sum);
  _aggregate->add_aggregate_column("max", value_b, AggregateFunction::Max);
  _aggregate->add_aggregate_column("min", value_b, AggregateFunction::Min);
  _aggregate->add_aggregate_column("avg", value_b, AggregateFunction::Avg);

  auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});
  _aggregate->before_query(*output_table, context);
  _aggregate->after_query(*output_table, context);

  const auto expected_column_definitions = TableColumnDefinitions({{"groupby", DataType::Int, false},
                                                                   {"count", DataType::Long, false},
                                                                   {"sum", DataType::Long, true},
                                                                   {"max", DataType::Int, true},
                                                                   {"min", DataType::Int, true},
                                                                   {"avg", DataType::Double, true}});

  auto expected_output_table = std::make_shared<Table>(expected_column_definitions, TableType::Data);
  EXPECT_TRUE(check_table_equal(output_table, expected_output_table, OrderSensitivity::No, TypeCmpMode::Strict,
                                FloatComparisonMode::AbsoluteDifference));
}

// Check the computation of aggregate values on an empty table with no groupby columns.
TEST_F(JitAggregateTest, EmptyInputTableNoGroupbyColumns) {
  JitRuntimeContext context;
  context.tuple.resize(1);

  const auto value = JitTupleValue(DataType::Int, false, 0);

  // We compute an aggregate of each type on the same input value.
  _aggregate->add_aggregate_column("count", value, AggregateFunction::Count);
  _aggregate->add_aggregate_column("sum", value, AggregateFunction::Sum);
  _aggregate->add_aggregate_column("max", value, AggregateFunction::Max);
  _aggregate->add_aggregate_column("min", value, AggregateFunction::Min);
  _aggregate->add_aggregate_column("avg", value, AggregateFunction::Avg);

  auto output_table = _aggregate->create_output_table(Table{TableColumnDefinitions{}, TableType::Data});
  _aggregate->before_query(*output_table, context);
  _aggregate->after_query(*output_table, context);

  const auto expected_column_definitions = TableColumnDefinitions({{"count", DataType::Long, false},
                                                                   {"sum", DataType::Long, true},
                                                                   {"max", DataType::Int, true},
                                                                   {"min", DataType::Int, true},
                                                                   {"avg", DataType::Double, true}});

  auto expected_output_table = std::make_shared<Table>(expected_column_definitions, TableType::Data);
  expected_output_table->append({0, NullValue{}, NullValue{}, NullValue{}, NullValue{}});
  EXPECT_TRUE(check_table_equal(output_table, expected_output_table, OrderSensitivity::No, TypeCmpMode::Strict,
                                FloatComparisonMode::AbsoluteDifference));
}

}  // namespace opossum
