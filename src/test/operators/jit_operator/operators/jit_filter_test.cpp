#include "base_test.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"

namespace opossum {

// Mock JitOperator that records whether tuples are passed to it
class MockSink : public AbstractJittable {
 public:
  std::string description() const final { return "MockSink"; }

  void reset() const { _consume_was_called = false; }

  bool consume_was_called() const { return _consume_was_called; }

 private:
  void _consume(JitRuntimeContext& context) const final { _consume_was_called = true; }

  // Must be static, since _consume is const
  static bool _consume_was_called;
};

bool MockSink::_consume_was_called = false;

// Mock JitOperator that passes on individual tuples
class MockSource : public AbstractJittable {
 public:
  std::string description() const final { return "MockSource"; }

  void emit(JitRuntimeContext& context) { _emit(context); }

 private:
  void _consume(JitRuntimeContext& context) const final {}
};

class JitFilterTest : public BaseTest {};

TEST_F(JitFilterTest, FiltersTuplesAccordingToCondition) {
  JitRuntimeContext context;
  context.tuple.resize(1);

  JitTupleEntry condition_tuple_entry{DataType::Bool, false, 0};
  auto condition_expression = std::make_shared<JitExpression>(condition_tuple_entry);
  auto source = std::make_shared<MockSource>();
  auto filter = std::make_shared<JitFilter>(condition_expression);
  auto sink = std::make_shared<MockSink>();

  // Link operators to pipeline
  source->set_next_operator(filter);
  filter->set_next_operator(sink);

  // Condition variables with a NULL value should be filtered out
  condition_tuple_entry.set_is_null(false, context);
  condition_tuple_entry.set<bool>(false, context);
  sink->reset();
  source->emit(context);
  ASSERT_FALSE(sink->consume_was_called());

  // FALSE condition variables should be filtered out
  condition_tuple_entry.set_is_null(false, context);
  condition_tuple_entry.set<bool>(true, context);
  sink->reset();
  source->emit(context);
  ASSERT_TRUE(sink->consume_was_called());

  // Only condition variables with TRUE should pass
  condition_tuple_entry.set_is_null(true, context);
  sink->reset();
  source->emit(context);
  ASSERT_FALSE(sink->consume_was_called());
}

TEST_F(JitFilterTest, UpdateNullableInformationBeforeSpecialization) {
  // The nullable information of the filter expression must be updated before specialization

  // Create tuple entry without setting the correct nullable information
  JitTupleEntry bool_tuple_entry{DataType::Bool, false, 0};

  auto bool_expression = std::make_shared<JitExpression>(bool_tuple_entry);
  auto not_expression = std::make_shared<JitExpression>(bool_expression, JitExpressionType::Not, 1);

  JitFilter jit_filter(not_expression);

  EXPECT_FALSE(jit_filter.expression->result_entry.guaranteed_non_null);

  // Update nullable information
  auto input_table = Table::create_dummy_table(TableColumnDefinitions{});
  bool unused_value = false;
  std::vector<bool> tuple_non_nullable_information{true, unused_value};
  jit_filter.before_specialization(*input_table, tuple_non_nullable_information);

  // Nullable information is updated in the result entry ...
  EXPECT_TRUE(jit_filter.expression->result_entry.guaranteed_non_null);
  // ... and the tuple_non_nullable_information vector
  EXPECT_TRUE(tuple_non_nullable_information[1]);
}

}  // namespace opossum
