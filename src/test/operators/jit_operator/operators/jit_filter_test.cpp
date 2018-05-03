#include "operators/jit_operator/operators/jit_filter.hpp"
#include "../../../base_test.hpp"

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

  JitTupleValue condition_value{DataType::Bool, true, 0};
  auto source = std::make_shared<MockSource>();
  auto filter = std::make_shared<JitFilter>(condition_value);
  auto sink = std::make_shared<MockSink>();

  // Link operators to pipeline
  source->set_next_operator(filter);
  filter->set_next_operator(sink);

  // Condition variables with a NULL value should be filtered out
  condition_value.set_is_null(false, context);
  condition_value.set<bool>(false, context);
  sink->reset();
  source->emit(context);
  ASSERT_FALSE(sink->consume_was_called());

  // FALSE condition variables should be filtered out
  condition_value.set_is_null(false, context);
  condition_value.set<bool>(true, context);
  sink->reset();
  source->emit(context);
  ASSERT_TRUE(sink->consume_was_called());

  // Only condition variables with TRUE should pass
  condition_value.set_is_null(true, context);
  sink->reset();
  source->emit(context);
  ASSERT_FALSE(sink->consume_was_called());
}

}  // namespace opossum
