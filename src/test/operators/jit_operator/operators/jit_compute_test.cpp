#include <random>

#include "base_test.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"

namespace opossum {

// Mock JitOperator that passes individual tuples into the chain and ignores consumed tuples
// This operator is used as both the tuple source and sink in this test
class MockOperator : public AbstractJittable {
 public:
  std::string description() const final { return "MockOperator"; }

  void emit(JitRuntimeContext& context) { _emit(context); }

 private:
  void _consume(JitRuntimeContext& context) const final {}
};

class JitComputeTest : public BaseTest {};

TEST_F(JitComputeTest, TriggersComputationOfNestedExpression) {
  JitRuntimeContext context;
  context.tuple.resize(5);

  // Create tuple entries for inputs
  JitTupleEntry a_tuple_entry{DataType::Int, true, 0};
  JitTupleEntry b_tuple_entry{DataType::Int, true, 1};
  JitTupleEntry c_tuple_entry{DataType::Int, true, 2};

  // Construct expression tree for "A + B > C"
  auto a_expression = std::make_shared<JitExpression>(a_tuple_entry);
  auto b_expression = std::make_shared<JitExpression>(b_tuple_entry);
  auto c_expression = std::make_shared<JitExpression>(c_tuple_entry);
  auto a_plus_b = std::make_shared<JitExpression>(a_expression, JitExpressionType::Addition, b_expression, 3);
  auto expression = std::make_shared<JitExpression>(a_plus_b, JitExpressionType::GreaterThan, c_expression, 4);

  // Construct operator chain
  auto source = std::make_shared<MockOperator>();
  auto compute = std::make_shared<JitCompute>(expression);
  auto sink = std::make_shared<MockOperator>();
  source->set_next_operator(compute);
  compute->set_next_operator(sink);

  // We test the correct computation of the expression ten times with random values
  std::random_device rd;
  std::mt19937 gen(rd());

  for (auto i = 0; i < 10; ++i) {
    // Create input tuple values
    std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
    auto a = dis(gen);
    auto c = dis(gen);
    // ensure that a + b does not cause overflow
    dis = std::uniform_int_distribution<int32_t>(0, std::numeric_limits<int32_t>::max() - a);
    auto b = dis(gen);

    // Set input values in tuple
    a_tuple_entry.set<int32_t>(a, context);
    b_tuple_entry.set<int32_t>(b, context);
    c_tuple_entry.set<int32_t>(c, context);

    source->emit(context);
    ASSERT_EQ(a + b > c, context.tuple.get<bool>(4));
  }
}

TEST_F(JitComputeTest, UpdateNullableInformationBeforeSpecialization) {
  // The nullable information of the compute expression must be updated before specialization

  // Create tuple entry without setting the correct nullable information
  JitTupleEntry bool_tuple_entry{DataType::Bool, false, 0};

  auto bool_expression = std::make_shared<JitExpression>(bool_tuple_entry);
  auto not_expression = std::make_shared<JitExpression>(bool_expression, JitExpressionType::Not, 1);

  JitCompute jit_compute(not_expression);

  EXPECT_FALSE(jit_compute.expression->result_entry.guaranteed_non_null);

  // Update nullable information
  auto input_table = Table::create_dummy_table(TableColumnDefinitions{});
  bool unused_value = false;
  std::vector<bool> tuple_non_nullable_information{true, unused_value};
  jit_compute.before_specialization(*input_table, tuple_non_nullable_information);

  // Nullable information is updated in the result entry ...
  EXPECT_TRUE(jit_compute.expression->result_entry.guaranteed_non_null);
  // ... and the tuple_non_nullable_information vector
  EXPECT_TRUE(tuple_non_nullable_information[1]);
}

}  // namespace opossum
