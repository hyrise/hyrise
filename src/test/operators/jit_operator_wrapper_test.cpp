#include <gmock/gmock.h>

#include "base_test.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_limit.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "operators/jit_operator_wrapper.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

class JitOperatorWrapperTest : public BaseTest {
 protected:
  void SetUp() override {
    _empty_table = Table::create_dummy_table({{"a", DataType::Int}});
    _empty_table_wrapper = std::make_shared<TableWrapper>(_empty_table);
    _empty_table_wrapper->execute();
    _int_table = load_table("resources/test_data/tbl/10_ints.tbl", 5);
    _int_table_wrapper = std::make_shared<TableWrapper>(_int_table);
    _int_table_wrapper->execute();
  }

  std::shared_ptr<Table> _empty_table;
  std::shared_ptr<Table> _int_table;
  std::shared_ptr<TableWrapper> _empty_table_wrapper;
  std::shared_ptr<TableWrapper> _int_table_wrapper;
};

class MockJitSource : public JitReadTuples {
 public:
  MOCK_CONST_METHOD3(before_query, void(const Table&, const std::vector<AllTypeVariant>&, JitRuntimeContext&));
  MOCK_CONST_METHOD3(before_chunk, void(const Table&, const Chunk&, JitRuntimeContext&));

  void forward_before_chunk(const Table& in_table, const Chunk& in_chunk, JitRuntimeContext& context) const {
    JitReadTuples::before_chunk(in_table, in_chunk, context);
  }
};

class MockJitSink : public JitWriteTuples {
 public:
  MOCK_CONST_METHOD2(before_query, void(Table&, JitRuntimeContext&));
  MOCK_CONST_METHOD2(after_query, void(Table&, JitRuntimeContext&));
  MOCK_CONST_METHOD2(after_chunk, void(Table&, JitRuntimeContext&));
};

TEST_F(JitOperatorWrapperTest, JitOperatorsAreAdded) {
  auto _operator_1 = std::make_shared<JitReadTuples>();
  auto _operator_2 = std::make_shared<JitFilter>(JitTupleValue(DataType::Bool, false, -1));
  auto _operator_3 = std::make_shared<JitWriteTuples>();

  JitOperatorWrapper jit_operator_wrapper(_empty_table_wrapper, JitExecutionMode::Interpret);
  jit_operator_wrapper.add_jit_operator(_operator_1);
  jit_operator_wrapper.add_jit_operator(_operator_2);
  jit_operator_wrapper.add_jit_operator(_operator_3);

  ASSERT_EQ(jit_operator_wrapper.jit_operators().size(), 3u);
  ASSERT_EQ(jit_operator_wrapper.jit_operators()[0], _operator_1);
  ASSERT_EQ(jit_operator_wrapper.jit_operators()[1], _operator_2);
  ASSERT_EQ(jit_operator_wrapper.jit_operators()[2], _operator_3);
}

TEST_F(JitOperatorWrapperTest, JitOperatorsAreConnectedToAChain) {
  auto _operator_1 = std::make_shared<JitReadTuples>();
  auto _operator_2 = std::make_shared<JitFilter>(JitTupleValue(DataType::Bool, false, -1));
  auto _operator_3 = std::make_shared<JitWriteTuples>();

  JitOperatorWrapper jit_operator_wrapper(_empty_table_wrapper, JitExecutionMode::Interpret);
  jit_operator_wrapper.add_jit_operator(_operator_1);
  jit_operator_wrapper.add_jit_operator(_operator_2);
  jit_operator_wrapper.add_jit_operator(_operator_3);
  jit_operator_wrapper.execute();

  ASSERT_EQ(_operator_1->next_operator(), _operator_2);
  ASSERT_EQ(_operator_2->next_operator(), _operator_3);
  ASSERT_EQ(_operator_3->next_operator(), nullptr);
}

TEST_F(JitOperatorWrapperTest, ExecutionFailsIfSourceOrSinkAreMissing) {
  {
    JitOperatorWrapper jit_operator_wrapper(_empty_table_wrapper, JitExecutionMode::Interpret);
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitReadTuples>());
    ASSERT_THROW(jit_operator_wrapper.execute(), std::logic_error);
  }
  {
    JitOperatorWrapper jit_operator_wrapper(_empty_table_wrapper);
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitWriteTuples>());
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitReadTuples>());
    ASSERT_THROW(jit_operator_wrapper.execute(), std::logic_error);
  }
  {
    // Both source and sink are set, so this should work
    JitOperatorWrapper jit_operator_wrapper(_empty_table_wrapper, JitExecutionMode::Interpret);
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitReadTuples>());
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitWriteTuples>());
    jit_operator_wrapper.execute();
  }
}

TEST_F(JitOperatorWrapperTest, CallsJitOperatorHooks) {
  auto source = std::make_shared<MockJitSource>();
  auto sink = std::make_shared<MockJitSink>();

  {
    testing::InSequence dummy;
    EXPECT_CALL(*source, before_query(testing::Ref(*_int_table), testing::_, testing::_));
    EXPECT_CALL(*sink, before_query(testing::_, testing::_));
    EXPECT_CALL(*source, before_chunk(testing::Ref(*_int_table), testing::Ref(*_int_table->chunks()[0]), testing::_));
    EXPECT_CALL(*sink, after_chunk(testing::_, testing::_));
    EXPECT_CALL(*source, before_chunk(testing::Ref(*_int_table), testing::Ref(*_int_table->chunks()[1]), testing::_));
    EXPECT_CALL(*sink, after_chunk(testing::_, testing::_));
    EXPECT_CALL(*sink, after_query(testing::_, testing::_));

    ON_CALL(*source, before_query(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Invoke(
            [](const Table& in_table, const std::vector<AllTypeVariant>& parameter_values, JitRuntimeContext& context) {
              context.limit_rows = std::numeric_limits<size_t>::max();
            }));

    ON_CALL(*source, before_chunk(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Invoke(source.get(), &MockJitSource::forward_before_chunk));
  }

  JitOperatorWrapper jit_operator_wrapper(_int_table_wrapper, JitExecutionMode::Interpret);
  jit_operator_wrapper.add_jit_operator(source);
  jit_operator_wrapper.add_jit_operator(sink);
  jit_operator_wrapper.execute();
}

TEST_F(JitOperatorWrapperTest, OperatorChecksLimitRowCount) {
  // A chunk is only processed if context.limit_rows > 0.
  // The input table in this test has two chunks which contain 5 rows each.
  // The row count limit is 5. So the first chunk is processed and the second is not.

  const auto source = std::make_shared<MockJitSource>();
  const auto limit = std::make_shared<JitLimit>();
  const auto sink = std::make_shared<MockJitSink>();

  {
    testing::InSequence dummy;
    EXPECT_CALL(*source, before_query(testing::Ref(*_int_table), testing::_, testing::_));
    EXPECT_CALL(*sink, before_query(testing::_, testing::_));
    EXPECT_CALL(*source, before_chunk(testing::Ref(*_int_table), testing::Ref(*_int_table->chunks()[0]), testing::_));
    EXPECT_CALL(*sink, after_chunk(testing::_, testing::_));
    // before_chunk is called only once, second chunk is not processed
    EXPECT_CALL(*sink, after_query(testing::_, testing::_));

    ON_CALL(*source, before_query(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Invoke([](const Table& in_table, const std::vector<AllTypeVariant>& parameter_values,
                                          JitRuntimeContext& context) { context.limit_rows = 5; }));
    ON_CALL(*source, before_chunk(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Invoke(source.get(), &MockJitSource::forward_before_chunk));
  }

  JitOperatorWrapper jit_operator_wrapper(_int_table_wrapper, JitExecutionMode::Interpret);
  jit_operator_wrapper.add_jit_operator(source);
  jit_operator_wrapper.add_jit_operator(limit);
  jit_operator_wrapper.add_jit_operator(sink);
  jit_operator_wrapper.execute();
}

TEST_F(JitOperatorWrapperTest, SetParameters) {
  // Prepare parameters
  AllTypeVariant value_1{1};
  AllTypeVariant value_2{2};
  AllTypeVariant value_3{3.f};
  AllTypeVariant value_4{4.};
  std::unordered_map<ParameterID, AllTypeVariant> parameters;
  parameters[ParameterID{1}] = value_1;
  parameters[ParameterID{2}] = value_2;
  parameters[ParameterID{3}] = value_3;
  parameters[ParameterID{4}] = value_4;

  // Prepare JitReadTuples
  const auto source = std::make_shared<JitReadTuples>();
  source->add_parameter(DataType::Double, ParameterID{4});
  source->add_parameter(DataType::Int, ParameterID{2});

  // Prepare JitOperatorWrapper
  JitOperatorWrapper jit_operator_wrapper(_empty_table_wrapper, JitExecutionMode::Interpret);
  jit_operator_wrapper.add_jit_operator(source);

  jit_operator_wrapper.set_parameters(parameters);

  const auto input_parameter_values = jit_operator_wrapper.input_parameter_values();

  EXPECT_EQ(input_parameter_values.size(), 2u);
  EXPECT_EQ(input_parameter_values[0], value_4);
  EXPECT_EQ(input_parameter_values[1], value_2);
}

TEST_F(JitOperatorWrapperTest, FilterTableWithLiteralAndParameter) {
  auto input_table = load_table("resources/test_data/tbl/int_float2.tbl", 2);
  auto table_wrapper = std::make_shared<TableWrapper>(input_table);
  table_wrapper->execute();

  auto expected_result = load_table("resources/test_data/tbl/int_float2_filtered.tbl", 2);

  // Create jittable operators
  auto read_tuples = std::make_shared<JitReadTuples>();
  auto a_value = read_tuples->add_input_column(DataType::Int, true, ColumnID{0});
  auto b_value = read_tuples->add_input_column(DataType::Float, true, ColumnID{1});
  auto literal_value = read_tuples->add_literal_value(12345);
  auto parameter_value = read_tuples->add_parameter(DataType::Float, ParameterID{1});

  // Create filter expression
  // clang-format off
  auto left_expression = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_value),
                                                         JitExpressionType::Equals,
                                                         std::make_shared<JitExpression>(literal_value),
                                                         read_tuples->add_temporary_value());
  auto right_expression = std::make_shared<JitExpression>(std::make_shared<JitExpression>(b_value),
                                                          JitExpressionType::GreaterThan,
                                                          std::make_shared<JitExpression>(parameter_value),
                                                          read_tuples->add_temporary_value());
  auto and_expression = std::make_shared<JitExpression>(left_expression,
                                                        JitExpressionType::And,
                                                        right_expression,
                                                        read_tuples->add_temporary_value());
  // clang-format on
  auto compute = std::make_shared<JitCompute>(and_expression);
  auto filter = std::make_shared<JitFilter>(and_expression->result());

  auto write_tuples = std::make_shared<JitWriteTuples>();
  write_tuples->add_output_column("a", a_value);
  write_tuples->add_output_column("b", b_value);

  // Prepare and execute JitOperatorWrapper
  JitOperatorWrapper jit_operator_wrapper{table_wrapper, JitExecutionMode::Interpret};
  jit_operator_wrapper.add_jit_operator(read_tuples);
  jit_operator_wrapper.add_jit_operator(compute);
  jit_operator_wrapper.add_jit_operator(filter);
  jit_operator_wrapper.add_jit_operator(write_tuples);
  std::unordered_map<ParameterID, AllTypeVariant> parameters{{ParameterID{1}, AllTypeVariant{457.1f}}};
  jit_operator_wrapper.set_parameters(parameters);
  jit_operator_wrapper.execute();

  auto output_table = jit_operator_wrapper.get_output();

  // Both tables should be equal now
  ASSERT_TRUE(check_table_equal(output_table, expected_result, OrderSensitivity::Yes, TypeCmpMode::Strict,
                                FloatComparisonMode::AbsoluteDifference));
}

TEST_F(JitOperatorWrapperTest, JitOperatorsSpecializedWithMultipleInliningOfSameFunction) {
  // During query specialization, the function calls of JitExpression::compute are inlined with two different objects:
  // First the compute function call with the object "expression" is inlined,
  // then the two function calls with the two time referenced object "column_expression" are inlined.

  // Specialize SQL query: SELECT a+a FROM resources/test_data/tbl/10_ints.tbl;

  // read column a into jit tuple at index 0
  auto read_operator = std::make_shared<JitReadTuples>();
  auto tuple_value = read_operator->add_input_column(DataType::Int, false, ColumnID(0));

  // compute a+a and write result to jit tuple at index 1
  auto column_expression = std::make_shared<JitExpression>(tuple_value);
  auto add_type = JitExpressionType::Addition;
  auto result_tuple_index = read_operator->add_temporary_value();  // of jit runtime context
  auto expression = std::make_shared<JitExpression>(column_expression, add_type, column_expression, result_tuple_index);
  auto compute_operator = std::make_shared<JitCompute>(expression);

  // copy computed value from jit tuple at index 1 to output table for non-jit operators
  auto write_operator = std::make_shared<JitWriteTuples>();
  write_operator->add_output_column("a+a", expression->result());

  JitOperatorWrapper jit_operator_wrapper(_int_table_wrapper, JitExecutionMode::Compile);
  jit_operator_wrapper.add_jit_operator(read_operator);
  jit_operator_wrapper.add_jit_operator(compute_operator);
  jit_operator_wrapper.add_jit_operator(write_operator);

  ASSERT_NO_THROW(jit_operator_wrapper.execute());

  auto result = jit_operator_wrapper.get_output();
  ASSERT_EQ(result->get_value<int>(ColumnID(0), 1), 48);
}

}  // namespace opossum
