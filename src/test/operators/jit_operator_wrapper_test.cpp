#include <gmock/gmock.h>

#include "../base_test.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
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
    _int_table = load_table("src/test/tables/10_ints.tbl", 5);
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
  MOCK_CONST_METHOD2(before_query, void(const Table&, JitRuntimeContext&));
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
    EXPECT_CALL(*source, before_query(testing::Ref(*_int_table), testing::_));
    EXPECT_CALL(*sink, before_query(testing::_, testing::_));
    EXPECT_CALL(*source, before_chunk(testing::Ref(*_int_table), testing::Ref(*_int_table->chunks()[0]), testing::_));
    EXPECT_CALL(*sink, after_chunk(testing::_, testing::_));
    EXPECT_CALL(*source, before_chunk(testing::Ref(*_int_table), testing::Ref(*_int_table->chunks()[1]), testing::_));
    EXPECT_CALL(*sink, after_chunk(testing::_, testing::_));
    EXPECT_CALL(*sink, after_query(testing::_, testing::_));

    ON_CALL(*source, before_chunk(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Invoke(source.get(), &MockJitSource::forward_before_chunk));
  }

  JitOperatorWrapper jit_operator_wrapper(_int_table_wrapper, JitExecutionMode::Interpret);
  jit_operator_wrapper.add_jit_operator(source);
  jit_operator_wrapper.add_jit_operator(sink);
  jit_operator_wrapper.execute();
}

TEST_F(JitOperatorWrapperTest, JitOperatorsSpecializedWithMultipleInliningOfSameFunction) {
  // During query specialization, the function calls of JitExpression::compute are inlined with two different objects:
  // First the compute function call with the object "expression" is inlined,
  // then the two function calls with the two time referenced object "column_expression" are inlined.

  // Specialize SQL query: SELECT a+a FROM src/test/tables/10_ints.tbl;

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
