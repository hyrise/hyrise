#include "../base_test.hpp"
#include "operators/jit_operator_wrapper.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

// class MockOperator : Jit

class JitOperatorWrapperTest : public BaseTest {};

TEST_F(JitOperatorWrapperTest, FailsOnMissingSourceAndSink) {
  auto empty_table = Table::create_dummy_table({{"a", DataType::Int}});
  auto table_wrapper = std::make_shared<TableWrapper>(empty_table);
  table_wrapper->execute();

  {
    JitOperatorWrapper jit_operator_wrapper(table_wrapper);
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitReadTuples>());
    ASSERT_THROW(jit_operator_wrapper.execute(), std::logic_error);
  }
  {
    JitOperatorWrapper jit_operator_wrapper(table_wrapper);
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitWriteTuples>());
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitReadTuples>());
    ASSERT_THROW(jit_operator_wrapper.execute(), std::logic_error);
  }
  {
    JitOperatorWrapper jit_operator_wrapper(table_wrapper);
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitReadTuples>());
    jit_operator_wrapper.add_jit_operator(std::make_shared<JitWriteTuples>());
    jit_operator_wrapper.execute();
  }
}

} // namespace opossum
