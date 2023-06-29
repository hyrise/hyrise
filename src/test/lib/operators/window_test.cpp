
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/window_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/window_function_evaluator.hpp"
#include "storage/table.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class OperatorsWindowTest : public BaseTest {
 public:
  static void SetUpTestCase() {
    _table = load_table("resources/test_data/tbl/windowoperator/rank/input_not_unique.tbl", ChunkOffset{2});
    _table_wrapper = std::make_shared<TableWrapper>(_table);
    _table_wrapper->never_clear_output();
    _table_wrapper->execute();
  }

 protected:
  inline static std::shared_ptr<Table> _table;
  inline static std::shared_ptr<TableWrapper> _table_wrapper;
};

TEST_F(OperatorsWindowTest, Rank) {
  const auto static_table_node = StaticTableNode::make(_table);

  const auto partition_by_expressions =
      std::vector<std::shared_ptr<AbstractExpression>>{lqp_column_(static_table_node, ColumnID{0})};
  const auto order_by_expressions =
      std::vector<std::shared_ptr<AbstractExpression>>{lqp_column_(static_table_node, ColumnID{1})};

  const auto sort_modes = std::vector<SortMode>{SortMode::Ascending};
  const auto frame_start = FrameBound(0, FrameBoundType::Preceding, true);
  const auto frame_end = FrameBound(0, FrameBoundType::CurrentRow, false);
  auto frame_description = std::make_unique<FrameDescription>(FrameType::Range, frame_start, frame_end);

  const auto window_funtion_expression =
      rank_(window_(partition_by_expressions, order_by_expressions, sort_modes, std::move(frame_description)));

  const auto partition_by_columns = std::vector<ColumnID>{ColumnID{0}};
  const auto order_by_columns = std::vector<ColumnID>{ColumnID{1}};

  const auto window_function_operator = std::make_shared<WindowFunctionEvaluator>(
      this->_table_wrapper, partition_by_columns, order_by_columns, window_funtion_expression);

  window_function_operator->execute();
  const auto result_table = window_function_operator->get_output();

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/windowoperator/rank/input_not_unique_result.tbl");
  EXPECT_TABLE_EQ_UNORDERED(result_table, expected_result);
}

}  // namespace hyrise