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

void test_output(const std::shared_ptr<AbstractOperator> in, std::shared_ptr<WindowExpression> window_expression,
                 WindowFunction aggregate_type) {
  std::shared_ptr<WindowFunctionExpression> window_funtion_expression;
  static const auto result_table_path = "resources/test_data/tbl/windowoperator/onepass/";
  std::string output_table;

  switch (aggregate_type) {
    case WindowFunction::Rank:
      window_funtion_expression = rank_(window_expression);
      output_table = "rank.tbl";
      break;
    case WindowFunction::DenseRank:
      window_funtion_expression = dense_rank_(window_expression);
      output_table = "dense_rank.tbl";
      break;
    case WindowFunction::RowNumber:
      window_funtion_expression = row_number_(window_expression);
      output_table = "row_number.tbl";
      break;
    default:
      FAIL();
  }

  const auto partition_by_columns = std::vector<ColumnID>{ColumnID{0}};
  const auto order_by_columns = std::vector<ColumnID>{ColumnID{1}};

  const auto window_function_operator =
      std::make_shared<WindowFunctionEvaluator>(in, partition_by_columns, order_by_columns, INVALID_COLUMN_ID, window_funtion_expression);

  window_function_operator->execute();
  const auto result_table = window_function_operator->get_output();

  std::shared_ptr<Table> expected_result = load_table(result_table_path + output_table);
  EXPECT_TABLE_EQ_UNORDERED(result_table, expected_result);
}

class OperatorsWindowTest : public BaseTest {
 public:
  static void SetUpTestCase() {
    _table = load_table("resources/test_data/tbl/windowoperator/onepass/input_not_unique.tbl", ChunkOffset{2});
    _table_wrapper = std::make_shared<TableWrapper>(_table);
    _table_wrapper->never_clear_output();
    _table_wrapper->execute();

    const auto static_table_node = StaticTableNode::make(_table);
    const auto partition_by_expressions =
        std::vector<std::shared_ptr<AbstractExpression>>{lqp_column_(static_table_node, ColumnID{0})};
    const auto order_by_expressions =
        std::vector<std::shared_ptr<AbstractExpression>>{lqp_column_(static_table_node, ColumnID{1})};

    const auto sort_modes = std::vector<SortMode>{SortMode::Ascending};
    const auto frame_start = FrameBound(0, FrameBoundType::Preceding, true);
    const auto frame_end = FrameBound(0, FrameBoundType::CurrentRow, false);
    auto frame_description = std::make_unique<FrameDescription>(FrameType::Range, frame_start, frame_end);

    _onepass_window_expression =
        window_(partition_by_expressions, order_by_expressions, sort_modes, std::move(frame_description));
  }

 protected:
  inline static std::shared_ptr<Table> _table;
  inline static std::shared_ptr<TableWrapper> _table_wrapper;
  inline static std::shared_ptr<WindowExpression> _onepass_window_expression;
};

TEST_F(OperatorsWindowTest, Rank) {
  test_output(this->_table_wrapper, _onepass_window_expression, WindowFunction::Rank);
}

TEST_F(OperatorsWindowTest, DenseRank) {
  test_output(this->_table_wrapper, _onepass_window_expression, WindowFunction::DenseRank);
}

TEST_F(OperatorsWindowTest, RowNumber) {
  test_output(this->_table_wrapper, _onepass_window_expression, WindowFunction::RowNumber);
}

}  // namespace hyrise