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

FrameDescription build_frame(uint64_t offset_preceding = 0, bool preceding_unbounded = true,
                             uint64_t offset_following = 0, bool following_unbounded = false,
                             FrameType type = FrameType::Range) {
  const auto frame_start = FrameBound(offset_preceding, FrameBoundType::Preceding, preceding_unbounded);
  const auto frame_end = FrameBound(offset_following, FrameBoundType::CurrentRow, following_unbounded);
  return FrameDescription(type, frame_start, frame_end);
}

struct WindowOperatorFactory {
 public:
  WindowOperatorFactory(const std::shared_ptr<Table> table, const std::vector<ColumnID>& partition_by_columns,
                        const std::vector<ColumnID>& order_by_columns, const std::vector<SortMode> sort_modes)
      : _partition_by_columns{partition_by_columns}, _order_by_columns{order_by_columns}, _sort_modes{sort_modes} {
    _static_table_node = StaticTableNode::make(table);

    const auto partition_column_count = partition_by_columns.size();
    _partition_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>(partition_column_count);
    for (auto column_index = size_t{0}; column_index < partition_column_count; ++column_index) {
      _partition_by_expressions[column_index] = lqp_column_(_static_table_node, partition_by_columns[column_index]);
    }

    const auto order_by_column_count = order_by_columns.size();
    _order_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>(order_by_column_count);
    for (auto column_index = size_t{0}; column_index < order_by_column_count; ++column_index) {
      _order_by_expressions[column_index] = lqp_column_(_static_table_node, order_by_columns[column_index]);
    }
  }

  std::shared_ptr<WindowFunctionEvaluator> build_operator(const std::shared_ptr<AbstractOperator> input_table,
                                                          FrameDescription frame_description,
                                                          WindowFunction function_type,
                                                          ColumnID argument_column = INVALID_COLUMN_ID) {
    const auto window = window_(std::move(_partition_by_expressions), std::move(_order_by_expressions),
                                std::move(_sort_modes), std::move(frame_description));

    const auto window_function_expression = [&]() -> std::shared_ptr<WindowFunctionExpression> {
      if (aggregate_functions.contains(function_type)) {
        const auto column_expression = std::make_shared<LQPColumnExpression>(_static_table_node, argument_column);
        return std::make_shared<WindowFunctionExpression>(function_type, column_expression, window);
      } else {
        return std::make_shared<WindowFunctionExpression>(function_type, nullptr, window);
      }
    }();
    return std::make_shared<WindowFunctionEvaluator>(input_table, _partition_by_columns, _order_by_columns,
                                                     argument_column, window_function_expression);
  };

  std::shared_ptr<StaticTableNode> _static_table_node;
  std::vector<ColumnID> _partition_by_columns;
  std::vector<std::shared_ptr<AbstractExpression>> _partition_by_expressions;
  std::vector<ColumnID> _order_by_columns;
  std::vector<std::shared_ptr<AbstractExpression>> _order_by_expressions;
  std::vector<SortMode> _sort_modes;
};

void test_output(const std::shared_ptr<WindowFunctionEvaluator> window_function_operator, std::string answer_table) {
  static const auto result_table_path = "resources/test_data/tbl/window_operator/";
  window_function_operator->execute();
  const auto result_table = window_function_operator->get_output();

  std::shared_ptr<Table> expected_result = load_table(result_table_path + answer_table);
  EXPECT_TABLE_EQ_UNORDERED(result_table, expected_result);
}

class OperatorsWindowTest : public BaseTest {
 public:
  static void SetUpTestCase() {
    _table = load_table("resources/test_data/tbl/window_operator/input_not_unique.tbl", ChunkOffset{2});
    _table_wrapper = std::make_shared<TableWrapper>(_table);
    _table_wrapper->never_clear_output();
    _table_wrapper->execute();

    const auto partition_columns = std::vector<ColumnID>{ColumnID{0}};
    const auto order_by_columns = std::vector<ColumnID>{ColumnID{1}};
    const auto sort_modes = std::vector<SortMode>{SortMode::Ascending};

    _window_operator_factory =
        std::make_shared<WindowOperatorFactory>(_table, partition_columns, order_by_columns, sort_modes);
  }

 protected:
  inline static std::shared_ptr<Table> _table;
  inline static std::shared_ptr<TableWrapper> _table_wrapper;
  inline static std::shared_ptr<WindowOperatorFactory> _window_operator_factory;
};

TEST_F(OperatorsWindowTest, OperatorName) {
  auto frame = build_frame();
  const auto window_function_operator =
      _window_operator_factory->build_operator(this->_table_wrapper, std::move(frame), WindowFunction::RowNumber);
  EXPECT_EQ(window_function_operator->name(), "WindowFunctionEvaluator");
}

TEST_F(OperatorsWindowTest, ExactlyOneOrderByForRange) {
  const auto partition_columns = std::vector<ColumnID>{ColumnID{0}};
  const auto order_by_columns = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}};
  const auto sort_modes = std::vector<SortMode>(2, SortMode::Ascending);

  const auto frame = build_frame();
  const auto invalid_operator_factory =
      std::make_shared<WindowOperatorFactory>(_table, partition_columns, order_by_columns, sort_modes);
  const auto window_function_operator_invalid =
      invalid_operator_factory->build_operator(this->_table_wrapper, frame, WindowFunction::Sum, ColumnID{1});
  // For non-rank-like window functions (e.g. sum/avg/etc.),
  // range mode frames are only allowed when there is at most one order-by column.
  EXPECT_THROW(window_function_operator_invalid->execute(), std::logic_error);

  const auto order_by_single_column = std::vector<ColumnID>{ColumnID{1}};
  const auto sort_single_mode = std::vector<SortMode>{SortMode::Ascending};
  const auto valid_operator_factory =
      std::make_shared<WindowOperatorFactory>(_table, partition_columns, order_by_single_column, sort_single_mode);
  const auto window_function_operator_valid =
      valid_operator_factory->build_operator(this->_table_wrapper, frame, WindowFunction::Sum, ColumnID{1});

  EXPECT_NO_THROW(window_function_operator_valid->execute());
}

TEST_F(OperatorsWindowTest, Rank) {
  const auto frame = build_frame();
  const auto window_function_operator =
      _window_operator_factory->build_operator(this->_table_wrapper, frame, WindowFunction::Rank);
  test_output(window_function_operator, "rank.tbl");
}

TEST_F(OperatorsWindowTest, DenseRank) {
  const auto frame = build_frame();
  const auto window_function_operator =
      _window_operator_factory->build_operator(this->_table_wrapper, frame, WindowFunction::DenseRank);
  test_output(window_function_operator, "dense_rank.tbl");
}

TEST_F(OperatorsWindowTest, RowNumber) {
  const auto frame = build_frame();
  const auto window_function_operator =
      _window_operator_factory->build_operator(this->_table_wrapper, frame, WindowFunction::RowNumber);
  test_output(window_function_operator, "row_number.tbl");
}

TEST_F(OperatorsWindowTest, Sum) {
  const auto frame = build_frame();
  const auto window_function_operator =
      _window_operator_factory->build_operator(this->_table_wrapper, frame, WindowFunction::Sum, ColumnID{1});
  test_output(window_function_operator, "prefix_sum.tbl");
}

}  // namespace hyrise