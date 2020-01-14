#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorsUpdateTest : public BaseTest {
 public:
  static void SetUpTestCase() {
    column_a = pqp_column_(ColumnID{0}, DataType::Int, false, "a");
    column_b = pqp_column_(ColumnID{1}, DataType::Float, false, "b");
  }

  void SetUp() override {
    const auto table = load_table("resources/test_data/tbl/int_float2.tbl", 2);
    // Update operator works on the StorageManager
    Hyrise::get().storage_manager.add_table(table_to_update_name, table);
  }

  void helper(const std::shared_ptr<AbstractExpression>& where_predicate,
              const std::vector<std::shared_ptr<AbstractExpression>>& update_expressions,
              const std::string& expected_result_path) {
    const auto get_table = std::make_shared<GetTable>(table_to_update_name);
    const auto where_scan = std::make_shared<TableScan>(get_table, where_predicate);
    const auto updated_values_projection = std::make_shared<Projection>(where_scan, update_expressions);

    get_table->execute();
    where_scan->execute();
    updated_values_projection->execute();

    const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
    const auto update = std::make_shared<Update>(table_to_update_name, where_scan, updated_values_projection);
    update->set_transaction_context(transaction_context);
    update->execute();
    transaction_context->commit();

    // Get validated table which should have the same row twice.
    const auto post_update_transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
    const auto post_update_get_table = std::make_shared<GetTable>(table_to_update_name);
    const auto validate = std::make_shared<Validate>(post_update_get_table);
    validate->set_transaction_context(post_update_transaction_context);
    post_update_get_table->execute();
    validate->execute();

    EXPECT_TABLE_EQ_UNORDERED(validate->get_output(), load_table(expected_result_path));
  }

  std::string table_to_update_name{"updateTestTable"};
  inline static std::shared_ptr<AbstractExpression> column_a, column_b;
};

TEST_F(OperatorsUpdateTest, SelfOverride) {
  helper(greater_than_(column_a, 0), expression_vector(column_a, column_b), "resources/test_data/tbl/int_float2.tbl");
}

TEST_F(OperatorsUpdateTest, UpdateWithLiteral) {
  helper(greater_than_(column_a, 100), expression_vector(column_a, 7.5f),
         "resources/test_data/tbl/int_float2_updated_0.tbl");
}

TEST_F(OperatorsUpdateTest, UpdateWithExpression) {
  helper(greater_than_(column_a, 1000), expression_vector(column_a, cast_(add_(column_a, 100), DataType::Float)),
         "resources/test_data/tbl/int_float2_updated_1.tbl");
}

TEST_F(OperatorsUpdateTest, UpdateNone) {
  helper(greater_than_(column_a, 100'000), expression_vector(1, 1.5f), "resources/test_data/tbl/int_float2.tbl");
}

}  // namespace opossum
