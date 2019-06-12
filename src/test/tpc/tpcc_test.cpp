#include "base_test.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "tpcc/procedures/tpcc_delivery.hpp"
#include "tpcc/tpcc_table_generator.hpp"

namespace opossum {

class TpccTest : public BaseTest {
 public:
  static void SetUpTestCase() {
    auto table_generator = TpccTableGenerator{Chunk::DEFAULT_SIZE, NUM_WAREHOUSES};

    tables = table_generator.generate_all_tables();
  }

  void SetUp() override {
    for (const auto& [table_name, table] : tables) {
      StorageManager::get().add_table(table_name, table);  // TODO copy this somehow for test isolation
    }
  }

  std::shared_ptr<const Table> get_validated(const std::string& table_name, const std::shared_ptr<TransactionContext>& transaction_context) {
    auto get_table = std::make_shared<GetTable>(table_name);
    get_table->execute();

    auto validate = std::make_shared<Validate>(get_table);
    validate->set_transaction_context(transaction_context);
    validate->execute();

    return validate->get_output();
  }

  void verify_table_sizes(const std::unordered_map<std::string, size_t>& sizes) {
    for (const auto& [table_name, size] : sizes) {
      auto transaction_context = TransactionManager::get().new_transaction_context();
      const auto table = get_validated(table_name, transaction_context);

      if (table_name == "ORDER_LINE" && size == 0) {
        // For ORDER_LINE, the number of lines per order is in [5, 15] and non-deterministic. If it is not specifically
        // calculated, we just check if it is within the acceptable range
        EXPECT_GE(table->row_count(), sizes.at("ORDER") * 5) << "Failed table: " << table_name;
        EXPECT_LE(table->row_count(), sizes.at("ORDER") * 15) << "Failed table: " << table_name;
      } else {
        EXPECT_EQ(table->row_count(), size) << "Failed table: " << table_name;
      }
    }
  }

  // See TPC-C Spec 1.2.1
  const std::unordered_map<std::string, size_t> initial_sizes{{"WAREHOUSE", NUM_WAREHOUSES},
                                                              {"DISTRICT", NUM_WAREHOUSES * 10},
                                                              {"HISTORY", NUM_WAREHOUSES * 30'000},
                                                              {"STOCK", NUM_WAREHOUSES * 100'000},
                                                              {"NEW_ORDER", NUM_WAREHOUSES * 9'000},
                                                              {"CUSTOMER", NUM_WAREHOUSES * 30'000},
                                                              {"ITEM", 100'000},
                                                              {"ORDER_LINE", 0},  // see verify_table_sizes
                                                              {"ORDER", NUM_WAREHOUSES * 30'000}};

  static std::map<std::string, std::shared_ptr<Table>> tables;
  static constexpr auto NUM_WAREHOUSES = 2;
};

std::map<std::string, std::shared_ptr<Table>> TpccTest::tables;

TEST_F(TpccTest, InitialTables) {
  verify_table_sizes(initial_sizes);
}

TEST_F(TpccTest, Delivery) {
  auto old_transaction_context = TransactionManager::get().new_transaction_context();
  auto old_time = time(nullptr);

  BenchmarkSQLExecutor sql_executor{false, nullptr, std::nullopt};
  auto delivery = TpccDelivery{NUM_WAREHOUSES, sql_executor};
  EXPECT_TRUE(delivery.execute());

  auto new_sizes = initial_sizes;
  new_sizes["NEW_ORDER"] -= 10;
  verify_table_sizes(new_sizes);

  // These tests assume that updates / insertions into a table are done in the same physical order as they were in the
  // original table
  for (auto d_id = 1; d_id <= 10; ++d_id) {
    // Store data as it was before the procedure was executed:
    uint64_t num_order_lines;
    float old_c_balance, expected_c_balance;
    int old_c_delivery_cnt;

    // Retrieve old data
    {
      // We have #ORDER (3000) - #NEW_ORDER (900) delivered orders per warehouse, so 2101 is the first undelivered ID
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM ORDER_LINE WHERE OL_W_ID = "} + std::to_string(delivery.w_id) + " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID = 2101"}.with_transaction_context(old_transaction_context).create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      num_order_lines = table->row_count();
      EXPECT_GE(num_order_lines, 5);
      EXPECT_LE(num_order_lines, 15);
    }
    {
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM CUSTOMER WHERE C_W_ID = "} + std::to_string(delivery.w_id) + " AND C_D_ID = " + std::to_string(d_id) + " AND C_ID = (SELECT O_C_ID FROM \"ORDER\" WHERE O_W_ID = " + std::to_string(delivery.w_id) + " AND O_D_ID = " + std::to_string(d_id) + " AND O_ID = 2101)"}.with_transaction_context(old_transaction_context).create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      EXPECT_EQ(table->row_count(), 1);
      old_c_balance = table->get_value<float>(ColumnID{16}, 0);
      old_c_delivery_cnt = table->get_value<int>(ColumnID{19}, 0);
    }
    expected_c_balance = old_c_balance;

    // Verify that the order was deleted from the NEW_ORDER table:
    {
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM NEW_ORDER WHERE NO_W_ID = "} + std::to_string(delivery.w_id) + " AND NO_D_ID = " + std::to_string(d_id) + " AND NO_O_ID = 2101"}.create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      EXPECT_EQ(table->row_count(), 0);
    }

    int c_id;

    // Verify that it was updated in the ORDER table - not filtering by the district ID to keep it interesting:
    {
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM \"ORDER\" WHERE O_W_ID = "} + std::to_string(delivery.w_id) + " AND O_ID = 2101"}.create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      EXPECT_EQ(table->row_count(), 10);
      EXPECT_EQ(table->get_value<int>(ColumnID{5}, d_id - 1), delivery.o_carrier_id);  // O_CARRIER_ID

      c_id = table->get_value<int>(ColumnID{3}, d_id - 1);
    }

    // Verify the entries in ORDER_LINE:
    {
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM ORDER_LINE WHERE OL_W_ID = "} + std::to_string(delivery.w_id) + " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID = 2101"}.create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      EXPECT_EQ(table->row_count(), num_order_lines);
      for (auto ol_number = uint64_t{1}; ol_number <= table->row_count(); ++ol_number) {
        EXPECT_EQ(table->get_value<int>(ColumnID{3}, ol_number - 1), ol_number);  // OL_NUMBER
        EXPECT_GE(table->get_value<int>(ColumnID{6}, ol_number - 1), old_time);  // OL_DELIVERY_D
        expected_c_balance += table->get_value<float>(ColumnID{8}, ol_number - 1);
      }
    }

    // Verify the entry in CUSTOMER:
    {
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT C_BALANCE, C_DELIVERY_CNT FROM CUSTOMER WHERE C_W_ID = "} + std::to_string(delivery.w_id) + " AND C_D_ID = " + std::to_string(d_id) + " AND C_ID = " + std::to_string(c_id)}.create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      EXPECT_EQ(table->row_count(), 1);
      EXPECT_FLOAT_EQ(table->get_value<float>(ColumnID{0}, 0), expected_c_balance);
      EXPECT_EQ(table->get_value<int>(ColumnID{1}, 0), old_c_delivery_cnt + 1);
    }
  }
}

}  // namespace opossum
