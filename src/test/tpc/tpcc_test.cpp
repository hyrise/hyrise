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
      StorageManager::get().add_table(table_name, table);
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
    // Verify that the order was deleted from the NEW_ORDER table:
    {
      // We have #ORDER (3000) - #NEW_ORDER (900) delivered orders per warehouse, so 2101 is the first undelivered ID
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM NEW_ORDER WHERE NO_W_ID = 1 AND NO_D_ID = "} + std::to_string(d_id) + " AND NO_O_ID = 2101"}.create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      EXPECT_EQ(table->row_count(), 0);
    }

    // Verify that it was updated in the ORDER table:
    {
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM ORDER WHERE NO_W_ID = 1 AND NO_D_ID = "} + std::to_string(d_id) + " AND NO_O_ID = 2101"}.create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      EXPECT_EQ(table->row_count(), 1);
      EXPECT_GT(table->get_value<int>(ColumnID{5}, d_id), 0);  // O_CARRIER_ID
    }

    // Verify the entries in ORDER_LINE:
    {
      auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM ORDER_LINE WHERE NO_W_ID = 1 AND NO_D_ID = "} + std::to_string(d_id) + " AND NO_O_ID = 2101"}.create_pipeline();
      const auto [_, table] = pipeline.get_result_table();
      EXPECT_GE(table->row_count(), 5);
      EXPECT_LE(table->row_count(), 15);
      for (auto ol_number = uint64_t{1}; ol_number < table->row_count(); ++ol_number) {
        EXPECT_EQ(table->get_value<int>(ColumnID{3}, ol_number), ol_number);  // OL_NUMBER
        EXPECT_GT(table->get_value<int>(ColumnID{6}, ol_number), old_time);  // OL_DELIVERY_D
      }
    }
  }
}

}  // namespace opossum
