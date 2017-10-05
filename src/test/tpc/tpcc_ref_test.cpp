#include <json.hpp>

#include <array>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "import_export/csv_parser.hpp"
#include "storage/storage_manager.hpp"

#include "tpcc/new_order.hpp"
#include "tpcc/order_status.hpp"

using json = nlohmann::json;

namespace opossum {

class TransactionTestImpl {
 public:
  virtual void run_and_test_transaction_from_json(const nlohmann::json &json_params,
                                                  const nlohmann::json &json_results) = 0;
};

class OrderStatusTestImpl : public TransactionTestImpl {
 public:
  void run_and_test_transaction_from_json(const nlohmann::json &json_params, const nlohmann::json &json_results) {
    tpcc::OrderStatusParams params = json_params;
    tpcc::OrderStatusResult sqlite_results = json_results;

    auto our_result = _ref_impl.run_transaction(params);

    EXPECT_EQ(sqlite_results.c_id, our_result.c_id);
    EXPECT_EQ(sqlite_results.c_first, our_result.c_first);
    EXPECT_EQ(sqlite_results.c_middle, our_result.c_middle);
    EXPECT_EQ(sqlite_results.c_last, our_result.c_last);
    EXPECT_FLOAT_EQ(sqlite_results.c_balance, our_result.c_balance);
    EXPECT_EQ(sqlite_results.o_id, our_result.o_id);
    EXPECT_EQ(sqlite_results.o_carrier_id, our_result.o_carrier_id);
    EXPECT_EQ(sqlite_results.o_entry_d, our_result.o_entry_d);

    ASSERT_EQ(sqlite_results.order_lines.size(), our_result.order_lines.size());
    for (size_t l = 0; l < sqlite_results.order_lines.size(); l++) {
      const auto &our_ol = our_result.order_lines[l];
      const auto &sqlite_ol = sqlite_results.order_lines[l];

      EXPECT_EQ(sqlite_ol.ol_supply_w_id, our_ol.ol_supply_w_id);
      EXPECT_EQ(sqlite_ol.ol_i_id, our_ol.ol_i_id);
      EXPECT_EQ(sqlite_ol.ol_quantity, our_ol.ol_quantity);
      EXPECT_FLOAT_EQ(sqlite_ol.ol_amount, our_ol.ol_amount);
      EXPECT_EQ(sqlite_ol.ol_delivery_d, our_ol.ol_delivery_d);
    }
  }

 private:
  tpcc::OrderStatusRefImpl _ref_impl;
};

class NewOrderTestImpl : public TransactionTestImpl {
 public:
  void run_and_test_transaction_from_json(const nlohmann::json &json_params, const nlohmann::json &json_results) {
    tpcc::NewOrderParams params = json_params;
    tpcc::NewOrderResult sqlite_results = json_results;

    auto our_result = _ref_impl.run_transaction(params);

    EXPECT_FLOAT_EQ(sqlite_results.w_tax_rate, our_result.w_tax_rate);
    EXPECT_FLOAT_EQ(sqlite_results.d_tax_rate, our_result.d_tax_rate);
    EXPECT_EQ(sqlite_results.d_next_o_id, our_result.d_next_o_id);
    EXPECT_FLOAT_EQ(sqlite_results.c_discount, our_result.c_discount);
    EXPECT_EQ(sqlite_results.c_last, our_result.c_last);
    EXPECT_EQ(sqlite_results.c_credit, our_result.c_credit);

    ASSERT_EQ(sqlite_results.order_lines.size(), our_result.order_lines.size());
    for (size_t l = 0; l < sqlite_results.order_lines.size(); l++) {
      const auto &our_ol = our_result.order_lines[l];
      const auto &sqlite_ol = sqlite_results.order_lines[l];

      EXPECT_FLOAT_EQ(sqlite_ol.i_price, our_ol.i_price);
      EXPECT_EQ(sqlite_ol.i_name, our_ol.i_name);
      EXPECT_EQ(sqlite_ol.i_data, our_ol.i_data);
      EXPECT_EQ(sqlite_ol.s_qty, our_ol.s_qty);
      EXPECT_EQ(sqlite_ol.s_dist_xx, our_ol.s_dist_xx);
      EXPECT_EQ(sqlite_ol.s_ytd, our_ol.s_ytd);
      EXPECT_EQ(sqlite_ol.s_order_cnt, our_ol.s_order_cnt);
      EXPECT_EQ(sqlite_ol.s_remote_cnt, our_ol.s_remote_cnt);
      EXPECT_EQ(sqlite_ol.s_data, our_ol.s_data);
      EXPECT_FLOAT_EQ(sqlite_ol.amount, our_ol.amount);
    }
  }

 private:
  tpcc::NewOrderRefImpl _ref_impl;
};

class TpccRefTest : public BaseTest {
 public:
  TpccRefTest() {
    _transaction_impls = {{"OrderStatus", std::make_shared<OrderStatusTestImpl>()},
                          {"NewOrder", std::make_shared<NewOrderTestImpl>()}};
  }

 protected:
  void SetUp() override {
    const auto TABLE_NAMES = {"CUSTOMER",   "DISTRICT", "HISTORY", "ITEM",     "NEW-ORDER",
                              "ORDER-LINE", "ORDER",    "STOCK",   "WAREHOUSE"};

    CsvParser parser;

    for (const auto &table_name : TABLE_NAMES) {
      auto table = parser.parse(std::string("./") + table_name + ".csv");
      StorageManager::get().add_table(table_name, table);
    }
  }

  void TearDown() override { StorageManager::get().reset(); }

 protected:
  std::unordered_map<std::string, std::shared_ptr<TransactionTestImpl>> _transaction_impls;
};

TEST_F(TpccRefTest, SimulationScenario) {
  // Load input
  auto json_simulation_file = std::ifstream("tpcc_test_requests.json");
  assert(json_simulation_file.is_open());

  auto simulation_input = nlohmann::json{};
  json_simulation_file >> simulation_input;

  // Load output
  auto json_results_file = std::ifstream("tpcc_test_results.json");
  assert(json_results_file.is_open());

  auto simulation_results = nlohmann::json{};
  json_results_file >> simulation_results;

  assert(simulation_results.size() == simulation_input.size());

  for (size_t t = 0; t < simulation_input.size(); t++) {
    const auto &transaction = simulation_input[t];
    const auto &results = simulation_results[t];

    const auto &transaction_name = transaction["transaction"];
    const auto &transaction_params = transaction["params"];

    std::cout << "Testing: " << transaction_name << ":" << transaction_params << std::endl;

    auto iter = _transaction_impls.find(transaction_name);
    assert(iter != _transaction_impls.end());

    auto &impl = *iter->second;

    impl.run_and_test_transaction_from_json(transaction_params, results);
  }
}

}  // namespace opossum
