#include <json.hpp>

#include <array>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "import_export/csv_rfc_parser.hpp"
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
    tpcc::OrderStatusResult ref_result = json_results;

    auto our_result = _ref_impl.run_transaction(params);

    ASSERT_EQ(ref_result.c_id, our_result.c_id);
    ASSERT_EQ(ref_result.c_first, our_result.c_first);
    ASSERT_EQ(ref_result.c_middle, our_result.c_middle);
    ASSERT_EQ(ref_result.c_last, our_result.c_last);
    ASSERT_FLOAT_EQ(ref_result.c_balance, our_result.c_balance);
    ASSERT_EQ(ref_result.o_id, our_result.o_id);
    ASSERT_EQ(ref_result.o_carrier_id, our_result.o_carrier_id);
    ASSERT_EQ(ref_result.o_entry_d, our_result.o_entry_d);

    ASSERT_EQ(ref_result.order_lines.size(), our_result.order_lines.size());
    for (size_t l = 0; l < ref_result.order_lines.size(); l++) {
      const auto &our = our_result.order_lines[l];
      const auto &ref = ref_result.order_lines[l];

      ASSERT_EQ(ref.ol_supply_w_id, our.ol_supply_w_id);
      ASSERT_EQ(ref.ol_i_id, our.ol_i_id);
      ASSERT_EQ(ref.ol_quantity, our.ol_quantity);
      ASSERT_FLOAT_EQ(ref.ol_amount, our.ol_amount);
      ASSERT_EQ(ref.ol_delivery_d, our.ol_delivery_d);
    }
  }

 private:
  tpcc::OrderStatusRefImpl _ref_impl;
};

class NewOrderTestImpl : public TransactionTestImpl {
 public:
  void run_and_test_transaction_from_json(const nlohmann::json &json_params, const nlohmann::json &json_results) {
    tpcc::NewOrderParams params = json_params;
    tpcc::NewOrderResult ref_result = json_results;

    auto our_result = _ref_impl.run_transaction(params);

    ASSERT_FLOAT_EQ(ref_result.w_tax_rate, our_result.w_tax_rate);
    ASSERT_FLOAT_EQ(ref_result.d_tax_rate, our_result.d_tax_rate);
    ASSERT_EQ(ref_result.d_next_o_id, our_result.d_next_o_id);
    ASSERT_FLOAT_EQ(ref_result.c_discount, our_result.c_discount);
    ASSERT_EQ(ref_result.c_last, our_result.c_last);
    ASSERT_EQ(ref_result.c_credit, our_result.c_credit);

    ASSERT_EQ(ref_result.order_lines.size(), our_result.order_lines.size());
    for (size_t l = 0; l < ref_result.order_lines.size(); l++) {
      const auto &our = our_result.order_lines[l];
      const auto &ref = ref_result.order_lines[l];

      ASSERT_FLOAT_EQ(ref.i_price, our.i_price);
      ASSERT_EQ(ref.i_name, our.i_name);
      ASSERT_EQ(ref.i_data, our.i_data);
      ASSERT_EQ(ref.s_qty, our.s_qty);
      ASSERT_EQ(ref.s_dist_xx, our.s_dist_xx);
      ASSERT_EQ(ref.s_ytd, our.s_ytd);
      ASSERT_EQ(ref.s_order_cnt, our.s_order_cnt);
      ASSERT_EQ(ref.s_remote_cnt, our.s_remote_cnt);
      ASSERT_EQ(ref.s_data, our.s_data);
      ASSERT_FLOAT_EQ(ref.amount, our.amount);
    }
  }

 private:
  tpcc::NewOrderRefImpl _ref_impl;
};

class TpccRefTest : public BaseTest {
 public:
  TpccRefTest() {
    m_transactionImpls = {{"OrderStatus", std::make_shared<OrderStatusTestImpl>()},
                          {"NewOrder", std::make_shared<NewOrderTestImpl>()}};
  }

 protected:
  void SetUp() override {
    const auto TABLE_NAMES = {"CUSTOMER",   "DISTRICT", "HISTORY", "ITEM",     "NEW-ORDER",
                              "ORDER-LINE", "ORDER",    "STOCK",   "WAREHOUSE"};

    CsvRfcParser parser(100 * 1000);

    for (const auto &table_name : TABLE_NAMES) {
      auto table = parser.parse(std::string("./") + table_name + ".csv");
      StorageManager::get().add_table(table_name, table);
    }
  }

  void TearDown() override { StorageManager::get().reset(); }

 protected:
  std::unordered_map<std::string, std::shared_ptr<TransactionTestImpl>> m_transactionImpls;
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

    auto iter = m_transactionImpls.find(transaction_name);
    assert(iter != m_transactionImpls.end());

    auto &impl = *iter->second;

    impl.run_and_test_transaction_from_json(transaction_params, results);
  }
}

}  // namespace opossum
