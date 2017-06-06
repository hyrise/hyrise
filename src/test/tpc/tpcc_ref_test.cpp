#include <array>
#include <fstream>
#include <memory>
#include <unordered_map>
#include <vector>

#include <json.hpp>

#include "gtest/gtest.h"

#include "../base_test.hpp"
#include "../../benchmark-libs/tpcc/order_status.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/import_export/csv_rfc_parser.hpp"

using json = nlohmann::json;
using namespace tpcc;

namespace opossum {

class TransactionTestImpl {
 public:
  virtual void run_and_test_transaction_from_json(const nlohmann::json & json_params,
                                                  const nlohmann::json & json_results) = 0;
};

class OrderStatusTestImpl : public TransactionTestImpl {
 public:
  void run_and_test_transaction_from_json(const nlohmann::json & json_params,
                                          const nlohmann::json & json_results) {
      OrderStatusParams params = json_params;
      OrderStatusResult ref_result = json_results;

      auto our_result = _ref_impl.run_transaction(params);

      ASSERT_EQ(ref_result.c_id, our_result.c_id);
      ASSERT_EQ(ref_result.c_first, our_result.c_first);
      ASSERT_EQ(ref_result.c_middle, our_result.c_middle);
      ASSERT_EQ(ref_result.c_last, our_result.c_last);
      ASSERT_EQ(ref_result.c_balance, our_result.c_balance);
      ASSERT_EQ(ref_result.o_id, our_result.o_id);
      ASSERT_EQ(ref_result.o_carrier_id, our_result.o_carrier_id);
      ASSERT_EQ(ref_result.o_entry_d, our_result.o_entry_d);

      ASSERT_EQ(ref_result.order_lines.size(), our_result.order_lines.size());
      for (size_t l = 0; l < ref_result.order_lines.size(); l++) {
        const auto & our = our_result.order_lines[l];
        const auto & ref = ref_result.order_lines[l];

        ASSERT_EQ(ref.ol_supply_w_id, our.ol_supply_w_id);
        ASSERT_EQ(ref.ol_i_id, our.ol_i_id);
        ASSERT_EQ(ref.ol_quantity, our.ol_quantity);
        ASSERT_EQ(ref.ol_amount, our.ol_amount);
        ASSERT_EQ(ref.ol_delivery_d, our.ol_delivery_d);
      }
  }

 private:
  OrderStatusRefImpl _ref_impl;
};

class NewOrderTestImpl : public TransactionTestImpl {
 public:
  void run_and_test_transaction_from_json(const nlohmann::json & json_params,
                                          const nlohmann::json & json_results) {
      NewOrderParams params = json_params;
      NewOrderResults ref_result = json_results;

      auto our_result = _ref_impl.run_transaction(params);

      ASSERT_EQ(ref_result.c_id, our_result.c_id);
      ASSERT_EQ(ref_result.c_first, our_result.c_first);
      ASSERT_EQ(ref_result.c_middle, our_result.c_middle);
      ASSERT_EQ(ref_result.c_last, our_result.c_last);
      ASSERT_EQ(ref_result.c_balance, our_result.c_balance);
      ASSERT_EQ(ref_result.o_id, our_result.o_id);
      ASSERT_EQ(ref_result.o_carrier_id, our_result.o_carrier_id);
      ASSERT_EQ(ref_result.o_entry_d, our_result.o_entry_d);

      ASSERT_EQ(ref_result.order_lines.size(), our_result.order_lines.size());
      for (size_t l = 0; l < ref_result.order_lines.size(); l++) {
        const auto & our = our_result.order_lines[l];
        const auto & ref = ref_result.order_lines[l];

        ASSERT_EQ(ref.ol_supply_w_id, our.ol_supply_w_id);
        ASSERT_EQ(ref.ol_i_id, our.ol_i_id);
        ASSERT_EQ(ref.ol_quantity, our.ol_quantity);
        ASSERT_EQ(ref.ol_amount, our.ol_amount);
        ASSERT_EQ(ref.ol_delivery_d, our.ol_delivery_d);
      }
  }

 private:
  OrderStatusRefImpl _ref_impl;
};

class TpccRefTest : public BaseTest {
 public:
  TpccRefTest() {
    m_transactionImpls = {
        {"OrderStatus", std::make_shared<OrderStatusTestImpl>()},
        {"NewOrder", std::make_shared<NewOrderTestImpl>()}
    };
  }

 protected:
  void SetUp() override {
    const auto TABLE_NAMES = {"CUSTOMER", "DISTRICT", "HISTORY", "ITEM", "NEW-ORDER", "ORDER-LINE",
                              "ORDER", "STOCK", "WAREHOUSE"};

    CsvRfcParser parser(100 * 1000);

    for (const auto &table_name : TABLE_NAMES) {
      auto table = parser.parse(std::string("./") + table_name + ".csv");
      StorageManager::get().add_table(table_name, table);
    }
  }

  void TearDown() override {
    StorageManager::get().reset();
  }

 protected:
  std::unordered_map<std::string,
      std::shared_ptr<TransactionTestImpl>> m_transactionImpls;
};

TEST_F(TpccRefTest, SimulationScenario) {
  // Load input
  auto json_simulation_file = std::ifstream("tpcc_simulation_input.json");
  auto simulation_input = nlohmann::json{};
  json_simulation_file >> simulation_input;

  // Load output
  auto json_results_file = std::ifstream("tpcc_simulation_results.json");
  auto simulation_results = nlohmann::json{};
  json_results_file >> simulation_results;

  assert(simulation_results.size() == simulation_input.size());

  for (size_t t = 0; t < simulation_input.size(); t++) {
    const auto &transaction = simulation_input[t];
    const auto &results = simulation_results[t];

    const auto &transaction_name = transaction["transaction"];
    const auto &transaction_params = transaction["params"];

    std::cout << "Testing: " << transaction_name << std::endl;

    auto iter = m_transactionImpls.find(transaction_name);
    assert(iter != m_transactionImpls.end());

    auto & impl = *iter->second;

    impl.run_and_test_transaction_from_json(transaction_params, results);
  }
}

}  // namespace opossum
