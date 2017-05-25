#include <array>
#include <fstream>
#include <memory>
#include <unordered_map>
#include <vector>

#include <json.hpp>

#include "gtest/gtest.h"

#include "../base_test.hpp"
#include "../../benchmark-libs/tpcc/abstract_transaction_impl.h"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/import_export/csv_rfc_parser.hpp"

using json = nlohmann::json;

namespace opossum {

class TpccRefTest : public BaseTest {
 public:
  TpccRefTest() {
    m_transactionImpls = {
        {"OrderStatus", std::make_shared<OrderStatusRefImpl>()}
    };
  }

 protected:
  void SetUp() override {
    const auto TABLE_NAMES = {"CUSTOMER", "DISTRICT", "HISTORY", "ITEM", "NEW-ORDER", "ORDER-LINE", "ORDER", "STOCK",
                              "WAREHOUSE"};

    CsvRfcParser parser(100 * 1000);

    for (const auto &table_name : TABLE_NAMES) {
      auto table = parser.parse(std::string("./") + table_name + ".csv");
      StorageManager::get().add_table(table_name, table);
    }
  }

  void TearDown() override {
    StorageManager::get().reset();
  }

 private:
  std::unordered_map<std::string,
      std::shared_ptr<AbstractTransactionImpl>> m_transactionImpls;
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

    const auto &transaction_name = transaction[0];
    const auto &transaction_params = transaction[1];

    std::cout << transaction_name << std::endl;

    auto iter = m_transactionImpls.find(transaction_name);
    assert(iter != m_transactionImpls.end());

    auto & impl = *iter.second;

    impl.run_and_test_transaction_from_json(transaction_params, results );
  }
}

}  // namespace opossum
