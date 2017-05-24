#include <array>
#include <memory>
#include <vector>

#include <json.hpp>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/import_export/csv_rfc_parser.hpp"

using json = nlohmann::json;

namespace opossum {

class TpccRefTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto TABLE_NAMES = {"CUSTOMER", "DISTRICT", "HISTORY", "ITEM", "NEW-ORDER", "ORDER-LINE", "ORDER", "STOCK", "WAREHOUSE"};

    CsvRfcParser parser(100 * 1000);

    for (const auto & table_name : TABLE_NAMES) {
      auto table = parser.parse(std::string("./") + table_name + ".csv");
      StorageManager::get().add_table(table_name, table);
    }
  }

  void TearDown() override {
    StorageManager::get().reset();
  }

private:
};

TEST_F(TpccRefTest, SimulationScenario) {
    auto json_file = std::ifstream("tpcc_simulation_input.json");
    auto simulation = nlohmann::json{};

    json_file >> simulation;

    for (const auto & transaction : simulation) {
        const auto & transaction_name = transaction[0];
       // const auto & transaction_params = transaction[1];

        std::cout << transaction_name << std::endl;
    }
}

}  // namespace opossum
