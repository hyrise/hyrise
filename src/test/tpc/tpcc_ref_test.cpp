#include <array>
#include <memory>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/import_export/csv_rfc_parser.hpp"

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

TEST_F(TpccRefTest, Test) {

}

}  // namespace opossum
