#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/insert.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class InsertTest : public BaseTest {
 protected:
  void SetUp() override {
    t = std::make_shared<Table>(Table(chunk_size));
    t->add_column("col_1", "int");
    t->add_column("col_2", "string");
    StorageManager::get().add_table(table_name, t);

    gt = std::make_shared<GetTable>(table_name);
    gt->execute();
  }

  std::ostringstream output;

  std::string table_name = "insertTestTable";

  uint32_t chunk_size = 10;

  std::shared_ptr<GetTable>(gt);
  std::shared_ptr<Table> t = nullptr;
};

TEST_F(InsertTest, EmptyTable) {
  auto ins = std::make_shared<Insert>(gt, std::vector<AllTypeVariant>{1, "brah"});
  ins->execute();

  // check that row has been unserted
  EXPECT_EQ((*t->get_chunk(0).get_column(0))[0], AllTypeVariant(1));
  EXPECT_EQ((*t->get_chunk(0).get_column(1))[0], AllTypeVariant("brah"));

  auto output_str = output.str();
}
}  // namespace opossum
