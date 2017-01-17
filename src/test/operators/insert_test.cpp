#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/concurrency/transaction_context.hpp"
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
    t->append({123, " bro"});
    StorageManager::get().add_table(table_name, t);

    gt = std::make_shared<GetTable>(table_name);
    gt->execute();

    t2 = std::make_shared<Table>(Table(chunk_size));
    t2->add_column("col_1", "int");
    t2->add_column("col_2", "string");
    t2->append({123, " bro"});
    StorageManager::get().add_table(table_name2, t2);

    gt2 = std::make_shared<GetTable>(table_name2);
    gt2->execute();
  }

  std::ostringstream output;

  std::string table_name = "insertTestTable";
  std::string table_name2 = "insertTestTable2";

  uint32_t chunk_size = 10;

  std::shared_ptr<GetTable>(gt);
  std::shared_ptr<Table> t = nullptr;
  std::shared_ptr<GetTable>(gt2);
  std::shared_ptr<Table> t2 = nullptr;
};

TEST_F(InsertTest, SelfInsert) {
  auto ins = std::make_shared<Insert>(gt, gt2);
  auto context = TransactionContext(1, 1);
  ins->execute(&context);

  // Check that row has been inserted.
  EXPECT_EQ(t->get_chunk(0).size(), 2u);
  EXPECT_EQ((*t->get_chunk(0).get_column(0))[0], AllTypeVariant(123));
  EXPECT_EQ((*t->get_chunk(0).get_column(1))[0], AllTypeVariant(" bro"));
  EXPECT_EQ((*t->get_chunk(0).get_column(0))[1], AllTypeVariant(123));
  EXPECT_EQ((*t->get_chunk(0).get_column(1))[1], AllTypeVariant(" bro"));

  auto output_str = output.str();
}
}  // namespace opossum
